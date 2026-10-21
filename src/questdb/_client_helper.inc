#pragma once
// This file is included into `_client.c`.


// Cython idiosyncrasy workaround.
// If we do this in Cython it treats `buf.obj` as
// a ref-counted `object` instead of a `PyObject*`,
// so we can't check it for NULL.
// Since `Py_buffer` is a Cython built-in we can't actually
// just re-define it in `extra_cpython.pxd`.
static int Py_buffer_obj_is_set(Py_buffer* buf)
{
    return buf->obj != NULL;
}

// Nonzero while the interpreter is shutting down. This is only a fallback
// check: OIDC callbacks use the admission/drain gate below for synchronization.
static int qdb_py_is_finalizing(void)
{
#if PY_VERSION_HEX >= 0x030D0000
    return Py_IsFinalizing();
#else
    return _Py_IsFinalizing();
#endif
}

// No Python objects or GIL acquisition in this gate. An admission covers the
// ENTIRE foreign callback, including the interval before it acquires the GIL.
// Shutdown closes admission and drains with the GIL released while Python is
// still alive. Otherwise CPython <=3.13 can pthread_exit() through Rust's
// non-unwinding callback ABI during finalization.
//
// Keep the gate allocated until process exit: detached native workers can still
// call a trampoline after module teardown, and must be able to refuse admission.
#include <pythread.h>
#include <stdlib.h>

typedef struct qdb_oidc_callback_gate {
    PyThread_type_lock mutex;
    PyThread_type_lock drained;
    size_t active;
    int stopped;
    // Deterministic native-only pre-GIL barrier, allocated only by tests.
    PyThread_type_lock test_ready;
    PyThread_type_lock test_release;
    int test_pause_next;
    int test_release_pending;
} qdb_oidc_callback_gate;

static qdb_oidc_callback_gate* qdb_oidc_gate = NULL;

static int qdb_oidc_callback_gate_init(void)
{
    qdb_oidc_callback_gate* gate = calloc(1, sizeof(*gate));
    if (gate == NULL)
        return 0;
    gate->mutex = PyThread_allocate_lock();
    gate->drained = PyThread_allocate_lock();
    if (gate->mutex == NULL || gate->drained == NULL) {
        if (gate->mutex != NULL) PyThread_free_lock(gate->mutex);
        if (gate->drained != NULL) PyThread_free_lock(gate->drained);
        free(gate);
        return 0;
    }
    // Also used after fork: never acquire/free the inherited locks, which may
    // belong to vanished threads. An inherited callback's leave is a no-op.
    qdb_oidc_gate = gate;
    return 1;
}

static qdb_oidc_callback_gate* qdb_oidc_callback_enter(void)
{
    qdb_oidc_callback_gate* gate = qdb_oidc_gate;
    if (gate == NULL)
        return NULL;
    PyThread_acquire_lock(gate->mutex, WAIT_LOCK);
    if (gate->stopped) {
        PyThread_release_lock(gate->mutex);
        return NULL;
    }
    if (gate->active++ == 0)
        PyThread_acquire_lock(gate->drained, WAIT_LOCK);
    PyThread_release_lock(gate->mutex);
    return gate;
}

static void qdb_oidc_callback_leave(qdb_oidc_callback_gate* gate)
{
    if (gate != qdb_oidc_gate)
        return; // fork inside a callback
    PyThread_acquire_lock(gate->mutex, WAIT_LOCK);
    if (--gate->active == 0)
        PyThread_release_lock(gate->drained);
    PyThread_release_lock(gate->mutex);
}

static void qdb_oidc_callback_stop(void)
{
    PyThread_acquire_lock(qdb_oidc_gate->mutex, WAIT_LOCK);
    qdb_oidc_gate->stopped = 1;
    PyThread_release_lock(qdb_oidc_gate->mutex);
}

static void qdb_oidc_callback_drain(void)
{
    // Called only after stop, with the GIL released and no Python locks held.
    PyThread_acquire_lock(qdb_oidc_gate->drained, WAIT_LOCK);
    PyThread_release_lock(qdb_oidc_gate->drained);
}

static int qdb_oidc_callback_stopped(qdb_oidc_callback_gate* gate)
{
    int stopped;
    PyThread_acquire_lock(gate->mutex, WAIT_LOCK);
    stopped = gate->stopped;
    PyThread_release_lock(gate->mutex);
    return stopped;
}

static int qdb_oidc_callback_reopen_for_test(void)
{
    int idle;
    PyThread_acquire_lock(qdb_oidc_gate->mutex, WAIT_LOCK);
    idle = qdb_oidc_gate->active == 0;
    if (idle) qdb_oidc_gate->stopped = 0;
    PyThread_release_lock(qdb_oidc_gate->mutex);
    return idle;
}

static int qdb_oidc_callback_pause_next_for_test(void)
{
    qdb_oidc_callback_gate* gate = qdb_oidc_gate;
    int ok;
    PyThread_acquire_lock(gate->mutex, WAIT_LOCK);
    if (gate->test_ready == NULL)
        gate->test_ready = PyThread_allocate_lock();
    if (gate->test_release == NULL)
        gate->test_release = PyThread_allocate_lock();
    ok = gate->test_ready != NULL && gate->test_release != NULL;
    if (ok) {
        PyThread_acquire_lock(gate->test_ready, NOWAIT_LOCK);
        PyThread_acquire_lock(gate->test_release, NOWAIT_LOCK);
        gate->test_pause_next = 1;
        gate->test_release_pending = 1;
    }
    PyThread_release_lock(gate->mutex);
    return ok;
}

static void qdb_oidc_callback_before_gil(qdb_oidc_callback_gate* gate)
{
    int pause;
    PyThread_acquire_lock(gate->mutex, WAIT_LOCK);
    pause = gate->test_pause_next;
    gate->test_pause_next = 0;
    PyThread_release_lock(gate->mutex);
    if (pause) {
        PyThread_release_lock(gate->test_ready);
        PyThread_acquire_lock(gate->test_release, WAIT_LOCK);
    }
}

static int qdb_oidc_callback_try_paused_for_test(void)
{
    if (qdb_oidc_gate->test_ready == NULL)
        return 0;
    // PyPy implements the public nonblocking operation, but not CPython's
    // timed-lock API. The Python test hook owns the timeout and polling sleep.
    return PyThread_acquire_lock(qdb_oidc_gate->test_ready, NOWAIT_LOCK);
}

static void qdb_oidc_callback_resume_for_test(void)
{
    qdb_oidc_callback_gate* gate = qdb_oidc_gate;
    PyThread_acquire_lock(gate->mutex, WAIT_LOCK);
    if (gate->test_release_pending) {
        gate->test_release_pending = 0;
        PyThread_release_lock(gate->test_release);
    }
    PyThread_release_lock(gate->mutex);
}
