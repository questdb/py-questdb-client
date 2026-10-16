cdef extern from "_client_helper.inc":
    bint Py_buffer_obj_is_set(Py_buffer* buf)
    bint qdb_py_is_finalizing() noexcept nogil
    ctypedef struct qdb_oidc_callback_gate:
        pass
    bint qdb_oidc_callback_gate_init() noexcept
    qdb_oidc_callback_gate* qdb_oidc_callback_enter() noexcept nogil
    void qdb_oidc_callback_leave(qdb_oidc_callback_gate*) noexcept nogil
    void qdb_oidc_callback_stop() noexcept nogil
    void qdb_oidc_callback_drain() noexcept nogil
    bint qdb_oidc_callback_stopped(qdb_oidc_callback_gate*) noexcept nogil
    bint qdb_oidc_callback_reopen_for_test() noexcept
    bint qdb_oidc_callback_pause_next_for_test() noexcept
    void qdb_oidc_callback_before_gil(qdb_oidc_callback_gate*) noexcept nogil
    bint qdb_oidc_callback_try_paused_for_test() noexcept nogil
    void qdb_oidc_callback_resume_for_test() noexcept
