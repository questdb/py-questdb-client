################################################################################
##     ___                  _   ____  ____
##    / _ \ _   _  ___  ___| |_|  _ \| __ )
##   | | | | | | |/ _ \/ __| __| | | |  _ \
##   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
##    \__\_\\__,_|\___||___/\__|____/|____/
##
##  Copyright (c) 2014-2019 Appsicle
##  Copyright (c) 2019-2024 QuestDB
##
##  Licensed under the Apache License, Version 2.0 (the "License");
##  you may not use this file except in compliance with the License.
##  You may obtain a copy of the License at
##
##  http://www.apache.org/licenses/LICENSE-2.0
##
##  Unless required by applicable law or agreed to in writing, software
##  distributed under the License is distributed on an "AS IS" BASIS,
##  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##  See the License for the specific language governing permissions and
##  limitations under the License.
##
################################################################################

from libc.stdint cimport int64_t, uint16_t, uint64_t, uint8_t, uint32_t, \
    int32_t, int8_t, int16_t

from .arrow_c_data_interface cimport ArrowArray, ArrowArrayStream, ArrowSchema

cdef extern from "stdbool.h":
    ctypedef unsigned char cbool "bool"

cdef extern from "questdb/ingress/line_sender.h":
    cdef struct line_sender_error:
        pass

    ctypedef line_sender_error questdb_error

    cdef enum line_sender_error_code:
        line_sender_error_could_not_resolve_addr,
        line_sender_error_invalid_api_call,
        line_sender_error_socket_error,
        line_sender_error_invalid_utf8,
        line_sender_error_invalid_name,
        line_sender_error_invalid_timestamp,
        line_sender_error_auth_error,
        line_sender_error_tls_error,
        line_sender_error_http_not_supported,
        line_sender_error_server_flush_error,
        line_sender_error_config_error,
        line_sender_error_array_error,
        line_sender_error_protocol_version_error,
        line_sender_error_invalid_decimal,
        line_sender_error_server_rejection,
        line_sender_error_arrow_unsupported_column_kind,
        line_sender_error_arrow_ingest,
        line_sender_error_failover_retry,
        line_sender_error_role_mismatch,
        line_sender_error_connect_timeout,
        # Query / reader (egress) categories, unified into this enum (20..36).
        line_sender_error_handshake_error,
        line_sender_error_unsupported_server,
        line_sender_error_protocol_error,
        line_sender_error_invalid_bind,
        line_sender_error_server_schema_mismatch,
        line_sender_error_server_parse_error,
        line_sender_error_server_internal_error,
        line_sender_error_server_security_error,
        line_sender_error_limit_exceeded,
        line_sender_error_server_limit_exceeded,
        line_sender_error_cancelled,
        line_sender_error_failover_would_duplicate,
        line_sender_error_schema_drift,
        line_sender_error_no_schema,
        line_sender_error_arrow_export,
        line_sender_error_batch_too_large,
        line_sender_error_store_resend_required

    ctypedef line_sender_error_code questdb_error_code

    cdef enum line_sender_protocol:
        line_sender_protocol_tcp,
        line_sender_protocol_tcps,
        line_sender_protocol_http,
        line_sender_protocol_https,
        line_sender_protocol_udp,
        line_sender_protocol_ws,
        line_sender_protocol_wss,
        line_sender_protocol_unknown,

    cdef enum line_sender_protocol_version:
        line_sender_protocol_version_1 = 1,
        line_sender_protocol_version_2 = 2,
        line_sender_protocol_version_3 = 3,

    cdef enum line_sender_ca:
        line_sender_ca_webpki_roots,
        line_sender_ca_os_roots,
        line_sender_ca_webpki_and_os_roots,
        line_sender_ca_pem_file,

    cdef enum line_sender_qwpws_progress:
        LINE_SENDER_QWPWS_PROGRESS_BACKGROUND,
        LINE_SENDER_QWPWS_PROGRESS_MANUAL,

    cdef struct line_sender_qwpws_fsn:
        cbool has_value
        uint64_t value

    cdef enum line_sender_qwpws_error_category:
        LINE_SENDER_QWPWS_ERROR_SCHEMA_MISMATCH,
        LINE_SENDER_QWPWS_ERROR_PARSE_ERROR,
        LINE_SENDER_QWPWS_ERROR_INTERNAL_ERROR,
        LINE_SENDER_QWPWS_ERROR_SECURITY_ERROR,
        LINE_SENDER_QWPWS_ERROR_WRITE_ERROR,
        LINE_SENDER_QWPWS_ERROR_NOT_WRITABLE,
        LINE_SENDER_QWPWS_ERROR_PROTOCOL_VIOLATION,
        LINE_SENDER_QWPWS_ERROR_UNKNOWN,

    cdef enum line_sender_qwpws_error_policy:
        LINE_SENDER_QWPWS_ERROR_RETRIABLE,
        LINE_SENDER_QWPWS_ERROR_RETRIABLE_OTHER,
        LINE_SENDER_QWPWS_ERROR_TERMINAL,

    cdef struct line_sender_qwpws_error:
        pass

    cdef struct line_sender_qwpws_error_view:
        line_sender_qwpws_error_category category
        line_sender_qwpws_error_policy applied_policy
        cbool has_status
        uint8_t status
        cbool has_message_sequence
        uint64_t message_sequence
        uint64_t from_fsn
        uint64_t to_fsn
        const char* message
        size_t message_len

    ctypedef void (*line_sender_qwpws_error_cb)(
        void* user_data,
        const line_sender_qwpws_error_view* event
        ) noexcept nogil

    questdb_error_code questdb_error_get_code(
        const questdb_error* error
        ) noexcept nogil

    const char* questdb_error_msg(
        const questdb_error* error,
        size_t* len_out
        ) noexcept nogil

    bint questdb_error_in_doubt(
        const questdb_error* error
        ) noexcept nogil

    void questdb_error_free(
        questdb_error* error
        ) noexcept nogil

    line_sender_error_code line_sender_error_get_code(
        const line_sender_error* error
        ) noexcept nogil

    const char* line_sender_error_msg(
        const line_sender_error* error,
        size_t* len_out
        ) noexcept nogil

    void line_sender_error_free(
        line_sender_error* error
        ) noexcept nogil

    cdef struct line_sender_utf8:
        size_t len
        const char *buf

    bint line_sender_utf8_init(
        line_sender_utf8* string,
        size_t len,
        const char* buf,
        line_sender_error** err_out
        ) noexcept nogil

    line_sender_utf8 line_sender_utf8_assert(
        size_t len,
        const char* buf
        ) noexcept nogil

    cdef struct line_sender_table_name:
        size_t len
        const char* buf

    bint line_sender_table_name_init(
        line_sender_table_name* name,
        size_t len,
        const char* buf,
        line_sender_error** err_out
        ) noexcept nogil

    line_sender_table_name line_sender_table_name_assert(
        size_t len,
        const char* buf
        ) noexcept nogil

    cdef struct line_sender_column_name:
        size_t len
        const char* buf

    cdef struct line_sender_buffer_view:
        size_t len
        const uint8_t* buf

    bint line_sender_column_name_init(
        line_sender_column_name* name,
        size_t len,
        const char* buf,
        line_sender_error** err_out
        ) noexcept nogil

    line_sender_column_name line_sender_column_name_assert(
        size_t len,
        const char* buf
        ) noexcept nogil

    cdef struct line_sender_buffer:
        pass

    line_sender_buffer* line_sender_buffer_new(
        line_sender_protocol_version version,
        ) noexcept nogil

    line_sender_buffer* line_sender_buffer_with_max_name_len(
        line_sender_protocol_version version,
        size_t max_name_len
        ) noexcept nogil

    line_sender_buffer* line_sender_buffer_new_qwp(
        ) noexcept nogil

    line_sender_buffer* line_sender_buffer_new_qwp_with_max_name_len(
        size_t max_name_len
        ) noexcept nogil

    void line_sender_buffer_free(
        line_sender_buffer* buffer
        ) noexcept nogil

    line_sender_buffer* line_sender_buffer_clone(
        const line_sender_buffer* buffer,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_buffer_reserve(
        line_sender_buffer* buffer,
        size_t additional,
        line_sender_error** err_out
        ) noexcept nogil

    size_t line_sender_buffer_capacity(
        const line_sender_buffer* buffer
        ) noexcept nogil

    bint line_sender_buffer_set_marker(
        line_sender_buffer* buffer,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_buffer_rewind_to_marker(
        line_sender_buffer* buffer,
        line_sender_error** err_out
        ) noexcept nogil

    void line_sender_buffer_clear_marker(
        line_sender_buffer* buffer
        ) noexcept nogil

    void line_sender_buffer_clear(
        line_sender_buffer* buffer
        ) noexcept nogil

    size_t line_sender_buffer_size(
        const line_sender_buffer* buffer
        ) noexcept nogil

    size_t line_sender_buffer_row_count(
        const line_sender_buffer* buffer
        ) noexcept nogil

    bint line_sender_buffer_transactional(
        const line_sender_buffer* buffer
        ) noexcept nogil

    line_sender_buffer_view line_sender_buffer_peek(
        const line_sender_buffer* buffer
        ) noexcept nogil

    bint line_sender_buffer_table(
        line_sender_buffer* buffer,
        line_sender_table_name name,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_buffer_symbol(
        line_sender_buffer* buffer,
        line_sender_column_name name,
        line_sender_utf8 value,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_buffer_column_bool(
        line_sender_buffer* buffer,
        line_sender_column_name name,
        bint value,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_buffer_column_i64(
        line_sender_buffer* buffer,
        line_sender_column_name name,
        int64_t value,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_buffer_column_f64(
        line_sender_buffer* buffer,
        line_sender_column_name name,
        double value,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_buffer_column_str(
        line_sender_buffer* buffer,
        line_sender_column_name name,
        line_sender_utf8 value,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_buffer_column_f64_arr_c_major(
        line_sender_buffer* buffer,
        line_sender_column_name name,
        size_t rank,
        const size_t* shapes,
        const double* data,
        size_t data_len,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_buffer_column_f64_arr_byte_strides(
        line_sender_buffer* buffer,
        line_sender_column_name name,
        size_t rank,
        const size_t* shapes,
        const ssize_t* strides,
        const double* data,
        size_t data_len,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_buffer_column_ts_nanos(
        line_sender_buffer* buffer,
        line_sender_column_name name,
        int64_t nanos,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_buffer_column_ts_micros(
        line_sender_buffer* buffer,
        line_sender_column_name name,
        int64_t micros,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_buffer_column_dec_str(
        line_sender_buffer* buffer,
        line_sender_column_name name,
        const char *value,
        size_t value_len,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_buffer_column_dec(
        line_sender_buffer* buffer,
        line_sender_column_name name,
        const unsigned int scale,
        const uint8_t* data,
        size_t data_len,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_buffer_at_nanos(
        line_sender_buffer* buffer,
        int64_t epoch_nanos,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_buffer_at_micros(
        line_sender_buffer* buffer,
        int64_t epoch_micros,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_buffer_at_now(
        line_sender_buffer* buffer,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_buffer_check_can_flush(
        const line_sender_buffer* buffer,
        line_sender_error** err_out
        ) noexcept nogil

    cdef struct line_sender:
        pass

    cdef struct line_sender_opts:
        pass

    line_sender_opts* line_sender_opts_from_conf(
        line_sender_utf8 conf,
        line_sender_error** err_out
        ) noexcept nogil

    line_sender_opts* line_sender_opts_from_env(
        line_sender_error** err_out
        ) noexcept nogil

    line_sender_opts* line_sender_opts_new(
        line_sender_protocol protocol,
        line_sender_utf8 host,
        uint16_t port
        ) noexcept nogil

    line_sender_opts* line_sender_opts_new_service(
        line_sender_protocol protocol,
        line_sender_utf8 host,
        line_sender_utf8 port
        ) noexcept nogil

    bint line_sender_opts_bind_interface(
        line_sender_opts* opts,
        line_sender_utf8 bind_interface,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_opts_max_datagram_size(
        line_sender_opts* opts,
        size_t max_datagram_size,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_opts_multicast_ttl(
        line_sender_opts* opts,
        uint32_t multicast_ttl,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_opts_qwpws_progress(
        line_sender_opts* opts,
        line_sender_qwpws_progress progress,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_opts_qwpws_error_handler(
        line_sender_opts* opts,
        line_sender_qwpws_error_cb cb,
        void* user_data,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_opts_username(
        line_sender_opts* opts,
        line_sender_utf8 username,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_opts_password(
        line_sender_opts* opts,
        line_sender_utf8 password,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_opts_token(
        line_sender_opts* opts,
        line_sender_utf8 token,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_opts_token_x(
        line_sender_opts* opts,
        line_sender_utf8 token_x,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_opts_token_y(
        line_sender_opts* opts,
        line_sender_utf8 token_y,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_opts_protocol_version(
        line_sender_opts* opts,
        line_sender_protocol_version version,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_opts_auth_timeout(
        line_sender_opts* opts,
        uint64_t millis,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_opts_tls_verify(
        line_sender_opts* opts,
        bint verify,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_opts_tls_ca(
        line_sender_opts* opts,
        line_sender_ca ca,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_opts_tls_roots(
        line_sender_opts* opts,
        line_sender_utf8 path,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_opts_tls_roots_password(
        line_sender_opts* opts,
        line_sender_utf8 password,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_opts_max_buf_size(
        line_sender_opts* opts,
        size_t max_buf_size,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_opts_max_name_len(
        line_sender_opts* opts,
        size_t max_name_len,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_opts_retry_timeout(
        line_sender_opts* opts,
        uint64_t millis,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_opts_retry_max_backoff(
        line_sender_opts* opts,
        uint64_t millis,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_opts_request_min_throughput(
        line_sender_opts* opts,
        uint64_t bytes_per_sec,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_opts_request_timeout(
        line_sender_opts* opts,
        uint64_t millis,
        line_sender_error** err_out
        ) noexcept nogil

    line_sender_opts* line_sender_opts_clone(
        line_sender_opts* opts
        ) noexcept nogil

    void line_sender_opts_free(
        line_sender_opts* opts
        ) noexcept nogil

    line_sender* line_sender_build(
        const line_sender_opts *opts,
        line_sender_error** err_out
        ) noexcept nogil

    line_sender* line_sender_from_conf(
        line_sender_utf8 config,
        line_sender_error** err_out
        ) noexcept nogil

    line_sender* line_sender_from_env(
        line_sender_error** err_out
        ) noexcept nogil

    line_sender_protocol_version line_sender_get_protocol_version(
        const line_sender * sender
        ) noexcept nogil

    line_sender_protocol line_sender_get_protocol(
        const line_sender * sender
        ) noexcept nogil

    size_t line_sender_get_max_name_len(
        const line_sender * sender
        ) noexcept nogil

    line_sender_buffer* line_sender_buffer_new_for_sender(
        const line_sender * sender
        ) noexcept nogil

    bint line_sender_must_close(
        const line_sender* sender
        ) noexcept nogil

    void line_sender_close(
        line_sender* sender
        ) noexcept nogil

    bint line_sender_qwpws_flush_and_get_fsn(
        line_sender* sender,
        line_sender_buffer* buffer,
        line_sender_qwpws_fsn* fsn_out,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_qwpws_flush_and_keep_and_get_fsn(
        line_sender* sender,
        const line_sender_buffer* buffer,
        line_sender_qwpws_fsn* fsn_out,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_qwpws_drive_once(
        line_sender* sender,
        cbool* progressed_out,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_qwpws_published_fsn(
        const line_sender* sender,
        line_sender_qwpws_fsn* fsn_out,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_qwpws_acked_fsn(
        const line_sender* sender,
        line_sender_qwpws_fsn* fsn_out,
        line_sender_error** err_out
        ) noexcept nogil

    cdef enum qwpws_ack_level:
        qwpws_ack_level_ok = 0
        qwpws_ack_level_durable = 1

    bint line_sender_qwpws_wait(
        line_sender* sender,
        uint32_t ack_level,
        uint64_t timeout_millis,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_qwpws_poll_error(
        line_sender* sender,
        line_sender_qwpws_error** error_out,
        line_sender_error** err_out
        ) noexcept nogil

    line_sender_qwpws_error_view line_sender_qwpws_error_get_view(
        const line_sender_qwpws_error* error
        ) noexcept nogil

    bint line_sender_error_qwpws_get_view(
        const line_sender_error* error,
        line_sender_qwpws_error_view* view_out
        ) noexcept nogil

    void line_sender_qwpws_error_free(
        line_sender_qwpws_error* error
        ) noexcept nogil

    bint line_sender_qwpws_errors_dropped(
        const line_sender* sender,
        uint64_t* dropped_out,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_qwpws_close_drain(
        line_sender* sender,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_flush(
        line_sender* sender,
        line_sender_buffer* buffer,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_flush_and_keep(
        line_sender *sender,
        const line_sender_buffer* buffer,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_flush_and_keep_with_flags(
        line_sender* sender,
        line_sender_buffer* buffer,
        bint transactional,
        line_sender_error** err_out
        ) noexcept nogil

    int64_t line_sender_now_nanos(
        ) noexcept nogil

    int64_t line_sender_now_micros(
        ) noexcept nogil


    # Extra private API, not exposed in header
    bint line_sender_opts_user_agent(
        line_sender_opts* opts,
        line_sender_utf8 user_agent,
        line_sender_error** err_out
        ) noexcept nogil


cdef extern from "questdb/ingress/column_sender.h":
    cdef struct questdb_db:
        pass

    cdef struct qwp_sender:
        pass

    cdef struct direct_column_sender:
        pass

    cdef struct column_sender_chunk:
        pass

    cdef struct column_sender_arrow_import:
        pass

    cdef struct column_sender_validity:
        const uint8_t* bits
        size_t bit_len

    questdb_db* questdb_db_connect(
        const char* conf,
        size_t conf_len,
        line_sender_error** err_out
        ) noexcept nogil

    void questdb_db_close(
        questdb_db* db
        ) noexcept nogil

    qwp_sender* questdb_db_borrow_sender(
        questdb_db* db,
        line_sender_error** err_out
        ) noexcept nogil

    qwp_sender* questdb_db_borrow_sender_with_retry(
        questdb_db* db,
        uint64_t budget_ms,
        line_sender_error** err_out
        ) noexcept nogil

    line_sender_buffer* questdb_db_new_buffer(
        const questdb_db* db,
        line_sender_error** err_out
        ) noexcept nogil

    size_t questdb_db_buffer_max_name_len(
        const questdb_db* db
        ) noexcept nogil

    direct_column_sender* questdb_db_borrow_direct_column_sender(
        questdb_db* db,
        line_sender_error** err_out
        ) noexcept nogil

    direct_column_sender* questdb_db_borrow_direct_column_sender_with_retry(
        questdb_db* db,
        uint64_t budget_ms,
        line_sender_error** err_out
        ) noexcept nogil

    direct_column_sender* direct_column_sender_from_conf(
        const char* conf,
        size_t conf_len,
        line_sender_error** err_out
        ) noexcept nogil

    direct_column_sender* direct_column_sender_from_opts(
        const line_sender_opts* opts,
        line_sender_error** err_out
        ) noexcept nogil

    void direct_column_sender_free(
        direct_column_sender* sender
        ) noexcept nogil

    uint64_t questdb_db_reconnect_max_duration_ms(
        const questdb_db* db
        ) noexcept nogil

    void questdb_db_return_sender(
        questdb_db* db,
        qwp_sender* sender
        ) noexcept nogil

    void questdb_db_drop_sender(
        questdb_db* db,
        qwp_sender* sender
        ) noexcept nogil

    void questdb_db_return_direct_column_sender(
        questdb_db* db,
        direct_column_sender* conn
        ) noexcept nogil

    void questdb_db_drop_direct_column_sender(
        questdb_db* db,
        direct_column_sender* conn
        ) noexcept nogil

    size_t questdb_db_reap_idle(
        questdb_db* db
        ) noexcept nogil

    cdef struct questdb_connection_event:
        uint32_t kind
        const char* host
        size_t host_len
        const char* port
        size_t port_len
        const char* previous_host
        size_t previous_host_len
        const char* previous_port
        size_t previous_port_len
        bint has_attempt
        uint64_t attempt_number
        bint has_cause
        line_sender_error_code cause_code
        const char* cause_msg
        size_t cause_msg_len
        int64_t timestamp_millis

    ctypedef void (*questdb_connection_event_cb)(
        void* user_data,
        const questdb_connection_event* event
        ) noexcept nogil

    questdb_db* questdb_db_connect_with_event_handler(
        const char* conf,
        size_t conf_len,
        questdb_connection_event_cb callback,
        void* user_data,
        size_t inbox_capacity,
        questdb_error** err_out
        ) noexcept nogil

    questdb_db* questdb_db_connect_with_handlers(
        const char* conf,
        size_t conf_len,
        questdb_connection_event_cb event_callback,
        void* event_user_data,
        size_t event_inbox_capacity,
        line_sender_qwpws_error_cb rejection_callback,
        void* rejection_user_data,
        size_t rejection_inbox_capacity,
        questdb_error** err_out
        ) noexcept nogil

    uint64_t questdb_db_connection_events_dropped(
        const questdb_db* db
        ) noexcept nogil

    uint64_t questdb_db_connection_events_delivered(
        const questdb_db* db
        ) noexcept nogil

    uint64_t questdb_db_rejection_events_delivered(
        const questdb_db* db
        ) noexcept nogil

    uint64_t questdb_db_rejection_events_dropped(
        const questdb_db* db
        ) noexcept nogil

    bint line_sender_opts_connection_event_handler(
        line_sender_opts* opts,
        questdb_connection_event_cb cb,
        void* user_data,
        size_t inbox_capacity,
        line_sender_error** err_out
        ) noexcept nogil

    uint64_t line_sender_connection_events_dropped(
        const line_sender* sender
        ) noexcept nogil

    uint64_t line_sender_connection_events_delivered(
        const line_sender* sender
        ) noexcept nogil

    column_sender_chunk* column_sender_chunk_new(
        const char* table_name,
        size_t table_name_len,
        line_sender_error** err_out
        ) noexcept nogil

    void column_sender_chunk_free(
        column_sender_chunk* chunk
        ) noexcept nogil

    bint column_sender_chunk_clear(
        column_sender_chunk* chunk,
        line_sender_error** err_out
        ) noexcept nogil

    size_t column_sender_chunk_row_count(
        const column_sender_chunk* chunk,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_column_bool(
        column_sender_chunk* chunk,
        const char* name,
        size_t name_len,
        const uint8_t* data,
        size_t row_count,
        const column_sender_validity* validity,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_column_str(
        column_sender_chunk* chunk,
        const char* name,
        size_t name_len,
        const int32_t* offsets,
        const uint8_t* bytes,
        size_t bytes_len,
        size_t row_count,
        const column_sender_validity* validity,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_column_binary(
        column_sender_chunk* chunk,
        const char* name,
        size_t name_len,
        const int32_t* offsets,
        const uint8_t* bytes,
        size_t bytes_len,
        size_t row_count,
        const column_sender_validity* validity,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_at_micros(
        column_sender_chunk* chunk,
        const int64_t* data,
        size_t row_count,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_at_nanos(
        column_sender_chunk* chunk,
        const int64_t* data,
        size_t row_count,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_at_millis(
        column_sender_chunk* chunk,
        const int64_t* data,
        size_t row_count,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_at_seconds(
        column_sender_chunk* chunk,
        const int64_t* data,
        size_t row_count,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_at_now(
        column_sender_chunk* chunk,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_at_scalar_nanos(
        column_sender_chunk* chunk,
        int64_t nanos,
        line_sender_error** err_out
        ) noexcept nogil

    cdef enum column_sender_symbol_mode:
        column_sender_symbol_mode_auto = 0
        column_sender_symbol_mode_symbol = 1
        column_sender_symbol_mode_not_symbol = 2

    column_sender_arrow_import* column_sender_arrow_import_new(
        ArrowArray* array,
        const ArrowSchema* schema,
        column_sender_symbol_mode symbol_mode,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_append_arrow_import(
        column_sender_chunk* chunk,
        const char* name,
        size_t name_len,
        const column_sender_arrow_import* imported,
        size_t row_offset,
        size_t row_count,
        line_sender_error** err_out
        ) noexcept nogil

    void column_sender_arrow_import_free(
        column_sender_arrow_import* imported
        ) noexcept nogil

    bint column_sender_chunk_append_arrow_column(
        column_sender_chunk* chunk,
        const char* name,
        size_t name_len,
        ArrowArray* array,
        const ArrowSchema* schema,
        size_t row_offset,
        size_t row_count,
        line_sender_error** err_out
        ) noexcept nogil

    cdef enum column_sender_numpy_dtype:
        column_sender_numpy_i8 = 0
        column_sender_numpy_i16 = 1
        column_sender_numpy_i32 = 2
        column_sender_numpy_i64 = 3
        column_sender_numpy_u8 = 4
        column_sender_numpy_u16 = 5
        column_sender_numpy_u32 = 6
        column_sender_numpy_u64 = 7
        column_sender_numpy_f32 = 8
        column_sender_numpy_f64 = 9
        column_sender_numpy_bool = 10
        column_sender_numpy_f16 = 11
        column_sender_numpy_datetime64_s = 12
        column_sender_numpy_datetime64_ms = 13
        column_sender_numpy_datetime64_us = 14
        column_sender_numpy_datetime64_ns = 15
        column_sender_numpy_timedelta64_s = 16
        column_sender_numpy_timedelta64_ms = 17
        column_sender_numpy_timedelta64_us = 18
        column_sender_numpy_timedelta64_ns = 19
        column_sender_numpy_s16 = 20
        column_sender_numpy_s32 = 21
        column_sender_numpy_decimal_s8 = 22
        column_sender_numpy_decimal_s16 = 23
        column_sender_numpy_decimal_s32 = 24
        column_sender_numpy_u32_ipv4 = 25
        column_sender_numpy_u16_char = 26
        column_sender_numpy_geohash_i8 = 27
        column_sender_numpy_geohash_i16 = 28
        column_sender_numpy_geohash_i32 = 29
        column_sender_numpy_geohash_i64 = 30
        column_sender_numpy_f64_ndarray = 31
        column_sender_numpy_datetime64_m = 32
        column_sender_numpy_datetime64_h = 33
        column_sender_numpy_datetime64_D = 34
        column_sender_numpy_datetime64_M = 35
        column_sender_numpy_datetime64_Y = 36
        column_sender_numpy_datetime64_W = 37
        column_sender_numpy_timedelta64_m = 38
        column_sender_numpy_timedelta64_h = 39
        column_sender_numpy_timedelta64_D = 40
        column_sender_numpy_timedelta64_M = 41
        column_sender_numpy_timedelta64_Y = 42

    cdef struct column_sender_numpy_extras:
        int8_t decimal_scale
        uint8_t geohash_bits
        uint8_t array_ndim
        const uint32_t* array_shape

    bint column_sender_chunk_append_numpy_column(
        column_sender_chunk* chunk,
        const char* name,
        size_t name_len,
        uint32_t dtype,
        const uint8_t* data,
        size_t data_len_bytes,
        size_t row_count,
        const column_sender_validity* validity,
        const column_sender_numpy_extras* extras,
        line_sender_error** err_out
        ) noexcept nogil

    bint qwp_sender_flush_buffer(
        qwp_sender* sender,
        line_sender_buffer* buffer,
        line_sender_error** err_out
        ) noexcept nogil

    bint qwp_sender_flush_buffer_and_wait(
        qwp_sender* sender,
        line_sender_buffer* buffer,
        uint32_t ack_level,
        line_sender_error** err_out
        ) noexcept nogil

    bint qwp_sender_wait(
        qwp_sender* sender,
        uint32_t ack_level,
        uint64_t timeout_millis,
        line_sender_error** err_out
        ) noexcept nogil

    bint direct_column_sender_flush(
        direct_column_sender* conn,
        column_sender_chunk* chunk,
        line_sender_error** err_out
        ) noexcept nogil

    bint direct_column_sender_commit(
        direct_column_sender* conn,
        uint32_t ack_level,
        line_sender_error** err_out
        ) noexcept nogil

    cdef enum column_sender_arrow_override_kind:
        column_sender_arrow_override_symbol = 0
        column_sender_arrow_override_ipv4 = 1
        column_sender_arrow_override_char = 2
        column_sender_arrow_override_geohash = 3
        column_sender_arrow_override_not_symbol = 4

    cdef struct column_sender_arrow_override:
        const char* column
        size_t column_len
        uint32_t kind
        uint32_t arg

    bint direct_column_sender_flush_arrow_batch_at_now(
        direct_column_sender* conn,
        line_sender_table_name table,
        ArrowArray* array,
        const ArrowSchema* schema,
        const column_sender_arrow_override* overrides,
        size_t overrides_len,
        line_sender_error** err_out
        ) noexcept nogil

    bint direct_column_sender_flush_arrow_batch_at_scalar_nanos(
        direct_column_sender* conn,
        line_sender_table_name table,
        ArrowArray* array,
        const ArrowSchema* schema,
        int64_t at_nanos,
        const column_sender_arrow_override* overrides,
        size_t overrides_len,
        line_sender_error** err_out
        ) noexcept nogil

    bint direct_column_sender_flush_arrow_batch_at_column(
        direct_column_sender* conn,
        line_sender_table_name table,
        ArrowArray* array,
        const ArrowSchema* schema,
        line_sender_column_name ts_column,
        const column_sender_arrow_override* overrides,
        size_t overrides_len,
        line_sender_error** err_out
        ) noexcept nogil


cdef extern from "questdb/egress/reader.h":
    cdef struct reader:
        pass

    cdef struct reader_query:
        pass

    cdef struct reader_cursor:
        pass

    cdef struct reader_server_info:
        pass

    cdef enum reader_server_role:
        reader_server_role_standalone = 0
        reader_server_role_primary = 1
        reader_server_role_replica = 2
        reader_server_role_primary_catchup = 3
        reader_server_role_other = 0xFF

    cdef enum reader_arrow_batch_result:
        reader_arrow_batch_ok = 0
        reader_arrow_batch_end = 1
        reader_arrow_batch_error = 2

    void reader_close(
        reader* reader
        ) noexcept nogil

    const reader_server_info* reader_current_server_info(
        const reader* reader
        ) noexcept nogil

    reader_server_role reader_server_info_role(
        const reader_server_info* si
        ) noexcept nogil

    uint8_t reader_server_info_role_byte(
        const reader_server_info* si
        ) noexcept nogil

    uint64_t reader_server_info_epoch(
        const reader_server_info* si
        ) noexcept nogil

    uint32_t reader_server_info_capabilities(
        const reader_server_info* si
        ) noexcept nogil

    int64_t reader_server_info_server_wall_ns(
        const reader_server_info* si
        ) noexcept nogil

    void reader_server_info_cluster_id(
        const reader_server_info* si,
        const char** out_buf,
        size_t* out_len
        ) noexcept nogil

    void reader_server_info_node_id(
        const reader_server_info* si,
        const char** out_buf,
        size_t* out_len
        ) noexcept nogil

    bint reader_server_info_zone_id(
        const reader_server_info* si,
        const char** out_buf,
        size_t* out_len
        ) noexcept nogil

    reader_query* reader_prepare(
        reader* reader,
        line_sender_utf8 sql,
        questdb_error** err_out
        ) noexcept nogil

    void reader_query_free(
        reader_query* query
        ) noexcept nogil

    reader_cursor* reader_query_execute(
        reader_query** query_inout,
        questdb_error** err_out
        ) noexcept nogil

    void reader_query_set_reset_symbol_dict(
        reader_query* query,
        cbool reset
        ) noexcept nogil

    void reader_query_bind_bool(
        reader_query* query,
        cbool v
        ) noexcept nogil

    void reader_query_bind_i64(
        reader_query* query,
        int64_t v
        ) noexcept nogil

    void reader_query_bind_f64(
        reader_query* query,
        double v
        ) noexcept nogil

    void reader_query_bind_timestamp_micros(
        reader_query* query,
        int64_t v
        ) noexcept nogil

    void reader_query_bind_timestamp_nanos(
        reader_query* query,
        int64_t v
        ) noexcept nogil

    void reader_query_bind_varchar(
        reader_query* query,
        line_sender_utf8 v
        ) noexcept nogil

    void reader_query_bind_uuid(
        reader_query* query,
        const uint8_t* value
        ) noexcept nogil

    void reader_query_bind_null_varchar(
        reader_query* query
        ) noexcept nogil

    reader_cursor* reader_execute(
        reader* reader,
        line_sender_utf8 sql,
        questdb_error** err_out
        ) noexcept nogil

    cdef struct reader_failover_reset_event:
        pass

    ctypedef void (*reader_failover_reset_callback)(
        const reader_failover_reset_event* event,
        void* user_data) noexcept nogil

    void reader_failover_reset_event_failed_host(
        const reader_failover_reset_event* event,
        const char** out_buf,
        size_t* out_len
        ) noexcept nogil

    uint16_t reader_failover_reset_event_failed_port(
        const reader_failover_reset_event* event
        ) noexcept nogil

    void reader_failover_reset_event_new_host(
        const reader_failover_reset_event* event,
        const char** out_buf,
        size_t* out_len
        ) noexcept nogil

    uint16_t reader_failover_reset_event_new_port(
        const reader_failover_reset_event* event
        ) noexcept nogil

    int64_t reader_failover_reset_event_new_request_id(
        const reader_failover_reset_event* event
        ) noexcept nogil

    uint32_t reader_failover_reset_event_attempts(
        const reader_failover_reset_event* event
        ) noexcept nogil

    uint64_t reader_failover_reset_event_elapsed_ns(
        const reader_failover_reset_event* event
        ) noexcept nogil

    questdb_error_code reader_failover_reset_event_trigger_code(
        const reader_failover_reset_event* event
        ) noexcept nogil

    void reader_failover_reset_event_trigger_msg(
        const reader_failover_reset_event* event,
        const char** out_buf,
        size_t* out_len
        ) noexcept nogil

    void reader_query_on_failover_reset(
        reader_query* query,
        reader_failover_reset_callback callback,
        void* user_data
        ) noexcept nogil

    void reader_cursor_free(
        reader_cursor* cursor
        ) noexcept nogil

    bint reader_cursor_cancel(
        reader_cursor* cursor,
        questdb_error** err_out
        ) noexcept nogil

    reader_arrow_batch_result reader_cursor_next_arrow_batch(
        reader_cursor* cursor,
        ArrowArray* out_array,
        ArrowSchema* out_schema,
        questdb_error** err_out
        ) noexcept nogil

    reader_arrow_batch_result reader_cursor_next_arrow_batch_compact(
        reader_cursor* cursor,
        ArrowArray* out_array,
        ArrowSchema* out_schema,
        questdb_error** err_out
        ) noexcept nogil

    cdef enum reader_column_kind:
        reader_column_kind_boolean = 0x01
        reader_column_kind_byte = 0x02
        reader_column_kind_short = 0x03
        reader_column_kind_int = 0x04
        reader_column_kind_long = 0x05
        reader_column_kind_float = 0x06
        reader_column_kind_double = 0x07
        reader_column_kind_symbol = 0x09
        reader_column_kind_timestamp = 0x0A
        reader_column_kind_date = 0x0B
        reader_column_kind_uuid = 0x0C
        reader_column_kind_long256 = 0x0D
        reader_column_kind_geohash = 0x0E
        reader_column_kind_varchar = 0x0F
        reader_column_kind_timestamp_nanos = 0x10
        reader_column_kind_double_array = 0x11
        reader_column_kind_long_array = 0x12
        reader_column_kind_decimal64 = 0x13
        reader_column_kind_decimal128 = 0x14
        reader_column_kind_decimal256 = 0x15
        reader_column_kind_char = 0x16
        reader_column_kind_binary = 0x17
        reader_column_kind_ipv4 = 0x18
        reader_column_kind_unknown = 0xFF

    cdef struct reader_batch:
        pass

    cdef struct reader_column_data:
        reader_column_kind kind
        size_t row_count
        const uint8_t* validity
        const void* values
        size_t value_stride
        const uint32_t* var_offsets
        const uint8_t* var_data
        size_t var_data_len
        const uint32_t* symbol_codes
        int8_t decimal_scale
        uint8_t geohash_precision_bits

    cdef struct reader_array_data:
        reader_column_kind kind
        size_t row_count
        const uint8_t* validity
        const uint8_t* data
        size_t data_len
        const uint32_t* data_offsets
        const uint32_t* shapes
        size_t shapes_len
        const uint32_t* shape_offsets

    cdef struct reader_symbol_entry:
        uint32_t offset
        uint32_t length

    cdef struct reader_symbol_dict:
        size_t entry_count
        const uint8_t* heap
        size_t heap_len
        const reader_symbol_entry* entries

    const reader_batch* reader_cursor_next_batch(
        reader_cursor* cursor,
        questdb_error** err_out
        ) noexcept nogil

    size_t reader_batch_row_count(
        const reader_batch* batch
        ) noexcept nogil

    size_t reader_batch_column_count(
        const reader_batch* batch
        ) noexcept nogil

    bint reader_batch_column_kind(
        const reader_batch* batch,
        size_t col_idx,
        reader_column_kind* out_kind,
        questdb_error** err_out
        ) noexcept nogil

    bint reader_batch_column_name(
        const reader_batch* batch,
        size_t col_idx,
        const char** out_buf,
        size_t* out_len,
        questdb_error** err_out
        ) noexcept nogil

    bint reader_batch_column_data(
        const reader_batch* batch,
        size_t col_idx,
        reader_column_data* out,
        questdb_error** err_out
        ) noexcept nogil

    bint reader_batch_array_column_data(
        const reader_batch* batch,
        size_t col_idx,
        reader_array_data* out,
        questdb_error** err_out
        ) noexcept nogil

    bint reader_batch_symbol_dict(
        const reader_batch* batch,
        reader_symbol_dict* out,
        questdb_error** err_out
        ) noexcept nogil

    bint reader_batch_symbol(
        const reader_batch* batch,
        size_t col_idx,
        uint32_t code,
        const char** out_buf,
        size_t* out_len,
        questdb_error** err_out
        ) noexcept nogil

    void reader_drop_on_return(
        reader* reader
        ) noexcept nogil

    # Reader-pool entry points live here (alongside reader)
    # because they wrap/unwrap reader instances; the questdb_db
    # opaque is forward-declared from the ingress-pool extern block
    # above.
    reader* questdb_db_borrow_reader(
        questdb_db* db,
        questdb_error** err_out
        ) noexcept nogil

    size_t questdb_db_dbg_reader_free_count(
        questdb_db* db
        ) noexcept nogil

    size_t questdb_db_dbg_reader_in_use_count(
        questdb_db* db
        ) noexcept nogil
