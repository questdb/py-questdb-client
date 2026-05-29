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
        line_sender_error_server_rejection

    cdef enum line_sender_protocol:
        line_sender_protocol_tcp,
        line_sender_protocol_tcps,
        line_sender_protocol_http,
        line_sender_protocol_https,
        line_sender_protocol_qwpudp,
        line_sender_protocol_qwpws,
        line_sender_protocol_qwpwss,
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
        LINE_SENDER_QWPWS_ERROR_PROTOCOL_VIOLATION,
        LINE_SENDER_QWPWS_ERROR_UNKNOWN,

    cdef enum line_sender_qwpws_error_policy:
        LINE_SENDER_QWPWS_ERROR_DROP_AND_CONTINUE,
        LINE_SENDER_QWPWS_ERROR_HALT,

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
        ) noexcept with gil

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
        const line_sender_buffer* buffer
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
        const line_sender_opts* opts
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

    bint line_sender_qwpws_await_acked_fsn(
        line_sender* sender,
        uint64_t fsn,
        uint64_t timeout_millis,
        cbool* reached_out,
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

    cdef struct qwpws_conn:
        pass

    cdef struct column_sender_chunk:
        pass

    cdef struct column_sender_validity:
        const uint8_t* bits
        size_t bit_len

    cdef enum column_sender_ack_level:
        column_sender_ack_level_ok
        column_sender_ack_level_durable

    questdb_db* questdb_db_connect(
        const char* conf,
        size_t conf_len,
        line_sender_error** err_out
        ) noexcept nogil

    void questdb_db_close(
        questdb_db* db
        ) noexcept nogil

    qwpws_conn* questdb_db_borrow_conn(
        questdb_db* db,
        line_sender_error** err_out
        ) noexcept nogil

    void questdb_db_return_conn(
        questdb_db* db,
        qwpws_conn* conn
        ) noexcept nogil

    void questdb_db_drop_conn(
        questdb_db* db,
        qwpws_conn* conn
        ) noexcept nogil

    size_t questdb_db_reap_idle(
        questdb_db* db
        ) noexcept nogil

    bint qwpws_conn_must_close(
        const qwpws_conn* conn
        ) noexcept nogil

    column_sender_chunk* column_sender_chunk_new(
        const char* table_name,
        size_t table_name_len,
        line_sender_error** err_out
        ) noexcept nogil

    void column_sender_chunk_free(
        column_sender_chunk* chunk
        ) noexcept nogil

    void column_sender_chunk_clear(
        column_sender_chunk* chunk
        ) noexcept nogil

    size_t column_sender_chunk_row_count(
        const column_sender_chunk* chunk
        ) noexcept nogil

    bint column_sender_chunk_column_i8(
        column_sender_chunk* chunk,
        const char* name,
        size_t name_len,
        const int8_t* data,
        size_t row_count,
        const column_sender_validity* validity,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_column_i16(
        column_sender_chunk* chunk,
        const char* name,
        size_t name_len,
        const int16_t* data,
        size_t row_count,
        const column_sender_validity* validity,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_column_i32(
        column_sender_chunk* chunk,
        const char* name,
        size_t name_len,
        const int32_t* data,
        size_t row_count,
        const column_sender_validity* validity,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_column_i64(
        column_sender_chunk* chunk,
        const char* name,
        size_t name_len,
        const int64_t* data,
        size_t row_count,
        const column_sender_validity* validity,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_column_f32(
        column_sender_chunk* chunk,
        const char* name,
        size_t name_len,
        const float* data,
        size_t row_count,
        const column_sender_validity* validity,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_column_f64(
        column_sender_chunk* chunk,
        const char* name,
        size_t name_len,
        const double* data,
        size_t row_count,
        const column_sender_validity* validity,
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

    bint column_sender_chunk_column_uuid(
        column_sender_chunk* chunk,
        const char* name,
        size_t name_len,
        const uint8_t* data,
        size_t row_count,
        const column_sender_validity* validity,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_column_long256(
        column_sender_chunk* chunk,
        const char* name,
        size_t name_len,
        const uint8_t* data,
        size_t row_count,
        const column_sender_validity* validity,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_column_ipv4(
        column_sender_chunk* chunk,
        const char* name,
        size_t name_len,
        const uint32_t* data,
        size_t row_count,
        const column_sender_validity* validity,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_column_ts_nanos(
        column_sender_chunk* chunk,
        const char* name,
        size_t name_len,
        const int64_t* data,
        size_t row_count,
        const column_sender_validity* validity,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_column_ts_micros(
        column_sender_chunk* chunk,
        const char* name,
        size_t name_len,
        const int64_t* data,
        size_t row_count,
        const column_sender_validity* validity,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_column_varchar(
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

    bint column_sender_chunk_symbol_dict_i8(
        column_sender_chunk* chunk,
        const char* name,
        size_t name_len,
        const int8_t* codes,
        size_t row_count,
        const int32_t* dict_offsets,
        size_t dict_offsets_len,
        const uint8_t* dict_bytes,
        size_t dict_bytes_len,
        const column_sender_validity* validity,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_symbol_dict_i16(
        column_sender_chunk* chunk,
        const char* name,
        size_t name_len,
        const int16_t* codes,
        size_t row_count,
        const int32_t* dict_offsets,
        size_t dict_offsets_len,
        const uint8_t* dict_bytes,
        size_t dict_bytes_len,
        const column_sender_validity* validity,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_symbol_dict_i32(
        column_sender_chunk* chunk,
        const char* name,
        size_t name_len,
        const int32_t* codes,
        size_t row_count,
        const int32_t* dict_offsets,
        size_t dict_offsets_len,
        const uint8_t* dict_bytes,
        size_t dict_bytes_len,
        const column_sender_validity* validity,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_designated_timestamp_micros(
        column_sender_chunk* chunk,
        const int64_t* data,
        size_t row_count,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_designated_timestamp_nanos(
        column_sender_chunk* chunk,
        const int64_t* data,
        size_t row_count,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_chunk_append_arrow_column(
        column_sender_chunk* chunk,
        const char* name,
        size_t name_len,
        const ArrowArray* array,
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

    bint column_sender_chunk_append_numpy_column(
        column_sender_chunk* chunk,
        const char* name,
        size_t name_len,
        column_sender_numpy_dtype dtype,
        const uint8_t* data,
        size_t row_count,
        const column_sender_validity* validity,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_flush(
        qwpws_conn* conn,
        column_sender_chunk* chunk,
        line_sender_error** err_out
        ) noexcept nogil

    bint column_sender_sync(
        qwpws_conn* conn,
        column_sender_ack_level ack_level,
        line_sender_error** err_out
        ) noexcept nogil


cdef extern from "questdb/egress/line_reader.h":
    cdef struct line_reader:
        pass

    cdef struct line_reader_query:
        pass

    cdef struct line_reader_cursor:
        pass

    cdef struct line_reader_error:
        pass

    cdef enum line_reader_error_code:
        line_reader_error_could_not_resolve_addr = 0
        line_reader_error_config_error = 1
        line_reader_error_invalid_api_call = 2
        line_reader_error_socket_error = 3
        line_reader_error_tls_error = 4
        line_reader_error_handshake_error = 5
        line_reader_error_auth_error = 6
        line_reader_error_unsupported_server = 7
        line_reader_error_role_mismatch = 8
        line_reader_error_protocol_error = 9
        line_reader_error_invalid_utf8 = 10
        line_reader_error_invalid_bind = 11
        line_reader_error_server_schema_mismatch = 14
        line_reader_error_server_parse_error = 15
        line_reader_error_server_internal_error = 16
        line_reader_error_server_security_error = 17
        line_reader_error_limit_exceeded = 18
        line_reader_error_server_limit_exceeded = 19
        line_reader_error_cancelled = 20
        line_reader_error_failover_would_duplicate = 21
        line_reader_error_schema_drift = 22
        line_reader_error_no_schema = 23
        line_reader_error_arrow_export = 24

    cdef enum line_reader_arrow_batch_result:
        line_reader_arrow_batch_ok = 0
        line_reader_arrow_batch_end = 1
        line_reader_arrow_batch_error = 2

    line_reader_error_code line_reader_error_get_code(
        const line_reader_error* error
        ) noexcept nogil

    const char* line_reader_error_msg(
        const line_reader_error* error,
        size_t* len_out
        ) noexcept nogil

    void line_reader_error_free(
        line_reader_error* error
        ) noexcept nogil

    line_reader* line_reader_from_conf(
        line_sender_utf8 config,
        line_reader_error** err_out
        ) noexcept nogil

    void line_reader_close(
        line_reader* reader
        ) noexcept nogil

    line_reader_query* line_reader_prepare(
        line_reader* reader,
        line_sender_utf8 sql,
        line_reader_error** err_out
        ) noexcept nogil

    void line_reader_query_free(
        line_reader_query* query
        ) noexcept nogil

    line_reader_cursor* line_reader_query_execute(
        line_reader_query** query_inout,
        line_reader_error** err_out
        ) noexcept nogil

    line_reader_cursor* line_reader_execute(
        line_reader* reader,
        line_sender_utf8 sql,
        line_reader_error** err_out
        ) noexcept nogil

    void line_reader_cursor_free(
        line_reader_cursor* cursor
        ) noexcept nogil

    bint line_reader_cursor_cancel(
        line_reader_cursor* cursor,
        line_reader_error** err_out
        ) noexcept nogil

    line_reader_arrow_batch_result line_reader_cursor_next_arrow_batch(
        line_reader_cursor* cursor,
        ArrowArray* out_array,
        ArrowSchema* out_schema,
        line_reader_error** err_out
        ) noexcept nogil

    void line_reader_mark_must_close(
        line_reader* reader
        ) noexcept nogil

    # Reader-pool entry points. Same FFI surface as questdb_db_*_conn
    # but for line_reader handles. Live here (alongside line_reader)
    # because they wrap/unwrap line_reader instances; the questdb_db
    # opaque is forward-declared from the column_sender extern block
    # above.
    line_reader* questdb_db_borrow_reader(
        questdb_db* db,
        line_reader_error** err_out
        ) noexcept nogil

    void questdb_db_return_reader(
        questdb_db* db,
        line_reader* reader
        ) noexcept nogil

    size_t questdb_db_reader_free_count(
        questdb_db* db
        ) noexcept nogil

    size_t questdb_db_reader_in_use_count(
        questdb_db* db
        ) noexcept nogil
