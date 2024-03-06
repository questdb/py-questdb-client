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

from libc.stdint cimport int64_t, uint16_t, uint64_t

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

    cdef enum line_sender_protocol:
        line_sender_protocol_tcp,
        line_sender_protocol_tcps,
        line_sender_protocol_http,
        line_sender_protocol_https,

    cdef enum line_sender_ca:
        line_sender_ca_webpki_roots,
        line_sender_ca_os_roots,
        line_sender_ca_webpki_and_os_roots,
        line_sender_ca_pem_file,

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
        ) noexcept nogil

    line_sender_buffer* line_sender_buffer_with_max_name_len(
        size_t max_name_len
        ) noexcept nogil

    void line_sender_buffer_free(
        line_sender_buffer* buffer
        ) noexcept nogil

    line_sender_buffer* line_sender_buffer_clone(
        const line_sender_buffer* buffer
        ) noexcept nogil

    void line_sender_buffer_reserve(
        line_sender_buffer* buffer,
        size_t additional
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

    const char* line_sender_buffer_peek(
        const line_sender_buffer* buffer,
        size_t* len_out
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

    bint line_sender_opts_max_buf_size(
        line_sender_opts* opts,
        size_t max_buf_size,
        line_sender_error** err_out
        ) noexcept nogil

    bint line_sender_opts_retry_timeout(
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

    bint line_sender_must_close(
        const line_sender* sender
        ) noexcept nogil

    void line_sender_close(
        line_sender* sender
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