# The C API is compiled through the `questdb-rs-ffi` via the `conf_str` feature.
# Search for the "confstr-ffi" feature in the code base to understand how it all
# wires up together.

# See: https://github.com/questdb/questdb-confstr-rs/blob/
#         0.1.0/questdb-confstr-ffi/include/questdb/conf_str.h

cdef extern from "conf_str.h":
    cdef struct questdb_conf_str:
        pass

    cdef struct questdb_conf_str_parse_err:
        const char* msg
        size_t msg_len
        size_t pos

    void questdb_conf_str_parse_err_free(
        questdb_conf_str_parse_err* err
        ) noexcept nogil

    cdef struct questdb_conf_str_iter:
        pass

    questdb_conf_str* questdb_conf_str_parse(
        const char* string,
        size_t len,
        questdb_conf_str_parse_err** err_out
        ) noexcept nogil

    const char* questdb_conf_str_service(
        const questdb_conf_str* conf_str,
        size_t* len_out
        ) noexcept nogil

    const char* questdb_conf_str_get(
        const questdb_conf_str* conf_str,
        const char* key,
        size_t key_len,
        size_t* val_len_out
        ) noexcept nogil
    
    questdb_conf_str_iter* questdb_conf_str_iter_pairs(
        const questdb_conf_str* conf_str
        ) noexcept nogil

    bint questdb_conf_str_iter_next(
        questdb_conf_str_iter* iter,
        const char** key_out,
        size_t* key_len_out,
        const char** val_out,
        size_t* val_len_out
        ) noexcept nogil

    void questdb_conf_str_iter_free(
        questdb_conf_str_iter* iter
        ) noexcept nogil

    void questdb_conf_str_free(
        questdb_conf_str* str
        ) noexcept nogil
