#pragma once

#include <stdint.h>
#include <stdbool.h>

#if defined(__cplusplus)
extern "C" {
#endif

typedef struct questdb_conf_str questdb_conf_str;

struct questdb_conf_str_parse_err {
    const char* msg;
    size_t msg_len;
    size_t pos;
};

typedef struct questdb_conf_str_parse_err questdb_conf_str_parse_err;

void questdb_conf_str_parse_err_free(questdb_conf_str_parse_err* err);

typedef struct questdb_conf_str_iter questdb_conf_str_iter;

questdb_conf_str* questdb_conf_str_parse(
    const char* str,
    size_t len,
    questdb_conf_str_parse_err** err_out);

const char* questdb_conf_str_service(
    const questdb_conf_str* conf_str,
    size_t* len_out);

const char* questdb_conf_str_get(
    const questdb_conf_str* conf_str,
    const char* key,
    size_t key_len,
    size_t* val_len_out);

questdb_conf_str_iter* questdb_conf_str_iter_pairs(
    const questdb_conf_str* conf_str);

bool questdb_conf_str_iter_next(
    questdb_conf_str_iter* iter,
    const char** key_out,
    size_t* key_len_out,
    const char** val_out,
    size_t* val_len_out);

void questdb_conf_str_iter_free(questdb_conf_str_iter* iter);

void questdb_conf_str_free(questdb_conf_str* str);

#if defined(__cplusplus)
}
#endif
