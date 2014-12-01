/*-
 *
 *  This file is part of Oracle NoSQL Database
 *  Copyright (C) 2011, 2014 Oracle and/or its affiliates.  All rights reserved.
 *
 * If you have received this file as part of Oracle NoSQL Database the
 * following applies to the work as a whole:
 *
 *   Oracle NoSQL Database server software is free software: you can
 *   redistribute it and/or modify it under the terms of the GNU Affero
 *   General Public License as published by the Free Software Foundation,
 *   version 3.
 *
 *   Oracle NoSQL Database is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *   Affero General Public License for more details.
 *
 * If you have received this file as part of Oracle NoSQL Database Client or
 * distributed separately the following applies:
 *
 *   Oracle NoSQL Database client software is free software: you can
 *   redistribute it and/or modify it under the terms of the Apache License
 *   as published by the Apache Software Foundation, version 2.0.
 *
 * You should have received a copy of the GNU Affero General Public License
 * and/or the Apache License in the LICENSE file along with Oracle NoSQL
 * Database client or server distribution.  If not, see
 * <http://www.gnu.org/licenses/>
 * or
 * <http://www.apache.org/licenses/LICENSE-2.0>.
 *
 * An active Oracle commercial licensing agreement for this product supersedes
 * these licenses and in such case the license notices, but not the copyright
 * notice, may be removed by you in connection with your distribution that is
 * in accordance with the commercial licensing terms.
 *
 * For more information please contact:
 *
 * berkeleydb-info_us@oracle.com
 *
 */


#include "utils.h"
#include "rkvstore_internal.h"

static kv_impl_t *kv_jni_impl = NULL;
static kv_error_t init_kvstore_jni_impl(const char *path);

rkv_error_t r_kvstore_open(const char *path, const char *storename,
                          const char *host, int port,
                          kv_store_t ** ret_store) {
    kv_error_t ret;
    kv_store_t *store = NULL;
    kv_config_t *config = NULL;
    kv_impl_t *impl = NULL;

#if DEBUG
    PRINTF("kvstore_open->storename:%s, host:%s, port:%d, classpath:%s",
           storename, host, port, path);
#endif

    /* Init kvstore jni impl */
    if ((ret = init_kvstore_jni_impl(path)) != KV_SUCCESS) {
        return RKV_ERROR;
    }
    impl = kv_jni_impl;

#if DEBUG
    PRINTF("init_kvstore_jni_impl done!");
#endif

    ret = kv_create_config(&config, storename, host, port);
    if (ret != KV_SUCCESS) {
        return RKV_ERROR;
    }
#if DEBUG
    PRINTF("kv_create_config done!");
#endif

    ret = kv_open_store(impl, &store, config);
    if (ret != KV_SUCCESS) {
	    const char *open_error = kv_get_open_error(impl);
        if (!open_error) {
	        open_error = "no additional information";
        }
	    Rprintf("Open kvstore failed: %s\n", open_error);
	    kv_release_config(&config);
        return RKV_ERROR;
    }

    if (ret_store != NULL) {
        *ret_store = store;
    }

    Rprintf("Connected.\n");
    return RKV_SUCCESS;
}

static kv_error_t init_kvstore_jni_impl(const char *path) {
    kv_impl_t *impl = NULL;
    kv_error_t ret;

    if (kv_jni_impl != NULL) {
        return KV_SUCCESS;
    }

    ret = kv_create_jni_impl(&impl, path);
    if (ret == KV_SUCCESS) {
        kv_jni_impl = impl;
    }
    return ret;
}

rkv_error_t r_kvstore_close(kv_store_t *store) {
    kv_error_t ret = kv_close_store(store);
    if (kv_jni_impl != NULL) {
        kv_release_impl(&kv_jni_impl);
        kv_jni_impl = NULL;
    }
    RETURN_MAP_TO_RERR(ret);
}

rkv_error_t r_kv_create_key(kv_store_t *store, kv_key_t **ret_key,
                           const char **major, const char **minor) {
    kv_key_t *key = NULL;
    kv_error_t ret;

    if (!store || !ret_key || !major) {
        return RKV_INVALID_ARGUEMENTS;
    }

    ret = kv_create_key_copy(store, &key, major, minor);
    RETURN_RERR_IF_ERR(ret);

    *ret_key = key;
    return RKV_SUCCESS;
}

rkv_error_t r_kv_create_key_from_uri(kv_store_t *store, kv_key_t **ret_key,
                                   const char *uri) {
    kv_key_t *key = NULL;
    kv_error_t ret;

    if (!store || !ret_key || !uri) {
        return RKV_INVALID_ARGUEMENTS;
    }

    ret = kv_create_key_from_uri_copy(store, &key, uri);
    RETURN_RERR_IF_ERR(ret);

    *ret_key = key;
    return RKV_SUCCESS;
}

void r_kv_release_key(kv_key_t **key) {
    kv_release_key(key);
}

rkv_error_t r_kv_create_value_bytes(kv_store_t *store, kv_value_t **ret_value,
                                    const unsigned char *data, int data_len) {
    kv_error_t ret;
    kv_value_t *value = NULL;

    if (!store || !ret_value || (!data || !data_len)) {
        return RKV_INVALID_ARGUEMENTS;
    }

    ret = kv_create_value_copy(store, &value, data, data_len);
    RETURN_RERR_IF_ERR(ret);
    *ret_value = value;

    return RKV_SUCCESS;
}

rkv_error_t r_kv_create_value_avro(kv_store_t *store,
                                   kv_value_t **ret_value,
                                   avro_value_t *avro_value) {

    kv_error_t ret;
    kv_value_t *value = NULL;

    if (!store || !ret_value || !avro_value) {
        return RKV_INVALID_ARGUEMENTS;
    }

    ret = kv_avro_generic_to_value(store, avro_value, &value);
    RETURN_RERR_IF_ERR(ret);
    *ret_value = value;

    return RKV_SUCCESS;
}

void r_kv_release_value(kv_value_t **value) {
    kv_release_value(value);
}


rkv_error_t r_kv_put(kv_store_t *store, const kv_key_t *key,
		            const kv_value_t *value,
                    kv_version_t ** ret_new_version) {
    kv_version_t *version = NULL;
    kv_error_t err;

    if (!store || !key || !value) {
        return RKV_INVALID_ARGUEMENTS;
    }

    err = kv_put(store, key, value, &version);
    RETURN_RERR_IF_ERR(err);

    if (ret_new_version) {
        *ret_new_version = version;
    }
    return RKV_SUCCESS;
}

rkv_error_t r_kv_get(kv_store_t *store, const kv_key_t *key,
                     kv_value_t ** ret_value) {
    kv_error_t err;
    kv_value_t *value = NULL;

    if (!store || !key || !ret_value) {
        return RKV_INVALID_ARGUEMENTS;
    }

    err = kv_get(store, key, &value);
    RETURN_RERR_IF_ERR(err);

    *ret_value = value;
    return RKV_SUCCESS;
}

int r_kv_delete(kv_store_t *store, const kv_key_t *key) {
    int ret;

    if (!store || !key) {
        return RKV_INVALID_ARGUEMENTS;
    }

    ret = kv_delete(store, key);
    if (ret > 0) {
        return ret;
    }
    RETURN_MAP_TO_RERR(ret);
}

rkv_error_t r_kv_get_key_uri(const kv_key_t *key,
                             const char ** ret_key_uri) {
    char *key_uri = NULL;
    if (!key) {
        return RKV_INVALID_ARGUEMENTS;
    }
    key_uri = (char *)kv_get_key_uri(key);
    if (ret_key_uri) {
        *ret_key_uri = key_uri;
    }
    return RKV_SUCCESS;
}

rkv_error_t r_kv_get_value(const kv_value_t *value,
                           const unsigned char **ret_value,
                           int *ret_value_len,
                           int *needFree) {
    const unsigned char *p_value = NULL;
    avro_value_t avro_value;
    kv_error_t err;

    if (!value || !ret_value) {
        return RKV_INVALID_ARGUEMENTS;
    }

    err = kv_avro_generic_to_object((kv_value_t *)value, &avro_value, NULL);
    if (err == KV_SUCCESS) { //AVRO type
        char *json_str = NULL;
        int ret;
        ret = avro_value_to_json(&avro_value, 1, &json_str);
        if (!ret) {
            *ret_value = (const unsigned char *)json_str;
            if (ret_value_len) {
                *ret_value_len = strlen(json_str);
            }
            avro_value_decref(&avro_value);
            if (needFree) {
                *needFree = 1;
            }
        } else {
            err = (ret == ENOMEM)?KV_NO_MEMORY:KV_ERROR_UNKNOWN;
        }
    } else {
        p_value = kv_get_value(value);
        *ret_value = p_value;
        if (ret_value_len) {
            *ret_value_len = kv_get_value_size(value);
        }
        err = KV_SUCCESS;
    }

    RETURN_MAP_TO_RERR(err);
}

rkv_error_t r_kv_get_avrovalue(const kv_value_t *value,
                               avro_value_t **ret_avro_value,
                               avro_schema_t schema) {

    avro_value_t *avro_value = NULL;
    kv_error_t err;
    rkv_error_t ret;

    if (!value || !ret_avro_value) {
        return RKV_INVALID_ARGUEMENTS;
    }

    ret = rkv_malloc(sizeof(avro_value_t), (void**)&avro_value);
    RETURN_IF_ERR(ret);

    err = kv_avro_generic_to_object((kv_value_t *)value, avro_value, schema);
    if (err != KV_SUCCESS) {
        return RKV_ERROR;
    }
    *ret_avro_value = avro_value;
    return RKV_SUCCESS;
}

int r_kv_multi_delete(kv_store_t *store,
                     const kv_key_t *parent_key,
                     const char *start,
                     const char *end) {

    kv_key_range_t key_range, *sub_range = NULL;
    kv_int_t nRec = 0;

    if (!store || !parent_key) {
        return RKV_INVALID_ARGUEMENTS;
    }

    if (start || end) {
        memset(&key_range, 0, sizeof(key_range));
        kv_init_key_range(&key_range, start, 1, end, 1);
        sub_range = &key_range;
    }

    nRec = kv_multi_delete(store, parent_key, sub_range,
                           KV_DEPTH_DEFAULT, 0, 0);
    return nRec;
}


rkv_error_t rkv_get_iterator(kv_store_t *store,
                            const kv_key_t *parent_key,
                            kv_iterator_t **return_iterator,
                            const char *start,
                            const char *end,
                            int isKeyOnly,
                            int isMultiGet) {
    kv_iterator_t *iterator = NULL;
    kv_error_t err;
    kv_key_range_t key_range, *sub_range = NULL;

    if (!store || (isMultiGet && !parent_key) || !return_iterator) {
        return RKV_INVALID_ARGUEMENTS;
    }

    if (start || end) {
        memset(&key_range, 0, sizeof(key_range));
        kv_init_key_range(&key_range, start, 1, end, 1);
        sub_range = &key_range;
    }

    if (isKeyOnly) {
        if (isMultiGet) {
            err = kv_multi_get_keys(store, parent_key, &iterator,
                                    sub_range, KV_DEPTH_DEFAULT,
                                    NULL, 0);
        } else {
            err = kv_store_iterator_keys(store, parent_key, &iterator,
                                        sub_range, KV_DEPTH_DEFAULT,
                                        KV_DIRECTION_UNORDERED,
                                        100, NULL, 0);
        }
    } else {
        if (isMultiGet) {
            err = kv_multi_get(store, parent_key, &iterator,
                                sub_range, KV_DEPTH_DEFAULT,
                                NULL, 0);
        } else {
            err = kv_store_iterator(store, parent_key, &iterator,
                                    sub_range, KV_DEPTH_DEFAULT,
                                    KV_DIRECTION_UNORDERED,
                                    100, NULL, 0);
        }
    }
    RETURN_RERR_IF_ERR(err);

    *return_iterator = iterator;
    return RKV_SUCCESS;
}

rkv_error_t r_kv_iterator_size(kv_iterator_t *iterator, int * ret_size) {
    int size = 0;

    if (!iterator || !ret_size) {
        return RKV_INVALID_ARGUEMENTS;
    }

    size = kv_iterator_size(iterator);
    if (size == KV_INVALID_OPERATION) {
        return RKV_ITR_NO_SIZE_INFO;
    }

    *ret_size = size;
    return RKV_SUCCESS;
}

rkv_error_t r_kv_iterator_next(kv_iterator_t *iterator,
                               const kv_key_t **ret_key,
                               const kv_value_t **ret_value) {
    const kv_key_t *key = NULL;
    const kv_value_t *value = NULL;
    kv_error_t err;

    if (!iterator || !ret_key ) {
        return RKV_INVALID_ARGUEMENTS;
    }
    /* A bug in C library here..
       Get a segment fault error from kv_iterator_next_key */
    err = kv_iterator_next(iterator, &key, &value);
    if (err == KV_NO_SUCH_OBJECT) {
        return RKV_NO_MORE_DATA;
    }
    RETURN_RERR_IF_ERR(err);

    *ret_key = key;
    if (ret_value) {
        *ret_value = value;
    }
    return RKV_SUCCESS;
}

void r_kv_release_iterator(kv_iterator_t **iterator) {
    kv_release_iterator(iterator);
}

rkv_error_t r_kv_create_avro_value(kv_store_t *kvstore,
                                   const char *space,
                                   const char *schemaName,
                                   avro_value_t **ret_avro_value) {
    int ret;
    avro_schema_t schema = NULL;
    avro_value_iface_t *iface = NULL;
    avro_value_t *avro_value = NULL;

    if (!kvstore || !schemaName || !ret_avro_value) {
        return RKV_INVALID_ARGUEMENTS;
    }

    if ((schema = r_kv_get_schema(kvstore, space, schemaName)) == NULL) {
        return RKV_INVALID_SCHEMA;
    }

    ret = rkv_malloc(sizeof(avro_value_t), (void**)&avro_value);
    RETURN_IF_ERR(ret);

    iface = avro_generic_class_from_schema(schema);
    if (avro_generic_value_new(iface, avro_value) != 0) {
        return RKV_ERROR;
    }

    *ret_avro_value = avro_value;
    return RKV_SUCCESS;
}

avro_schema_t r_kv_get_schema(kv_store_t *kvstore,
                             const char *space,
                             const char *name) {
    int nsch, i;
    avro_schema_t *schemas = NULL, schema = NULL;

    if (!kvstore || !name) {
        return NULL;
    }

    schema = avro_schema_record(name, space);
    nsch = kv_avro_get_current_schemas(kvstore, &schemas);
    PRINTF("r_kv_get_schema, name = %s, space = %s, nsch = %d\n", name, space, nsch);
    for (i = 0; i < nsch; i++) {
        avro_schema_t sch = schemas[i];
        if (avro_typeof(sch) == AVRO_RECORD &&
            avro_schema_equal(schema, sch)) {
            avro_schema_decref(schema);
            return sch;
        }
    }
    avro_schema_decref(schema);
    return NULL;
}

void r_kv_release_avro_value(avro_value_t *avro_value) {
    if (!avro_value) {
        return;
    }
    avro_value_decref(avro_value);
    free(avro_value);
}

rkv_error_t r_kv_avro_value_set_int(avro_value_t *avro_value,
                                    const char *name,
                                    int value) {
    avro_value_t field;
    rkv_error_t ret = RKV_SUCCESS;

    if (!avro_value || !name) {
        return RKV_INVALID_ARGUEMENTS;
    }

    if (avro_value_get_by_name(avro_value, name, &field, NULL) != 0) {
        return RKV_INVALID_ARGUEMENTS;
    }
    if (avro_value_set_int(&field, value) == EINVAL) {
        ret = RKV_INVALID_AVRO_SET_OP;
    }
    //avro_value_decref(&field);
    return ret;
}

rkv_error_t r_kv_avro_value_get_int(avro_value_t *avro_value,
                                    const char *name,
                                    int *ret_value) {
    avro_value_t field;
    rkv_error_t ret = RKV_SUCCESS;
    int value = 0;

    if (!avro_value || !name || !ret_value) {
        return RKV_INVALID_ARGUEMENTS;
    }

    if (avro_value_get_by_name(avro_value, name, &field, NULL) != 0) {
        return RKV_INVALID_ARGUEMENTS;
    }
    if (avro_value_get_int(&field, &value) == EINVAL) {
        ret = RKV_INVALID_AVRO_SET_OP;
    }
    *ret_value = value;
    //avro_value_decref(&field);
    return ret;
}

rkv_error_t r_kv_avro_value_set_long(avro_value_t *avro_value,
                                     const char *name,
                                     long value) {
    avro_value_t field;
    rkv_error_t ret = RKV_SUCCESS;

    if (!avro_value || !name) {
        return RKV_INVALID_ARGUEMENTS;
    }

    if (avro_value_get_by_name(avro_value, name, &field, NULL) != 0) {
        return RKV_INVALID_ARGUEMENTS;
    }
    if (avro_value_set_long(&field, value) == EINVAL) {
        ret = RKV_INVALID_AVRO_SET_OP;
    }
    //avro_value_decref(&field);
    return ret;
}

rkv_error_t r_kv_avro_value_get_long(avro_value_t *avro_value,
                                     const char *name,
                                     int64_t *ret_value) {
    avro_value_t field;
    rkv_error_t ret = RKV_SUCCESS;
    int64_t value = 0;

    if (!avro_value || !name || !ret_value) {
        return RKV_INVALID_ARGUEMENTS;
    }

    if (avro_value_get_by_name(avro_value, name, &field, NULL) != 0) {
        return RKV_INVALID_ARGUEMENTS;
    }
    if (avro_value_get_long(&field, &value) == EINVAL) {
        ret = RKV_INVALID_AVRO_SET_OP;
    }
    *ret_value = value;
    //avro_value_decref(&field);
    return ret;
}

rkv_error_t r_kv_avro_value_set_string(avro_value_t *avro_value,
                                        const char *name,
                                        const char *value) {
    avro_value_t field;
    rkv_error_t ret = RKV_SUCCESS;

    if (!avro_value || !name) {
        return RKV_INVALID_ARGUEMENTS;
    }

    if (avro_value_get_by_name(avro_value, name, &field, NULL) != 0) {
        return RKV_INVALID_ARGUEMENTS;
    }

    if (avro_value_set_string(&field, value) == EINVAL) {
        ret = RKV_INVALID_AVRO_SET_OP;
    }
    //avro_value_decref(&field);
    return ret;
}

rkv_error_t r_kv_avro_value_get_string(avro_value_t *avro_value,
                                        const char *name,
                                        const char **ret_value,
                                        int *ret_size) {
    avro_value_t field;
    rkv_error_t ret = RKV_SUCCESS;
    const char *value;
    size_t size;

    if (!avro_value || !name || !ret_value) {
        return RKV_INVALID_ARGUEMENTS;
    }

    if (avro_value_get_by_name(avro_value, name, &field, NULL) != 0) {
        return RKV_INVALID_ARGUEMENTS;
    }

    if (avro_value_get_string(&field, &value, &size) == EINVAL) {
        ret = RKV_INVALID_AVRO_SET_OP;
    }
    *ret_value = value;
    if (ret_size) {
        *ret_size = size;
    }
    //avro_value_decref(&field);
    return ret;
}


rkv_error_t r_kv_avro_value_set_double(avro_value_t *avro_value,
                                       const char *name,
                                       double value) {
    avro_value_t field;
    rkv_error_t ret = RKV_SUCCESS;

    if (!avro_value || !name) {
        return RKV_INVALID_ARGUEMENTS;
    }

    if (avro_value_get_by_name(avro_value, name, &field, NULL) != 0) {
        return RKV_INVALID_ARGUEMENTS;
    }
    if (avro_value_set_double(&field, value) == EINVAL) {
        ret = RKV_INVALID_AVRO_SET_OP;
    }
    //avro_value_decref(&field);
    return ret;
}

rkv_error_t r_kv_avro_value_get_double(avro_value_t *avro_value,
                                       const char *name,
                                       double *ret_value) {
    avro_value_t field;
    rkv_error_t ret = RKV_SUCCESS;
    double value = 0.0;

    if (!avro_value || !name || !ret_value) {
        return RKV_INVALID_ARGUEMENTS;
    }

    if (avro_value_get_by_name(avro_value, name, &field, NULL) != 0) {
        return RKV_INVALID_ARGUEMENTS;
    }
    if (avro_value_get_double(&field, &value) == EINVAL) {
        ret = RKV_INVALID_AVRO_SET_OP;
    }
    *ret_value = value;
    //avro_value_decref(&field);
    return ret;
}

rkv_error_t r_kv_avro_value_set_boolean(avro_value_t *avro_value,
                                       const char *name,
                                       int value) {
    avro_value_t field;
    rkv_error_t ret = RKV_SUCCESS;

    if (!avro_value || !name) {
        return RKV_INVALID_ARGUEMENTS;
    }

    if (avro_value_get_by_name(avro_value, name, &field, NULL) != 0) {
        return RKV_INVALID_ARGUEMENTS;
    }

    if (avro_value_set_boolean(&field, value) == EINVAL) {
        ret = RKV_INVALID_AVRO_SET_OP;
    }
    //avro_value_decref(&field);
    return ret;
}

rkv_error_t r_kv_avro_value_get_boolean(avro_value_t *avro_value,
                                       const char *name,
                                       int *ret_value) {
    avro_value_t field;
    rkv_error_t ret = RKV_SUCCESS;
    int value = 0;

    if (!avro_value || !name || !ret_value) {
        return RKV_INVALID_ARGUEMENTS;
    }

    if (avro_value_get_by_name(avro_value, name, &field, NULL) != 0) {
        return RKV_INVALID_ARGUEMENTS;
    }

    if (avro_value_get_boolean(&field, &value) == EINVAL) {
        ret = RKV_INVALID_AVRO_SET_OP;
    }
    *ret_value = value;
    //avro_value_decref(&field);
    return ret;
}

/*rkv_error_t r_kv_avro_get_schema(const kv_value_t *value,
                                 avro_schema_t *schema) {
    kv_error_t err = kv_avro_get_schema(value, schema);
    if (value == NULL || schema == NULL) {
        return RKV_INVALID_ARGUEMENTS;
    }
    RETURN_MAP_TO_RERR(err);
}*/

/*rkv_error_t r_kv_avro_value_set_null(avro_value_t *avro_value,
                                     const char *name) {
    avro_value_t field;
    rkv_error_t ret = RKV_SUCCESS;

    if (!avro_value || !name) {
        return RKV_INVALID_ARGUEMENTS;
    }

    if (avro_value_get_by_name(avro_value, name, &field, NULL) != 0) {
        return RKV_INVALID_ARGUEMENTS;
    }
    if (avro_value_set_null(&field) == EINVAL) {
        ret = RKV_INVALID_AVRO_SET_OP;
    }
    //avro_value_decref(&field);
    return ret;
}

rkv_error_t r_kv_avro_value_set_bytes(avro_value_t *avro_value,
                                      const char *name,
                                      char *buffer, int size) {
    avro_value_t field;
    rkv_error_t ret = RKV_SUCCESS;

    if (!avro_value || !name) {
        return RKV_INVALID_ARGUEMENTS;
    }

    if (avro_value_get_by_name(avro_value, name, &field, NULL) != 0) {
        return RKV_INVALID_ARGUEMENTS;
    }
    if (avro_value_set_bytes(&field, buffer, size) == EINVAL) {
        ret = RKV_INVALID_AVRO_SET_OP;
    }
    avro_value_decref(&field);
    return ret;
}*/

void printfAvroVlaue(avro_value_t *avro_value) {
    char *json_str = NULL;
    if (avro_value_to_json(avro_value, 1, &json_str) == 0) {
        if (json_str == NULL) {
            Rprintf("json_str is null.\n");
        } else {
            Rprintf("%s\n", json_str);
            free(json_str);
        }
    }
}
