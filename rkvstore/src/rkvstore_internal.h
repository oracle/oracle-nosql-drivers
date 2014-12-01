/*-
 *
 *  This file is part of Oracle NoSQL Database
 *  Copyright (C) 2011, 2013 Oracle and/or its Affiliates
 *
 *  Oracle NoSQL Database is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU Affero General Public License
 *  as published by the Free Software Foundation, version 3.
 *
 *  Oracle NoSQL Database is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public
 *  License in the LICENSE file along with Oracle NoSQL Database.  If not,
 *  see <http://www.gnu.org/licenses/>.
 *
 *  For more information please contact:
 *
 *  Vice President Legal, Development
 *  Oracle America, Inc.
 *  5OP-10
 *  500 Oracle Parkway
 *  Redwood Shores, CA 94065
 *
 *  or
 *
 *  berkeleydb-info_us@oracle.com
 *
 */

#ifndef __RKVSTORE_INTERNAL_H__
#define __RKVSTORE_INTERNAL_H__

#include <kvstore.h>
#include "rkverr.h"

/* kvstore - open, close */
rkv_error_t r_kvstore_open(const char *path,
                           const char *storename,
                           const char *host,
                           int port,
                           kv_store_t ** ret_store);
rkv_error_t r_kvstore_close(kv_store_t *store);

/* key - create, get, release */
rkv_error_t r_kv_create_key(kv_store_t *store,
                            kv_key_t **ret_key,
                            const char **major,
                            const char **minor);
rkv_error_t r_kv_create_key_from_uri(kv_store_t *store,
                                     kv_key_t **ret_key,
                                     const char *uri);
rkv_error_t r_kv_get_key_uri(const kv_key_t *key,
                             const char ** ret_key_uri);
/*const char ** r_kv_get_key_major(const kv_key_t *key);
const char ** r_kv_get_key_minor(const kv_key_t *key); */
void r_kv_release_key(kv_key_t **key);

/* value - create, get, release */
rkv_error_t r_kv_create_value_bytes(kv_store_t *store,
                                    kv_value_t **ret_value,
                                    const unsigned char *data,
                                    int data_len);
rkv_error_t r_kv_create_value_avro(kv_store_t *store,
                                   kv_value_t **ret_value,
                                   avro_value_t *avro_value);
rkv_error_t r_kv_get_value(const kv_value_t *value,
                           const unsigned char **ret_value,
                           int *ret_value_len,
                           int *needFree);
rkv_error_t r_kv_get_avrovalue(const kv_value_t *value,
                               avro_value_t **ret_avro_value,
                               avro_schema_t schema);
void r_kv_release_value(kv_value_t **value);

/* kvstore: put, get, delete */
rkv_error_t r_kv_put(kv_store_t *store,
                     const kv_key_t *key,
		             const kv_value_t *value,
                     kv_version_t ** ret_new_version);
rkv_error_t r_kv_get(kv_store_t *store,
                     const kv_key_t *key,
                     kv_value_t ** ret_value);
int r_kv_delete(kv_store_t *store, const kv_key_t *key);
int r_kv_multi_delete(kv_store_t *store,
                     const kv_key_t *parent_key,
                     const char *start,
                     const char *end);

/* kvstore: iterator related operation */
rkv_error_t rkv_get_iterator(kv_store_t *store,
                             const kv_key_t *parent_key,
                             kv_iterator_t **return_iterator,
                             const char *start,
                             const char *end,
                             int isKeyOnly,
                             int isMultiGet);
rkv_error_t r_kv_iterator_size(kv_iterator_t *iterator, int * ret_size);
rkv_error_t r_kv_iterator_next(kv_iterator_t *iterator,
                               const kv_key_t **ret_key,
                               const kv_value_t **ret_value);
void r_kv_release_iterator(kv_iterator_t **iterator);
avro_schema_t r_kv_get_schema(kv_store_t *kvstore, const char *space,
                             const char *name);

/* AVRO related APIs */
rkv_error_t r_kv_create_avro_value(kv_store_t *kvstore,
                                   const char *space,
                                   const char *schemaName,
                                   avro_value_t **ret_avro_value);
rkv_error_t r_kv_avro_value_set_int(avro_value_t *avro_value,
                                    const char *name,
                                    int32_t value);
rkv_error_t r_kv_avro_value_get_int(avro_value_t *avro_value,
                                    const char *name,
                                    int32_t *ret_value);
rkv_error_t r_kv_avro_value_set_long(avro_value_t *avro_value,
                                     const char *name,
                                     int64_t value);
rkv_error_t r_kv_avro_value_get_long(avro_value_t *avro_value,
                                     const char *name,
                                     int64_t *ret_value);
rkv_error_t r_kv_avro_value_set_string(avro_value_t *avro_value,
                                       const char *name,
                                       const char *value);
rkv_error_t r_kv_avro_value_get_string(avro_value_t *avro_value,
                                       const char *name,
                                       const char **ret_value,
                                       int *ret_len);
rkv_error_t r_kv_avro_value_set_double(avro_value_t *avro_value,
                                       const char *name,
                                       double value);
rkv_error_t r_kv_avro_value_get_double(avro_value_t *avro_value,
                                       const char *name,
                                       double *ret_value);
rkv_error_t r_kv_avro_value_set_boolean(avro_value_t *avro_value,
                                       const char *name,
                                       int value);
rkv_error_t r_kv_avro_value_get_boolean(avro_value_t *avro_value,
                                       const char *name,
                                       int *ret_value);
/*rkv_error_t r_kv_avro_value_set_bytes(avro_value_t *avro_value,
                                      const char *name,
                                      char *buffer, int size);
rkv_error_t r_kv_avro_value_set_null(avro_value_t *avro_value,
                                     const char *name);*/
void r_kv_release_avro_value(avro_value_t *avro_value);
/*rkv_error_t r_kv_avro_get_schema(const kv_value_t *value,
                                 avro_schema_t *schema);*/

#endif
