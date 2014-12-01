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


#include <R.h>
#include <R_ext/Rdynload.h>
#include <R_ext/Visibility.h>

#include "rkvstore.h"
#include "symbols.h"

static const R_CallMethodDef callMethods[] = {
    {".rkv_open_store", (DL_FUNC)rkv_open_store, 4},
    {".rkv_close_store", (DL_FUNC)rkv_close_store, 1},
    {".rkv_create_key", (DL_FUNC)rkv_create_key, 3},
    {".rkv_create_key_from_uri", (DL_FUNC)rkv_create_key_from_uri, 2},
    {".rkv_get_key_uri", (DL_FUNC)rkv_get_key_uri, 1},
    /*{".rkv_get_key_major", (DL_FUNC)rkv_get_key_major, 1},
    {".rkv_get_key_minor", (DL_FUNC)rkv_get_key_minor, 1}, */
    {".rkv_release_key", (DL_FUNC)rkv_release_key, 1},
    {".rkv_create_value", (DL_FUNC)rkv_create_value, 2},
    {".rkv_get_value", (DL_FUNC)rkv_get_value, 1},
    {".rkv_get_avro_value", (DL_FUNC)rkv_get_avro_value, 1},
    {".rkv_release_value", (DL_FUNC)rkv_release_value, 1},
    {".rkv_put", (DL_FUNC)rkv_put, 3},
    {".rkv_get", (DL_FUNC)rkv_get, 2},
    {".rkv_delete", (DL_FUNC)rkv_delete, 2},
    {".rkv_multi_delete", (DL_FUNC)rkv_multi_delete, 4},
    {".rkv_multiget_values", (DL_FUNC)rkv_multiget_values, 5},
    {".rkv_multiget_iterator", (DL_FUNC)rkv_multiget_iterator, 5},
    {".rkv_store_iterator", (DL_FUNC)rkv_store_iterator, 5},
    {".rkv_iterator_next", (DL_FUNC)rkv_iterator_next, 1},
    {".rkv_iterator_get_key", (DL_FUNC)rkv_iterator_get_key, 1},
    {".rkv_iterator_get_value", (DL_FUNC)rkv_iterator_get_value, 1},
    {".rkv_iterator_size", (DL_FUNC)rkv_iterator_size, 1},
    {".rkv_release_iterator", (DL_FUNC)rkv_release_iterator, 1},
    {".rkv_create_avro_value", (DL_FUNC)rkv_create_avro_value, 2},
    {".rkv_avro_value_set_int", (DL_FUNC)rkv_avro_value_set_int, 3},
    {".rkv_avro_value_get_int", (DL_FUNC)rkv_avro_value_get_int, 2},
    {".rkv_avro_value_set_long", (DL_FUNC)rkv_avro_value_set_long, 3},
    {".rkv_avro_value_get_long", (DL_FUNC)rkv_avro_value_get_long, 2},
    {".rkv_avro_value_set_string", (DL_FUNC)rkv_avro_value_set_string, 3},
    {".rkv_avro_value_get_string", (DL_FUNC)rkv_avro_value_get_string, 2},
    {".rkv_avro_value_set_double", (DL_FUNC)rkv_avro_value_set_double, 3},
    {".rkv_avro_value_get_double", (DL_FUNC)rkv_avro_value_get_double, 2},
    {".rkv_avro_value_set_boolean", (DL_FUNC)rkv_avro_value_set_boolean, 3},
    {".rkv_avro_value_get_boolean", (DL_FUNC)rkv_avro_value_get_boolean, 2},
    {".rkv_avro_value_print", (DL_FUNC)rkv_avro_value_print, 1},
    {".rkv_release_avro_value", (DL_FUNC)rkv_release_avro_value, 1},
    {NULL, NULL, 0}
};

void attribute_visible R_init_rkvstore(DllInfo *dll) {
    R_registerRoutines(dll, NULL, callMethods, NULL, NULL);
    install_kvstore_symbols();
    Rprintf("rkvstore package (rkvstore-driver) loaded\n"
            "Use 'help(\"rkvstore\")' to get started.\n\n");
}
