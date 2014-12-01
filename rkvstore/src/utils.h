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

#ifndef __UTILS_H__
#define __UTILS_H__

#include <Rinternals.h>
#include <kvstore.h>

#include "rkverr.h"

#ifndef DEBUG
#define DEBUG 0
#endif

#define CLASS_KVSTORE       "kvstore"
#define CLASS_KV_KEY        "kvkey"
#define CLASS_KV_VALUE      "kvvalue"
#define CLASS_KV_ITERATOR   "kviterator"
#define CLASS_KV_AVRO_VALUE "kvavrovalue"

#define CHECK_IF_VALID_STRING(arg, name)  \
do { \
    if (!isValidString(arg) || STRING_ELT(arg, 0) == NA_STRING) { \
        ERROR_INVALID_STRING(name); \
    } \
} while(0)

#define CHECK_IF_NOT_NULL(arg, name)  \
do { \
    if (isNull(arg)) { \
        ERROR_NOT_NULL(name); \
    } \
} while(0)

#define CHECK_IF_LOGICAL(arg, name)  \
do { \
    if (!isLogical(arg)) { \
        ERROR_INVALID_LOGIC(name); \
    } \
} while(0)

#define CHECK_IF_INT(arg, name)  \
do { \
    if (!isInteger(arg)) { \
        ERROR_INVALID_INT(name); \
    } \
} while(0)

#define CHECK_IF_REAL(arg, name)  \
do { \
    if (!isReal(arg)) { \
        ERROR_INVALID_REAL(name); \
    } \
} while(0)

#define ERROR_INVALID_ARGUMENT(arg) \
    error("Invalid argument: %s.", arg)

#define ERROR_INVALID_STRING(name) \
    error("'%s' must be non-null character string.", name)

#define ERROR_INVALID_LOGIC(name) \
    error("'%s' must be a boolean value.", name)

#define ERROR_INVALID_INT(name) \
    error("'%s' must be a integer.", name)

#define ERROR_INVALID_REAL(name) \
    error("'%s' must be a real.", name)

#define ERROR_NOT_NULL(name) \
    error("'%s' must be non-null.", name)

#define CHECK_OBJ_HAS_CLASS(obj, name) \
do { \
    if (!checkObjHasClass(obj, name)) {\
        error("This object doesn't has the class %s", name); \
    } \
}while(0)

#define RETURN_RERR_IF_ERR(err) \
do { \
    if (err != KV_SUCCESS) { \
        PRINTF("err = %d\n", err); \
        RETURN_MAP_TO_RERR(err); \
    } \
}while(0)

#define RETURN_MAP_TO_RERR(err) \
do { \
    if (err == KV_KEY_NOT_FOUND) { \
        return RKV_KEY_NOT_FOUND; \
    } else if (err == KV_AVRO) { \
        return RKV_VALUE_NOT_AVRO; \
    } else if (err != KV_SUCCESS) { \
        PRINTF("err = %d\n", err); \
        return RKV_ERROR; \
    } else { \
        return RKV_SUCCESS; \
    } \
}while(0)

#define RETURN_NULL_IF_ERR(ret) \
do { \
    if (ret != RKV_SUCCESS) { \
        Rprintf("[Error]%s (err = %d).\n", getRKVStoreErrStr(ret), ret); \
        return R_NilValue; \
    } \
}while(0)

#define RETURN_IF_ERR(ret) \
do { \
    if (ret != RKV_SUCCESS) { \
        return ret; \
    } \
}while(0)

#define PRINT_ERRMSG_IF_ERR(ret) \
do { \
    if (ret != RKV_SUCCESS) { \
        Rprintf("[Error]%s (err = %d).\n", getRKVStoreErrStr(ret), ret); \
    } \
}while(0)

#define CLEANUP_IF_ERR(err) \
do { \
    if (err != KV_SUCCESS) { \
        PRINTF("err = %d\n", err); \
        goto Cleanup; \
    } \
}while(0)

#define CLEANUP_IF_RERR(ret) \
do { \
    if (ret != RKV_SUCCESS) { \
        PRINTF("ret = %d\n", ret); \
        goto Cleanup; \
    } \
}while(0)

int checkObjHasClass(SEXP obj, const char *name);
kv_store_t *getKVStore(SEXP storeObj);
kv_key_t *getKey(SEXP keyObj);
kv_value_t *getValue(SEXP valueObj);
void *getIterator(SEXP iteratorObj);
avro_value_t *getAvroValue(SEXP avroValue);
rkv_error_t rkv_malloc(int size, void **ptr);
const char *getRKVStoreErrStr(int rc);

#if DEBUG
#define PRINTF(fmt, ...) \
do { \
    Rprintf("[DEBUG]"); \
    Rprintf(fmt, ##__VA_ARGS__); \
    Rprintf("\n"); \
} while(0)
#else
#define PRINTF(fmt, ...)
#endif

#endif
