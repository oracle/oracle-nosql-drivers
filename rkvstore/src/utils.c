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
#include "utils.h"
#include "symbols.h"

static void * getKVObject(SEXP obj, SEXP symbol, const char *cls_name);

kv_store_t *getKVStore(SEXP storeObj) {
    return (kv_store_t *)getKVObject(storeObj, sym_kvstore, CLASS_KVSTORE);
}

kv_key_t *getKey(SEXP keyObj) {
    return (kv_key_t *)getKVObject(keyObj, sym_kv_key, CLASS_KV_KEY);
}

kv_value_t *getValue(SEXP valueObj) {
    return (kv_value_t *)getKVObject(valueObj, sym_kv_value, CLASS_KV_VALUE);
}

void *getIterator(SEXP iteratorObj) {
    return (void *)getKVObject(iteratorObj, sym_kv_iterator,
                               CLASS_KV_ITERATOR);
}

avro_value_t *getAvroValue(SEXP avroValue) {
    return (void *)getKVObject(avroValue, sym_kv_avro_value,
                               CLASS_KV_AVRO_VALUE);
}

static void * getKVObject(SEXP obj, SEXP symbol, const char *cls_name) {
    SEXP ptr = NULL;
    void *ret = NULL;

    CHECK_OBJ_HAS_CLASS(obj, cls_name);
    ptr = getAttrib(obj, symbol);
    if (ptr == R_NilValue) {
        error("Attribute \"%s\" is missing from the class object\n",
              cls_name);
    }

    ret = (void*)R_ExternalPtrAddr(ptr);
    if (!ret) {
        error("This \"%s\" object have been destroyed.\n", cls_name);
    }
    return ret;
}

int checkObjHasClass(SEXP obj, const char *name) {
    SEXP cls = NULL;
    int i, len;

    if (obj == R_NilValue) {
        return 0;
    }

    cls = getAttrib(obj, R_ClassSymbol);
    if (cls == R_NilValue) {
        return 0;
    }

    len = LENGTH(cls);
    for (i = 0; i < len; i++) {
        if (strcmp(CHAR(STRING_ELT(cls, i)), name) == 0) {
            return 1;
        }
    }
    return 0;
}

rkv_error_t rkv_malloc(int size, void **ret) {
    void *ptr = NULL;
    if (!ret || !size) {
        return RKV_INVALID_ARGUEMENTS;
    }

    ptr = malloc(size);
    if (ptr == NULL) {
        return RKV_NO_MEMORY;
    }

    memset(ptr, 0, size);
    *ret = ptr;

    return RKV_SUCCESS;
}

typedef struct rkv_error_msg {
    rkv_error_t err;
    const char *msg;
} rkv_error_msg_t;

const char *getRKVStoreErrStr(int rc) {
    static rkv_error_msg_t rkv_error_msgs[] = {
        {RKV_SUCCESS, "Not an error"},
        {RKV_INVALID_ARGUEMENTS, "Invalid arguments"},
        {RKV_NO_MEMORY, "No memory error"},
        {RKV_ITR_NO_SIZE_INFO, "No size information for the iterator"},
        {RKV_INVALID_SCHEMA, "Schema doesn't exist"},
        {RKV_INVALID_AVRO_SET_OP, "Failed to set avro value, invalid field type."},
        {RKV_ERROR, "General error"},
        {RKV_NO_MORE_DATA, "No more record"},
        {RKV_KEY_NOT_FOUND, "Can't found the key"},
    };
    int i;
    rkv_error_msg_t *err_entry = NULL;
    for (i = 0; i < sizeof(rkv_error_msgs)/sizeof(rkv_error_msg_t); i++) {
        err_entry = &rkv_error_msgs[i];
        if (err_entry->err == rc) {
            return err_entry->msg;
        }
    }
    return "Unknown error";
}
