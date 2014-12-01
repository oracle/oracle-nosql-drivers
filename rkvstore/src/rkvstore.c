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

#include <R.h>

#include "symbols.h"
#include "utils.h"
#include "rkvstore.h"
#include "rkvstore_internal.h"

#define CLASS_KVSTORE   "kvstore"

typedef struct rkv_iterator {
    kv_iterator_t * kvIterator;
    kv_key_t * currentKey;
    kv_value_t * currentValue;
}rkv_iterator_t;

typedef struct {
    char *name;
    avro_type_t type;
}rkv_avro_field;

static SEXP makeExternalInt(int value);
static SEXP makeExternalReal(double value);
static SEXP makeExternalLogic(int value);
static SEXP makeExternalString(const char *data[], int len[], int size);
static SEXP makeExternalPtr(void *ptr, SEXP symbol, const char *cls_name,
                            R_CFinalizer_t finalizer);
static SEXP createIteartorInternal(SEXP store, SEXP key,
            SEXP start, SEXP end, SEXP keyonly, int isMultiGet);
static rkv_iterator_t *rkv_itr_init(kv_iterator_t *iterator);
static kv_iterator_t *rkv_itr_get_kvIterator(rkv_iterator_t * rkvIterator);
static void rkv_itr_set_key_value(rkv_iterator_t * rkvIterator,
                                  const kv_key_t *key,
                                  const kv_value_t *value);
static kv_key_t *rkv_itr_get_currentKey(rkv_iterator_t * rkvIterator);
static kv_value_t *rkv_itr_get_currentValue(rkv_iterator_t * rkvIterator);
static kv_iterator_t *get_kvIterator_from_Obj(SEXP iterator,
                                    rkv_iterator_t ** ret_rkvIterator);

static void rkvStoreFinalizer(SEXP ptr);
static void rkvKeyFinalizer(SEXP ptr);
static void rkvValueFinalizer(SEXP ptr);
static void rkvIteratorFinalizer(SEXP ptr);
static void rkvAvroValueFinalizer(SEXP ptr);
static void release_rkvItearator(rkv_iterator_t *rkvIterator);
static void release_avro_fields (rkv_avro_field *fields, int nFields);

SEXP rkv_open_store(SEXP kvhome, SEXP host, SEXP port, SEXP kvname) {
    kv_store_t *store = NULL;
    kv_error_t err;
    const char *l_host, *l_kvname, *l_classpath;
    char path[512];
    int l_port;

    /* Check input parameters */
    if (!isValidString(host) || STRING_ELT(host, 0) == NA_STRING) {
        ERROR_INVALID_ARGUMENT("host");
    }
    l_host = CHAR(STRING_ELT(host, 0));
    l_port = asInteger(port);

    if (!isValidString(kvname) || STRING_ELT(kvname, 0) == NA_STRING) {
        ERROR_INVALID_ARGUMENT("kvname");
    }
    l_kvname = CHAR(STRING_ELT(kvname, 0));

    if (!isValidString(kvhome) || STRING_ELT(kvhome, 0) == NA_STRING) {
        ERROR_INVALID_ARGUMENT("kvhome");
    }
    l_classpath = CHAR(STRING_ELT(kvhome, 0));

    memset(path, 0, sizeof(path));
    sprintf(path, "%s/lib/kvclient.jar", (char*)l_classpath);
    err = r_kvstore_open(path, l_kvname, l_host, l_port, &store);
    if (err != KV_SUCCESS) {
        return R_NilValue;
    }
    return makeExternalPtr(store, sym_kvstore, CLASS_KVSTORE,
                           rkvStoreFinalizer);
}

static void rkvStoreFinalizer(SEXP ptr) {
    if (!R_ExternalPtrAddr(ptr)) {
        return;
    }
    r_kvstore_close((kv_store_t *)R_ExternalPtrAddr(ptr));
    R_ClearExternalPtr(ptr);
}

SEXP rkv_close_store(SEXP store) {
    kv_store_t *kvstore = getKVStore(store);
#if DEBUG
    kv_error_t err = r_kvstore_close(kvstore);
    PRINTF("rkv_close_store, ret = %d.\n", err);
#else
    r_kvstore_close(kvstore);
#endif
    R_ClearExternalPtr(getAttrib(store, sym_kvstore));
    return R_NilValue;
}

#define MAX_KEY_COMPONENTS  32
SEXP rkv_create_key(SEXP store, SEXP major, SEXP minor) {
    int l_major = 0, l_minor = 0, i, l_paths;
    char *buf[MAX_KEY_COMPONENTS] = {0};
    char **p_major = (char**)&buf, **p_minor = NULL;
    char **p_cur;
    kv_store_t *kvstore = NULL;
    kv_key_t *key = NULL;
    rkv_error_t err;
    SEXP ret = R_NilValue;

    /* Aruguments validataion check:
        - major should be a valid string vector,
        - minor should be a vdali string vector if it is not null.
    */
    CHECK_IF_VALID_STRING(major, "major");
    l_major = LENGTH(major);
    if (!isNull(minor)) {
        CHECK_IF_VALID_STRING(minor, "minor");
        l_minor = LENGTH(minor);
    }
    l_paths = l_major + l_minor;
    if ((sizeof(buf)/sizeof(char *)) < l_paths) {
        err = rkv_malloc(sizeof(char *) * l_paths, (void **)&p_major);
        RETURN_NULL_IF_ERR(err);
    }

    p_cur = p_major;
    for (i = 0; i < l_major; i++, p_cur++) {
        *p_cur = (char *)CHAR(STRING_ELT(major, i));
        PRINTF("[%d] %s", i, *p_cur);
    }
    if (l_minor > 0) {
        p_cur++;
        p_minor = p_cur;
        for (i = 0; i < l_minor; i++, p_cur++) {
            *p_cur = (char *)CHAR(STRING_ELT(minor, i));
        }
    }
    kvstore = getKVStore(store);

    err = r_kv_create_key(kvstore, &key, (const char**)p_major,
                         (const char**)p_minor);
    CLEANUP_IF_RERR(err);
    ret = makeExternalPtr(key, sym_kv_key, CLASS_KV_KEY, rkvKeyFinalizer);

Cleanup:
    if (p_major != (char**)&buf) {
        free(p_major);
    }
    return ret;
}

SEXP rkv_create_key_from_uri(SEXP store, SEXP uri) {
    kv_store_t *kvstore = NULL;
    kv_key_t *kvkey = NULL;
    const char *pbuf = NULL;
    rkv_error_t err;
    SEXP ret = R_NilValue;

    CHECK_IF_VALID_STRING(uri, "uri");
    kvstore = getKVStore(store);
    pbuf = (const char *)CHAR(STRING_ELT(uri, 0));

    err = kv_create_key_from_uri(kvstore, &kvkey, pbuf);
    RETURN_NULL_IF_ERR(err);

    ret = makeExternalPtr(kvkey, sym_kv_key, CLASS_KV_KEY, rkvKeyFinalizer);
    return ret;
}

SEXP rkv_get_key_uri(SEXP key) {
    rkv_error_t err;
    const char *uri = NULL;
    int len = 0;
    kv_key_t * kkey = getKey(key);

    err = r_kv_get_key_uri(kkey, &uri);
    RETURN_NULL_IF_ERR(err);
    len = strlen(uri);
    return makeExternalString(&uri, &len, 1);
}

/*SEXP rkv_get_key_major(SEXP key) {
    kv_key_t * kkey = getKey(key);
    const char *paths = NULL;
    int size;
    rkv_error_t err;

    paths = *(r_kv_get_key_major(kkey));
    RETURN_NULL_IF_ERR(err);
    size = sizeof(paths)/sizeof(char*);
    return makeExternalString(&paths, NULL, size);
}

SEXP rkv_get_key_minor(SEXP key) {
    const kv_key_t * kkey = getKey(key);
    const char *paths = NULL;
    int size;

    paths = *(r_kv_get_key_minor(kkey));
    size = sizeof(paths)/sizeof(char*);
    return makeExternalString(&paths, NULL, size);
}*/

static void rkvKeyFinalizer(SEXP ptr) {
    kv_key_t *key = NULL;
    if (!R_ExternalPtrAddr(ptr))
        return;
    key = (kv_key_t *)R_ExternalPtrAddr(ptr);
    r_kv_release_key(&key);
    R_ClearExternalPtr(ptr);
}

SEXP rkv_release_key(SEXP key) {
    kv_key_t * kkey = getKey(key);
    r_kv_release_key(&kkey);
    R_ClearExternalPtr(getAttrib(key, sym_kv_key));
    return R_NilValue;
}

SEXP rkv_create_value(SEXP store, SEXP data) {
    kv_store_t *kvstore = NULL;
    kv_value_t *value = NULL;
    rkv_error_t err;

    kvstore = getKVStore(store);
    if (!isNull(data) && checkObjHasClass(data, CLASS_KV_AVRO_VALUE)) {
        avro_value_t *avro_value = getAvroValue(data);
        err = r_kv_create_value_avro(kvstore, &value, avro_value);
    } else {
        const unsigned char *pbuf;
        CHECK_IF_VALID_STRING(data, "data");
        pbuf = (const unsigned char *)CHAR(STRING_ELT(data, 0));
        err = r_kv_create_value_bytes(kvstore, &value, pbuf,
                                     (int)strlen((const char*)pbuf));
    }
    RETURN_NULL_IF_ERR(err);
    return makeExternalPtr(value, sym_kv_value, CLASS_KV_VALUE,
                           rkvValueFinalizer);
}

SEXP rkv_get_value(SEXP value) {
    rkv_error_t ret;
    char *pBuf = NULL;
    int len = 0, needFree = 0;
    kv_value_t * kvValue = getValue(value);

    ret = r_kv_get_value(kvValue, (const unsigned char**)&pBuf, &len, &needFree);
    RETURN_NULL_IF_ERR(ret);
    SEXP retVal = makeExternalString((const char **)&pBuf, &len, 1);
    if (needFree && pBuf) {
        free(pBuf);
    }
    return retVal;
}

SEXP rkv_get_avro_value(SEXP value) {
    rkv_error_t ret;
    avro_value_t *avroValue = NULL;
    const kv_value_t * kvValue = getValue(value);

    ret = r_kv_get_avrovalue(kvValue, &avroValue, NULL);
    RETURN_NULL_IF_ERR(ret);
    return makeExternalPtr(avroValue, sym_kv_avro_value,
                           CLASS_KV_AVRO_VALUE,
                           rkvAvroValueFinalizer);
}

static void rkvValueFinalizer(SEXP ptr) {
    kv_value_t *value = NULL;
    if (!R_ExternalPtrAddr(ptr))
        return;
    value = (kv_value_t *)R_ExternalPtrAddr(ptr);
    r_kv_release_value(&value);
    R_ClearExternalPtr(ptr);
}

SEXP rkv_release_value(SEXP value) {
    kv_value_t * kvalue = getValue(value);
    r_kv_release_value(&kvalue);
    R_ClearExternalPtr(getAttrib(value, sym_kv_value));
    return R_NilValue;
}

SEXP rkv_put(SEXP store, SEXP key, SEXP value) {
    kv_store_t *kvstore = NULL;
    kv_key_t *kvKey = NULL;
    kv_value_t *kvValue = NULL;
    rkv_error_t ret;

    kvstore = getKVStore(store);
    kvKey = getKey(key);
    kvValue = getValue(value);

    ret = r_kv_put(kvstore, kvKey, kvValue, NULL);
    Rprintf("Operation %s.\n", (ret == RKV_SUCCESS)?"successful":"failed");

    return R_NilValue;
}

SEXP rkv_get(SEXP store, SEXP key) {
    kv_store_t *kvstore = NULL;
    kv_key_t *kvKey = NULL;
    kv_value_t *kvValue = NULL;
    rkv_error_t ret;

    kvstore = getKVStore(store);
    kvKey = getKey(key);

    ret = r_kv_get(kvstore, kvKey, &kvValue);
    if (ret == RKV_KEY_NOT_FOUND) {
        Rprintf("The specified key is not not existed.\n");
    }
    RETURN_NULL_IF_ERR(ret);
    return makeExternalPtr(kvValue, sym_kv_value, CLASS_KV_VALUE,
                           rkvValueFinalizer);
}

SEXP rkv_delete(SEXP store, SEXP key) {
    kv_store_t *kvstore = NULL;
    kv_key_t *kvKey = NULL;
    int ret;

    kvstore = getKVStore(store);
    kvKey = getKey(key);

    ret = r_kv_delete(kvstore, kvKey);
    if (ret == 1) {
        Rprintf("Operation successful.\n");
    } else if (ret == 0) {
        Rprintf("Key is not existed.\n");
    } else {
        Rprintf("Operation failed.\n");
    }
    return R_NilValue;
}

SEXP rkv_multi_delete(SEXP store, SEXP key, SEXP start, SEXP end){
    kv_store_t *kvstore = NULL;
    kv_key_t *kvKey = NULL;
    const char *keyStart = NULL, *keyEnd = NULL;
    int nDeleted = 0;

    kvstore = getKVStore(store);
    kvKey = getKey(key);

    if (!isNull(start)) {
        CHECK_IF_VALID_STRING(start, "start");
        keyStart = (const char *)CHAR(STRING_ELT(start, 0));
    }
    if (!isNull(end)) {
        CHECK_IF_VALID_STRING(end, "end");
        keyEnd = (const char *)CHAR(STRING_ELT(end, 0));
    }
    nDeleted = r_kv_multi_delete(kvstore, kvKey, keyStart, keyEnd);
    if (nDeleted >= 0) {
        Rprintf("%d records deleted.\n", nDeleted);
    }
    return makeExternalInt(nDeleted);
}

rkv_error_t getAvroSchemaFields(const avro_schema_t schema,
                                rkv_avro_field ** ret_avro_fields,
                                int * ret_field_size) {

    int i, col_size = avro_schema_record_size(schema);
    rkv_avro_field * fields = NULL, *pFields;

    rkv_malloc(sizeof(rkv_avro_field) * col_size, (void**)&fields);
    if (fields == NULL) {
        return RKV_NO_MEMORY;
    }
    pFields = fields;
    for (i = 0; i < col_size; i++) {
        const char *fname = avro_schema_record_field_name(schema, i);
        pFields->name = strdup(fname);
        pFields->type =
            avro_typeof(avro_schema_record_field_get(schema, fname));
        pFields++;
	}
    if (ret_avro_fields) {
        *ret_avro_fields = fields;
    }
    if (ret_field_size) {
        *ret_field_size = col_size;
    }
    return RKV_SUCCESS;
}

SEXP rkv_multiget_values(SEXP store, SEXP schema,
                         SEXP key, SEXP start, SEXP end) {

    kv_store_t *kvstore = NULL;
    kv_key_t *kvKey = NULL;
    kv_iterator_t *iterator = NULL;
    avro_schema_t avroSchema = NULL;
    const char *keyStart = NULL, *keyEnd = NULL;
    char *space = NULL, *name = NULL;
    int nRecs = 0, nCols = 0, pc = 0, i = 0, nRvar = 0, iRec;
    rkv_avro_field *avroFields = NULL;
    avro_value_t *avroValue = NULL;
    SEXP tmp, varlabels, row_names, df = R_NilValue;
    rkv_error_t ret = RKV_SUCCESS;

    /* get kvstore */
    kvstore = getKVStore(store);

    /* Check if specified schame is valid, get avro schema object */
    CHECK_IF_VALID_STRING(schema, "schema");
    space = strdup((char*)CHAR(STRING_ELT(schema, 0)));
    name = memchr(space, '.', strlen(space));
    if (name != NULL) {
        *name = '\0';
        name++;
    } else {
        name = space;
        space = NULL;
    }
    avroSchema = r_kv_get_schema(kvstore, space, name);
    if (!avroSchema) {
        ret = RKV_INVALID_SCHEMA;
    }
    free(space);
    RETURN_NULL_IF_ERR(ret);

    /* get kvKey */
    kvKey = getKey(key);

    if (!isNull(start)) {
        CHECK_IF_VALID_STRING(start, "start");
        keyStart = (const char *)CHAR(STRING_ELT(start, 0));
    }
    if (!isNull(end)) {
        CHECK_IF_VALID_STRING(end, "end");
        keyEnd = (const char *)CHAR(STRING_ELT(end, 0));
    }

    /* create iterator */
    ret = rkv_get_iterator(kvstore, kvKey, &iterator, keyStart,
                           keyEnd, 0, 1);
    RETURN_NULL_IF_ERR(ret);

    /* get number of records */
    ret = r_kv_iterator_size(iterator, &nRecs);
    CLEANUP_IF_RERR(ret);

    /* get avro fields */
    ret = getAvroSchemaFields(avroSchema, &avroFields, &nCols);
    CLEANUP_IF_RERR(ret);

    /* initialize dataframe */
    PROTECT(df = allocVector(VECSXP, nCols)); pc++;
    PROTECT(varlabels = allocVector(STRSXP, nCols)); pc++;
    for (i = 0, nRvar = 0; i < nCols; i++) {
        switch(avroFields[i].type) {
	    case AVRO_INT32:
            SET_VECTOR_ELT(df, nRvar, allocVector(INTSXP, nRecs));
            break;
        case AVRO_INT64:
            SET_VECTOR_ELT(df, nRvar, allocVector(REALSXP, nRecs));
            break;
        case AVRO_DOUBLE:
            SET_VECTOR_ELT(df, nRvar, allocVector(REALSXP, nRecs));
            break;
        case AVRO_STRING:
            SET_VECTOR_ELT(df, nRvar, allocVector(STRSXP, nRecs));
            break;
        case AVRO_BOOLEAN:
            SET_VECTOR_ELT(df, nRvar, allocVector(LGLSXP, nRecs));
            break;
        default:
            continue;
        }
        SET_STRING_ELT(varlabels, nRvar, mkChar(avroFields[i].name));
	    nRvar++;
    }

    /* iterate the record and save it to datafram */
    for(i = 0, iRec = 0; i < nRecs; i++) {
        const kv_key_t *rKey = NULL;
        const kv_value_t *kvValue = NULL;
        int iCol;

        /* move iterator next */
        ret = r_kv_iterator_next(iterator, &rKey, &kvValue);
        if (ret == RKV_NO_MORE_DATA) {
            ret = RKV_SUCCESS;
            break;
        }
        CLEANUP_IF_RERR(ret);

        /* read the value of each field of the avro value. */
        ret = r_kv_get_avrovalue(kvValue, &avroValue, avroSchema);
        if (ret != RKV_SUCCESS) {
            ret = RKV_SUCCESS;
            continue;
        }

        for (iCol = 0; iCol < nCols; iCol++) {
            const char *fname = (const char *)avroFields[iCol].name;
            switch(avroFields[iCol].type) {
            case AVRO_INT32: {
                int iValue = 0;
                ret = r_kv_avro_value_get_int(avroValue, fname, &iValue);
                CLEANUP_IF_RERR(ret);
                INTEGER(VECTOR_ELT(df, iCol))[iRec] = iValue;
                break;
            }
            case AVRO_INT64: {
                int64_t i64Value = 0;
                ret = r_kv_avro_value_get_long(avroValue, fname, &i64Value);
                CLEANUP_IF_RERR(ret);
                if (i64Value <= 2147483647 && i64Value >= -2147483648) {
                    INTEGER(VECTOR_ELT(df, iCol))[iRec] = (int)i64Value;
                } else {
                    REAL(VECTOR_ELT(df, iCol))[iRec] = (double)i64Value;
                }
                break;
            }
            case AVRO_DOUBLE:{
                double dValue = 0.0;
                ret = r_kv_avro_value_get_double(avroValue, fname, &dValue);
                CLEANUP_IF_RERR(ret);
                REAL(VECTOR_ELT(df, iCol))[iRec] = dValue;
                break;
            }
            case AVRO_STRING: {
                const char *strValue = NULL;
                int sLen = 0;
                ret = r_kv_avro_value_get_string(avroValue, fname, &strValue, &sLen);
                CLEANUP_IF_RERR(ret);
                SET_STRING_ELT(VECTOR_ELT(df, iCol), iRec, mkChar(strValue));
                break;
            }
            case AVRO_BOOLEAN: {
                int iValue = 0;
                ret = r_kv_avro_value_get_boolean(avroValue, fname, &iValue);
                CLEANUP_IF_RERR(ret);
                LOGICAL(VECTOR_ELT(df, iCol))[iRec] = iValue;
                break;
            }
            default:
                continue;
            }
        }
        iRec++;
        r_kv_release_avro_value(avroValue);
        avroValue = NULL;
    }

    PROTECT(tmp = mkString("data.frame")); pc++;
    setAttrib(df, R_ClassSymbol, tmp);
    setAttrib(df, R_NamesSymbol, varlabels);
    PROTECT(row_names = allocVector(STRSXP, iRec)); pc++;
    for (i = 0; i < iRec; i++) {
        char labelbuff[10] = {0};
	    sprintf(labelbuff, "%d", i+1);
	    SET_STRING_ELT(row_names, i, mkChar(labelbuff));
    }
    setAttrib(df, R_RowNamesSymbol, row_names);

    /* Shrink down rows size. */
    if (iRec < nRecs) {
        for (i = 0; i < nCols; i++) {
            SETLENGTH(VECTOR_ELT(df, i), iRec);
        }
    }
Cleanup:
    UNPROTECT(pc);
    if (avroFields != NULL) {
        release_avro_fields(avroFields, nCols);
    }
    if (avroValue != NULL) {
        r_kv_release_avro_value(avroValue);
    }
    if (iterator != NULL) {
        r_kv_release_iterator(&iterator);
    }
    if (ret != RKV_SUCCESS) {
        return R_NilValue;
    }
    return df;
}

static void release_avro_fields (rkv_avro_field *fields, int nFields) {
    int i;
    for (i = 0; i < nFields; i++) {
        if (fields[i].name) {
            free(fields[i].name);
        }
    }
    free(fields);
}

SEXP rkv_multiget_iterator(SEXP store, SEXP key, SEXP start,
                           SEXP end, SEXP keyonly){

    return createIteartorInternal(store, key, start, end, keyonly, 1);
}

SEXP rkv_store_iterator(SEXP store, SEXP key, SEXP start,
                        SEXP end, SEXP keyonly){
    return createIteartorInternal(store, key, start, end, keyonly, 0);
}

static SEXP createIteartorInternal(SEXP store, SEXP key,
                                   SEXP start, SEXP end,
                                   SEXP keyonly, int isMultiGet) {
    rkv_iterator_t *rkvIterator = NULL;
    kv_store_t *kvstore = NULL;
    kv_key_t *kvKey = NULL;
    kv_iterator_t *iterator = NULL;
    const char *keyStart = NULL, *keyEnd = NULL;
    int isKeyOnly = 0;
    rkv_error_t ret;

    kvstore = getKVStore(store);
    if (isMultiGet) {
        kvKey = getKey(key);
    } else {
        if (!isNull(key)) {
            kvKey = getKey(key);
        }
    }

    CHECK_IF_LOGICAL(keyonly, "keyonly");
    isKeyOnly = LOGICAL(keyonly)[0];

    if (!isNull(start)) {
        CHECK_IF_VALID_STRING(start, "start");
        keyStart = (const char *)CHAR(STRING_ELT(start, 0));
    }
    if (!isNull(end)) {
        CHECK_IF_VALID_STRING(end, "end");
        keyEnd = (const char *)CHAR(STRING_ELT(end, 0));
    }

    ret = rkv_get_iterator(kvstore, kvKey, &iterator, keyStart,
                           keyEnd, isKeyOnly, isMultiGet);
    RETURN_NULL_IF_ERR(ret);

    rkvIterator = rkv_itr_init(iterator);
    return makeExternalPtr(rkvIterator, sym_kv_iterator, CLASS_KV_ITERATOR,
                           rkvIteratorFinalizer);
}

SEXP rkv_iterator_size(SEXP iterator) {
    kv_iterator_t *kvIterator = NULL;
    int size = 0;
    rkv_error_t ret;

    kvIterator = get_kvIterator_from_Obj(iterator, NULL);
    ret = r_kv_iterator_size(kvIterator, &size);
    RETURN_NULL_IF_ERR(ret);

    return makeExternalInt(size);
}

SEXP rkv_iterator_next(SEXP iterator) {
    rkv_iterator_t *rkvIterator = NULL;
    kv_iterator_t *kvIterator = NULL;
    const kv_key_t *kvKey = NULL;
    const kv_value_t *kvValue = NULL;
    rkv_error_t ret;

    kvIterator = get_kvIterator_from_Obj(iterator, &rkvIterator);
    ret = r_kv_iterator_next(kvIterator, &kvKey, &kvValue);
    if (ret != RKV_SUCCESS) {
        return makeExternalLogic(0);
    }

    rkv_itr_set_key_value(rkvIterator, kvKey, kvValue);
    return makeExternalLogic(1);
}

SEXP rkv_iterator_get_key(SEXP iterator){
    rkv_iterator_t * rkvIterator = NULL;
    kv_key_t * kvKey = NULL;

    rkvIterator = (rkv_iterator_t *)getIterator(iterator);
    kvKey = rkv_itr_get_currentKey(rkvIterator);
    /*Returned key are owned by the iterator and released implicitly
      when it is released */
    return makeExternalPtr(kvKey, sym_kv_key, CLASS_KV_KEY, NULL);
}

SEXP rkv_iterator_get_value(SEXP iterator){
    rkv_iterator_t * rkvIterator = NULL;
    kv_value_t * kvValue = NULL;

    rkvIterator = (rkv_iterator_t *)getIterator(iterator);
    kvValue = rkv_itr_get_currentValue(rkvIterator);
    /*Returned value are owned by the iterator and released implicitly
      when it is released */
    return makeExternalPtr(kvValue, sym_kv_value, CLASS_KV_VALUE, NULL);
}

static void rkvIteratorFinalizer(SEXP ptr) {
    rkv_iterator_t *rkvIterator = NULL;
    if (!R_ExternalPtrAddr(ptr))
        return;
    rkvIterator = (rkv_iterator_t *)R_ExternalPtrAddr(ptr);
    release_rkvItearator(rkvIterator);
    R_ClearExternalPtr(ptr);
}

static void release_rkvItearator(rkv_iterator_t *rkvIterator) {
    if (rkvIterator == NULL)
        return;
    r_kv_release_iterator(&rkvIterator->kvIterator);
    free(rkvIterator);
}

SEXP rkv_release_iterator(SEXP iterator){
    rkv_iterator_t *rkvIterator = NULL;
    get_kvIterator_from_Obj(iterator, &rkvIterator);
    release_rkvItearator(rkvIterator);
    R_ClearExternalPtr(getAttrib(iterator, sym_kv_iterator));
    return R_NilValue;
}

/**
* AVRO value related operations
*/
SEXP rkv_create_avro_value(SEXP store, SEXP schema){
    kv_store_t * kvstore = NULL;
    avro_value_t * value = NULL;
    char *space = NULL, *name = NULL;
    int ret;

    CHECK_IF_VALID_STRING(schema, "schema");
    space = strdup((char*)CHAR(STRING_ELT(schema, 0)));
    name = memchr(space, '.', strlen(space));
    if (name != NULL) {
        *name = '\0';
        name++;
    } else {
        name = space;
        space = NULL;
    }

    kvstore = getKVStore(store);
    ret = r_kv_create_avro_value(kvstore, (const char *)space,
                                 (const char *)name, &value);
    free(space);
    RETURN_NULL_IF_ERR(ret);
    return makeExternalPtr(value, sym_kv_avro_value, CLASS_KV_AVRO_VALUE,
                           rkvAvroValueFinalizer);
}

static void rkvAvroValueFinalizer(SEXP ptr) {
    if (!R_ExternalPtrAddr(ptr))
        return;
    r_kv_release_avro_value((avro_value_t *)R_ExternalPtrAddr(ptr));
    R_ClearExternalPtr(ptr);
}

SEXP rkv_release_avro_value(SEXP avroValue) {
    avro_value_t * avro_value = getAvroValue(avroValue);

    r_kv_release_avro_value(avro_value);
    R_ClearExternalPtr(getAttrib(avroValue, sym_kv_avro_value));
    return R_NilValue;
}

SEXP rkv_avro_value_set_int(SEXP avroValue, SEXP name, SEXP value) {
    avro_value_t * avro_value = getAvroValue(avroValue);
    const char *fname;
    rkv_error_t ret;

    CHECK_IF_VALID_STRING(name, "name");
    fname = (const char *)CHAR(STRING_ELT(name, 0));

    ret = r_kv_avro_value_set_int(avro_value, fname, asInteger(value));
    PRINT_ERRMSG_IF_ERR(ret);
    return avroValue;
}

SEXP rkv_avro_value_get_int(SEXP avroValue, SEXP name) {
    avro_value_t * avro_value = getAvroValue(avroValue);
    const char *fname;
    int value = 0;
    rkv_error_t ret;

    CHECK_IF_VALID_STRING(name, "name");
    fname = (const char *)CHAR(STRING_ELT(name, 0));
    ret = r_kv_avro_value_get_int(avro_value, fname, &value);
    RETURN_NULL_IF_ERR(ret);
    return makeExternalInt(value);
}

SEXP rkv_avro_value_set_long(SEXP avroValue, SEXP name, SEXP value) {
    avro_value_t * avro_value = getAvroValue(avroValue);
    const char *fname;
    rkv_error_t ret;

    CHECK_IF_VALID_STRING(name, "name");
    fname = (const char *)CHAR(STRING_ELT(name, 0));
    CHECK_IF_REAL(value, "value");

    ret = r_kv_avro_value_set_long(avro_value, fname, asReal(value));
    PRINT_ERRMSG_IF_ERR(ret);
    return avroValue;
}

SEXP rkv_avro_value_get_long(SEXP avroValue, SEXP name) {
    avro_value_t * avro_value = getAvroValue(avroValue);
    const char *fname;
    int64_t value = 0;
    rkv_error_t ret;

    CHECK_IF_VALID_STRING(name, "name");
    fname = (const char *)CHAR(STRING_ELT(name, 0));
    ret = r_kv_avro_value_get_long(avro_value, fname, &value);
    RETURN_NULL_IF_ERR(ret);
    if (value <= 2147483647 && value >= -2147483648) {
        return makeExternalInt((int32_t)value);
    }
    return makeExternalReal((double)value);
}

SEXP rkv_avro_value_set_string(SEXP avroValue, SEXP name, SEXP value) {
    avro_value_t * avro_value = getAvroValue(avroValue);
    const char *fname, *str;
    rkv_error_t ret;

    CHECK_IF_VALID_STRING(name, "name");
    fname = (const char *)CHAR(STRING_ELT(name, 0));
    CHECK_IF_VALID_STRING(value, "value");
    str = CHAR(STRING_ELT(value, 0));

    ret = r_kv_avro_value_set_string(avro_value, fname, str);
    PRINT_ERRMSG_IF_ERR(ret);
    return avroValue;
}

SEXP rkv_avro_value_get_string(SEXP avroValue, SEXP name) {
    avro_value_t * avro_value = getAvroValue(avroValue);
    const char *fname;
    const char *value = NULL;
    int len = 0;
    rkv_error_t ret;

    CHECK_IF_VALID_STRING(name, "name");
    fname = (const char *)CHAR(STRING_ELT(name, 0));
    ret = r_kv_avro_value_get_string(avro_value, fname, &value, &len);
    RETURN_NULL_IF_ERR(ret);
    return makeExternalString(&value, &len, 1);
}

SEXP rkv_avro_value_set_double(SEXP avroValue, SEXP name, SEXP value){
    avro_value_t * avro_value = getAvroValue(avroValue);
    const char *fname;
    rkv_error_t ret;

    CHECK_IF_VALID_STRING(name, "name");
    fname = (const char *)CHAR(STRING_ELT(name, 0));
    CHECK_IF_REAL(value, "value");

    ret = r_kv_avro_value_set_double(avro_value, fname, asReal(value));
    PRINT_ERRMSG_IF_ERR(ret);
    return avroValue;
}


SEXP rkv_avro_value_get_double(SEXP avroValue, SEXP name) {
    avro_value_t * avro_value = getAvroValue(avroValue);
    const char *fname;
    double value = 0;
    rkv_error_t ret;

    CHECK_IF_VALID_STRING(name, "name");
    fname = (const char *)CHAR(STRING_ELT(name, 0));
    ret = r_kv_avro_value_get_double(avro_value, fname, &value);
    RETURN_NULL_IF_ERR(ret);
    return makeExternalReal(value);
}

SEXP rkv_avro_value_set_boolean(SEXP avroValue, SEXP name, SEXP value){
    avro_value_t * avro_value = getAvroValue(avroValue);
    const char *fname;
    rkv_error_t ret;

    CHECK_IF_VALID_STRING(name, "name");
    fname = (const char *)CHAR(STRING_ELT(name, 0));
    CHECK_IF_LOGICAL(value, "value");

    ret = r_kv_avro_value_set_boolean(avro_value, fname, asLogical(value));
    PRINT_ERRMSG_IF_ERR(ret);
    return avroValue;
}

SEXP rkv_avro_value_get_boolean(SEXP avroValue, SEXP name) {
    avro_value_t * avro_value = getAvroValue(avroValue);
    const char *fname;
    int value = 0;
    rkv_error_t ret;

    CHECK_IF_VALID_STRING(name, "name");
    fname = (const char *)CHAR(STRING_ELT(name, 0));
    ret = r_kv_avro_value_get_boolean(avro_value, fname, &value);
    RETURN_NULL_IF_ERR(ret);
    return makeExternalLogic(value);
}

/*SEXP rkv_avro_value_set_bytes(SEXP avroValue, SEXP name, SEXP value){
    avro_value_t * avro_value = getAvroValue(avroValue);
    const char *fname;
    char *data = NULL;
    int size = 0;
    rkv_error_t ret;

    CHECK_IF_VALID_STRING(name, "name");
    fname = (const char *)CHAR(STRING_ELT(name, 0));
    data = (char*)RAW(value);
    size = LENGTH(value);

    ret = r_kv_avro_value_set_bytes(avro_value, fname, data, size);
    PRINT_ERRMSG_IF_ERR(ret);
    return avroValue;
}

SEXP rkv_avro_value_set_null(SEXP avroValue, SEXP name){
    avro_value_t * avro_value = getAvroValue(avroValue);
    const char *fname;
    rkv_error_t ret;

    CHECK_IF_VALID_STRING(name, "name");
    fname = (const char *)CHAR(STRING_ELT(name, 0));

    ret = r_kv_avro_value_set_null(avro_value, fname);
    PRINT_ERRMSG_IF_ERR(ret);
    return avroValue;
}*/

SEXP rkv_avro_value_print(SEXP avroValue) {
     avro_value_t * avro_value = getAvroValue(avroValue);
     char *json_str = NULL;

     if (avro_value_to_json(avro_value, 0, &json_str) == 0) {
        if (json_str == NULL) {
            Rprintf("json_str is null.\n");
        } else {
            Rprintf("%s\n", json_str);
            free(json_str);
        }
     }
     return R_NilValue;
}

static SEXP makeExternalString(const char *data[], int len[], int size) {
    SEXP ret;
    char buf[1024] = {0}, *pBuf = buf;
    int i, pBuf_size = sizeof(buf);
    rkv_error_t err;

    ret = PROTECT(allocVector(STRSXP, size));
    for (i = 0; i < size; i++) {
        const char *sdata = data[i];
        int slen = (len != NULL)?len[i]:strlen(sdata);
        if (slen > pBuf_size){
            if (pBuf != buf) {
                free(pBuf);
                pBuf = NULL;
            }
            err = rkv_malloc(slen, (void**)&pBuf);
            RETURN_NULL_IF_ERR(err);
            pBuf_size = slen;
        }
        memset(pBuf, 0, pBuf_size);
        memcpy(pBuf, sdata, slen);
        SET_STRING_ELT(ret, i, mkChar(pBuf));
    }
    UNPROTECT(1);
    if (pBuf != buf) {
        free(pBuf);
    }
    return ret;
}

static SEXP makeExternalPtr(void *obj, SEXP symbol, const char *cls_name,
                            R_CFinalizer_t finalizer) {
    SEXP ret, ptr, cls;

    ret = PROTECT(allocVector(INTSXP, 1));
    INTEGER(ret)[0] = 0;
    ptr = PROTECT(R_MakeExternalPtr(obj, symbol, R_NilValue));
    setAttrib(ret, symbol, ptr);
    if (finalizer) {
        R_RegisterCFinalizerEx(ptr, finalizer, TRUE);
    }
    cls = PROTECT(allocVector(STRSXP, 1));
    SET_STRING_ELT(cls, 0, mkChar(cls_name));
    classgets(ret, cls);
    UNPROTECT(3);
    return ret;
}

static SEXP makeExternalInt(int value) {
    SEXP ret;
    ret = PROTECT(allocVector(INTSXP, 1));
    INTEGER(ret)[0] = value;
    UNPROTECT(1);
    return ret;
}

static SEXP makeExternalReal(double value) {
    SEXP ret;
    ret = PROTECT(allocVector(REALSXP, 1));
    REAL(ret)[0] = value;
    UNPROTECT(1);
    return ret;
}

static SEXP makeExternalLogic(int value) {
    SEXP ret;
    ret = PROTECT(allocVector(LGLSXP, 1));
    LOGICAL(ret)[0] = value;
    UNPROTECT(1);
    return ret;
}

static rkv_iterator_t * rkv_itr_init(kv_iterator_t *iterator) {
    rkv_iterator_t * rkvIterator = NULL;

    if (iterator == NULL) {
        return NULL;
    }
    rkv_malloc(sizeof(rkv_iterator_t), (void**)&rkvIterator);
    if (rkvIterator == NULL) {
        return NULL;
    }
    rkvIterator->kvIterator = iterator;

    return rkvIterator;
}

static kv_iterator_t *rkv_itr_get_kvIterator(rkv_iterator_t * rkvIterator) {
    if (rkvIterator == NULL) {
        return NULL;
    }
    return rkvIterator->kvIterator;
}

static void rkv_itr_set_key_value(rkv_iterator_t * rkvIterator,
                                  const kv_key_t *key,
                                  const kv_value_t *value) {
    if (rkvIterator == NULL) {
        return;
    }
    rkvIterator->currentKey = (kv_key_t *)key;
    rkvIterator->currentValue = (kv_value_t *)value;
}

static kv_key_t * rkv_itr_get_currentKey(rkv_iterator_t * rkvIterator) {
    if (rkvIterator == NULL) {
        return NULL;
    }
    return rkvIterator->currentKey;
}

static kv_value_t * rkv_itr_get_currentValue(rkv_iterator_t * rkvIterator) {
    if (rkvIterator == NULL) {
        return NULL;
    }
    return rkvIterator->currentValue;
}

static kv_iterator_t *get_kvIterator_from_Obj(SEXP iterator,
                            rkv_iterator_t ** ret_rkvIterator) {
    rkv_iterator_t *rkvIterator = (rkv_iterator_t *)getIterator(iterator);
    if (ret_rkvIterator) {
        *ret_rkvIterator = rkvIterator;
    }
    return rkv_itr_get_kvIterator(rkvIterator);
}
