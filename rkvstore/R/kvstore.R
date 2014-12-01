#
#
#  This file is part of Oracle NoSQL Database
#  Copyright (C) 2011, 2013 Oracle and/or its affiliates.  All rights reserved.
#
#  Oracle NoSQL Database is free software: you can redistribute it and/or
#  modify it under the terms of the GNU Affero General Public License
#  as published by the Free Software Foundation, version 3.
#
#  Oracle NoSQL Database is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#  Affero General Public License for more details.
#
#  You should have received a copy of the GNU Affero General Public
#  License in the LICENSE file along with Oracle NoSQL Database.  If not,
#  see <http://www.gnu.org/licenses/>.
#
#  An active Oracle commercial licensing agreement for this product
#  supercedes this license.
#
#  For more information please contact:
#
#  Vice President Legal, Development
#  Oracle America, Inc.
#  5OP-10
#  500 Oracle Parkway
#  Redwood Shores, CA 94065
#
#  or
#
#  berkeleydb-info_us@oracle.com
#
#

rkv_open_store <- function(host="localhost", port=5000, kvname="kvstore") {
    if (Sys.getenv("KVHOME") == "") {
        print("Please set the environment variable [KVHOME] to the directory where Oracle NoSQL Database binaries are installed.");
        return (NULL)
    }
    home <- Sys.getenv("KVHOME");
    .Call(".rkv_open_store", home, host, port, kvname)
}

rkv_close_store <- function(store) {
    .Call(".rkv_close_store", store)
}

rkv_create_key <- function(store, major, minor=NULL) {
    .Call(".rkv_create_key", store, major, minor)
}

rkv_create_key_from_uri <- function(store, uri) {
    .Call(".rkv_create_key_from_uri", store, uri)
}

rkv_get_key_uri <- function(key) {
    .Call(".rkv_get_key_uri", key)
}

rkv_release_key <- function(key) {
    .Call(".rkv_release_key", key)
}

rkv_create_value <- function(store, data) {
    .Call(".rkv_create_value", store, data)
}

rkv_get_value <- function(value) {
    .Call(".rkv_get_value", value)
}

rkv_get_avro_value <- function(value) {
    .Call(".rkv_get_avro_value", value)
}

rkv_release_value <- function(value) {
    .Call(".rkv_release_value", value)
}

rkv_put <- function(store, key, value=NULL) {
    .Call(".rkv_put", store, key, value)
}

rkv_get <- function(store, key) {
    .Call(".rkv_get", store, key)
}

rkv_delete <- function(store, key) {
    .Call(".rkv_delete", store, key)
}

rkv_multi_delete <- function(store, key, start=NULL, end=NULL) {
    .Call(".rkv_multi_delete", store, key, start, end)
}

rkv_multiget_values <- function(store, schema, key, start=NULL, end=NULL) {
    .Call(".rkv_multiget_values", store, schema, key, start, end)
}

rkv_multiget_iterator <- function(store, key, start=NULL, end=NULL, keyonly=FALSE) {
    .Call(".rkv_multiget_iterator", store, key, start, end, keyonly)
}

rkv_store_iterator <- function(store, key=NULL, start=NULL, end=NULL, keyonly=FALSE) {
    .Call(".rkv_store_iterator", store, key, start, end, keyonly)
}

rkv_iterator_size <- function(iterator) {
    .Call(".rkv_iterator_size", iterator)
}

rkv_iterator_next <- function(iterator) {
    .Call(".rkv_iterator_next", iterator)
}

rkv_iterator_get_key <- function(iterator) {
    .Call(".rkv_iterator_get_key", iterator)
}

rkv_iterator_get_value <- function(iterator) {
    .Call(".rkv_iterator_get_value", iterator)
}

rkv_release_iterator <- function(iterator) {
    .Call(".rkv_release_iterator", iterator)
}

rkv_get_sample_for_keyspace <- function(store, schema, majorKey, percentage=1, limit=1000) {    
    if (is.null(store)) {
        print("Invalid argument, store can't be NULL.")
        return(NULL)
    }
    if (is.null(schema)) {
        print("Invalid argument, schema can't be NULL.")
        return(NULL)
    }
    if (is.null(majorKey)) {
        print("Invalid argument, majorKey can't be NULL.")
        return(NULL)
    }
    if (percentage < 0 || percentage > 100) {
        print("Invalid argument, percentage should in range [1, 100]")
        return(NULL)
    }    
    if (limit < 0) {
        print("Invalid argument, limit should be a positive interger.")
        return(NULL)
    }
    
    iterator = rkv_multiget_iterator(store, majorKey)
    if (is.null(iterator)) {
        print("Failed to create iterator.")
        return(NULL)
    }    
    size <- rkv_iterator_size(iterator)
    rkv_release_iterator(iterator)
    if (is.null(size)) {
        print("Failed to get the size of iterator.")
        return(NULL)
    }
    if (size == 0) {
        print("No data found.")
        return(NULL)
    }
    
    num = as.integer(size * (percentage/100))
    if (num == 0) {
        num = 1
    }
    if (num > limit) {
        num = limit
    }

    dfValues <- rkv_multiget_values(store, schema, majorKey)    
    df <- dfValues[sample(1:dim(dfValues)[1], size=num, replace=F),]
    row.names(df)<-c(1:num)
    return(df)
}

rkv_create_avro_value <- function(store, schema) {
    .Call(".rkv_create_avro_value", store, schema);
}

rkv_avro_value_set_int <- function(avroValue, name, value) {
    .Call(".rkv_avro_value_set_int", avroValue, name, value);
}

rkv_avro_value_get_int <- function(avroValue, name) {
    .Call(".rkv_avro_value_get_int", avroValue, name);
}

rkv_avro_value_set_long <- function(avroValue, name, value) {
    .Call(".rkv_avro_value_set_long", avroValue, name, value);
}

rkv_avro_value_get_long <- function(avroValue, name) {
    .Call(".rkv_avro_value_get_long", avroValue, name);
}

rkv_avro_value_set_string <- function(avroValue, name, value) {
    .Call(".rkv_avro_value_set_string", avroValue, name, value);
}

rkv_avro_value_get_string <- function(avroValue, name) {
    .Call(".rkv_avro_value_get_string", avroValue, name);
}

rkv_avro_value_set_double <- function(avroValue, name, value) {
    .Call(".rkv_avro_value_set_double", avroValue, name, value);
}

rkv_avro_value_get_double <- function(avroValue, name) {
    .Call(".rkv_avro_value_get_double", avroValue, name);
}

rkv_avro_value_set_boolean <- function(avroValue, name, value) {
    .Call(".rkv_avro_value_set_boolean", avroValue, name, value);
}

rkv_avro_value_get_boolean <- function(avroValue, name) {
    .Call(".rkv_avro_value_get_boolean", avroValue, name);
}

rkv_avro_value_print <- function(avroValue) {
    .Call(".rkv_avro_value_print", avroValue);
}
rkv_release_avro_value <- function(avroValue) {
    .Call(".rkv_release_avro_value", avroValue);
}
