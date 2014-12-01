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

#include "symbols.h"

SEXP sym_kvstore;
SEXP sym_kv_key;
SEXP sym_kv_value;
SEXP sym_kv_iterator;
SEXP sym_kv_avro_value;

void install_kvstore_symbols() {
    sym_kvstore = install("kvstore");
    sym_kv_key = install("kvkey");
    sym_kv_value = install("kvvalue");
    sym_kv_iterator = install("kviterator");
    sym_kv_avro_value = install("kvavrovalue");
}
