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

#ifndef __RKVERR_H__
#define __RKVERR_H__

typedef enum {
    RKV_SUCCESS = 0,
    RKV_INVALID_ARGUEMENTS = -1,
    RKV_NO_MEMORY = -2,
    RKV_ITR_NO_SIZE_INFO = -3,
    RKV_INVALID_SCHEMA = -4,
    RKV_INVALID_AVRO_SET_OP = -5,
    RKV_VALUE_NOT_AVRO = -6,
    RKV_ERROR = -100,
    RKV_NO_MORE_DATA = 1,
    RKV_KEY_NOT_FOUND = 2
} rkv_error_t;

#endif
