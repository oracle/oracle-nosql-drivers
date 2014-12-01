# Oracle NoSQL R Driver

## Introduction
A simple R driver to access Oracle NoSQL Database. It is based on the Oracle NoSQL C JNI driver and provides subset of Key/Value APIs, table APIs are not supported in this package yet.

### Prerequisites
Install Oracle NoSQL C JNI Driver

 * Download source from [Oracle NoSQL Download](http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html)  (Currently use 3.0.14)
 * Build and install it, please follow the BUILDING.html in the Oracle NoSQL C JNI driver. The prerequisite is AVRO C library.

### Installation
The RKVStore build depens on the AVRO C library and Oracle NoSQL C JNI driver. After build and intallaing the above 2 libraries, change LD_LIBRARY_PATH as follows:
```
  export LD_LIBRARY_PATH=$AVRO_LIB/lib:$KV_C_LIB/lib:$LD_LIBRARY_PATH
```
Build and install the RKVStore library:
```
  R CMD INSTALL rkvstore.tar.gz  
```
 
### Run demo programs
 * Start KVLite on localhost:5000.
 * Add the avro schema "schema.UserInfo" to kvstore using below command, the userinfo.avsc is located in "demo" directory.
```
   java -jar $KVHOME/lib/kvcli.jar -host localhost -port 5000 ddl add-schema -file $RKVSTORE_INSTALL_DIR/demo/userinfo.avsc
```
 * Launch R console and run demo programs.
```
   > library("rkvstore")
   > Sys.setenv(KVHOME="<path-to-kvclient-home-dir>")
   > demo(demo_basic, package = "rkvstore")
   > demo(demo_avro, package = "rkvstore")
   > demo(demo_sample, package = "rkvstore")
```
