library("rkvstore")

# #
# This is a simple exmpale to demo the function rkv_get_sample_for_keyspace, basic steps:
#
# Please add the AVRO schema "userinfo.avsc" to kvstore before running this R script by running bellow comand:
#    java -jar KVHOME/lib/kvstore.jar runadmin -host <hostname> -port <port> ddl add-schema -file <path-of-rkvstore-demo>/userinfo.avsc
#
#   1. Open the kvstore
#   2. Insert records
#   3. Get 1% sampling records with the specified major key.
#   4. Get 10% sampling records with the specified major key.
#   5. Get 10% sampling records up to 50 records with the specified major key.
#   6. Close the kvstore
# #

# Open the kvstore"
cat("\nOpen the kvstore\n")
store <- rkv_open_store("localhost", 5000, "kvstore"); 

# Delete the existing records with the major prefix /avrotest"
cat("\nDelete the existing records with the major prefix /avrotest")
key <- rkv_create_key_from_uri(store, "/user")
iterator <- rkv_store_iterator(store, key)
while(rkv_iterator_next(iterator)) {
    dkey <- rkv_iterator_get_key(iterator);    
    rkv_delete(store, dkey)    
}
rkv_release_key(key)
rkv_release_iterator(iterator)

# Insert 1000 records
cat("\nInsert 1000 records\n")
for (i in 1:1000)  {    
    if (i %% 2 == 0) {
        uri <- paste("/user/group1/-/" , i, sep="")
        expired = TRUE;
    } else {
        uri <- paste("/user/group2/-/" , i, sep="")
        expired = FALSE;
    }    
    key <- rkv_create_key_from_uri(store, uri)
    avro_value <- rkv_create_avro_value(store, "schema.UserInfo")
    
    name <- paste("user" , i, sep="")
    phone <- paste("phone" , i, sep="")
    address <- paste("address" , i, sep="")
    rkv_avro_value_set_string(avro_value, "name", name)
    rkv_avro_value_set_int(avro_value, "age", (20 + i%%40))
    rkv_avro_value_set_string(avro_value, "phone", phone)
    rkv_avro_value_set_string(avro_value, "address", address) 
    rkv_avro_value_set_boolean(avro_value, "expired", expired)
    rkv_avro_value_set_long(avro_value, "time", 1382081095650 + 3600*i)
    rkv_avro_value_set_double(avro_value, "income", ((i%%50) * 101212.1))
    
    value <- rkv_create_value(store, avro_value)
    rkv_put(store, key, value)
    rkv_release_key(key)
    rkv_release_avro_value(avro_value)
    rkv_release_value(value)
}

cat("\nGet 1% sampling records, parent key = /user/group1 (500 records)\n")
key <- rkv_create_key_from_uri(store, "/user/group1")
df <- rkv_get_sample_for_keyspace(store, "schema.UserInfo", key)
rkv_release_key(key)
print(df)

cat("\nGet 10% sampling records, parent key = /user/group1 (500 records)\n")
key <- rkv_create_key_from_uri(store, "/user/group1")
df <- rkv_get_sample_for_keyspace(store, "schema.UserInfo", key, 10)
rkv_release_key(key)
print(df)

cat("\nGet 20% sampling records up to 35, parent key = /user/group2 (500 records)\n")
key <- rkv_create_key_from_uri(store, "/user/group2")
df <- rkv_get_sample_for_keyspace(store, "schema.UserInfo", key, 20, 35)
rkv_release_key(key)
print(df)

cat("\nClose the kvstore\n")
# Close the kvstore
rkv_close_store(store)
