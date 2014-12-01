library("rkvstore")

#
# This simple example demos using AVRO related APIs.
#
# Please add the AVRO schema "userinfo.avsc" to kvstore before running this R script by running bellow comand:
#    java -jar KVHOME/lib/kvstore.jar runadmin -host <hostname> -port <port> ddl add-schema -file <path-of-rkvstore-demo>/userinfo.avsc
#  
#  1. Open the kvstore
#  2. Insert 10 records: AVRO type value is used for value.
#  3. Retrieve the records just inserted.
#  4. Close the kvstore
# #

# Open the kvstore"
cat("\nOpen the kvstore\n")
store <- rkv_open_store("localhost", 5000, "kvstore"); 

# Delete the existing records
cat("\nDelete the existing records.")
iterator <- rkv_store_iterator(store)
while(rkv_iterator_next(iterator)) {
    dkey <- rkv_iterator_get_key(iterator);    
    rkv_delete(store, dkey)    
}
rkv_release_iterator(iterator)

# Insert 20 records
cat("\nInsert 20 records\n")
for (i in 1:20)  {    
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
    rkv_avro_value_set_long(avro_value, "time", 1382081095650 + 3600000*24*i)
    rkv_avro_value_set_double(avro_value, "income", ((i%%50) * 101212.1))
    rkv_avro_value_print(avro_value)
    
    value <- rkv_create_value(store, avro_value)
    rkv_put(store, key, value)
    rkv_release_key(key)
    rkv_release_avro_value(avro_value)
    rkv_release_value(value)
}

cat("\nRetrieve all the records.\n")
key <- rkv_create_key_from_uri(store, "/user")
iterator <- rkv_store_iterator(store, key)
c_name <- c()
c_age <- c()
c_phone <- c()
c_address <- c()
c_expired <- c()
c_time <- c()
c_income <- c()
while(rkv_iterator_next(iterator)) {
    rkey <- rkv_iterator_get_key(iterator)
    rvalue <- rkv_iterator_get_value(iterator)
    print(rkv_get_value(rvalue))
    rAvroValue <- rkv_get_avro_value(rvalue)
    c_name <- append(c_name, rkv_avro_value_get_string(rAvroValue, "name"))
    c_age <- append(c_age, rkv_avro_value_get_int(rAvroValue, "age"))
    c_phone <- append(c_phone, rkv_avro_value_get_string(rAvroValue, "phone"))
    c_address <- append(c_address, rkv_avro_value_get_string(rAvroValue, "address"))
    c_expired <- append(c_expired, rkv_avro_value_get_boolean(rAvroValue, "expired"))
    c_time <- append(c_time, rkv_avro_value_get_long(rAvroValue, "time"))
    c_income <- append(c_income, rkv_avro_value_get_double(rAvroValue, "income"))
    rkv_release_avro_value(rAvroValue);
}
rkv_release_key(key)
rkv_release_iterator(iterator)
df <- data.frame(c_name, c_age, c_phone, c_address, c_expired, c_time, c_income) 
colnames(df) <- c("name", "age", "phone", "address", "expired", "time", "income")
print(df)

# Get rows using rkv_multiget_values, parent_key = /user/group1
cat("\nRetrieve the the descendant values associated with the parent_key /user/group1\n")
key <- rkv_create_key_from_uri(store, "/user/group1")
df <- rkv_multiget_values(store, "schema.UserInfo", key)
print(df)
rkv_release_key(key)

# Get rows using rkv_multiget_values, parent_key = /user/group2
cat("\nRetrieve the the descendant values associated with the parent_key /user/group2\n")
key <- rkv_create_key_from_uri(store, "/user/group2")
df <- rkv_multiget_values(store, "schema.UserInfo", key)
print(df)
rkv_release_key(key)

cat("\nClose the kvstore\n")
# Close the kvstore
rkv_close_store(store)
