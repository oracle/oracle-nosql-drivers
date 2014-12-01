library("rkvstore")

# #
# This is a simple exmpale to demo the basic CRUD operations, basic steps:
#   1. Open the kvstore
#   2. Insert records
#   3. Retrieve the records using rkv_store_iterator
#   4. Retrieve the records using rkv_multiget_iterator
#   5. Delete the records using rkv_multi_delete
#   6. Close the kvstore
# #

# Open the kvstore
cat("\nOpen the kvstore")
store <- rkv_open_store("localhost", 5000, "kvstore"); 

# Delete the existing records with the major prefix /user"
cat("\nRetrieve the records matching the major path /user\n")
key <- rkv_create_key_from_uri(store, "/user")
iterator <- rkv_store_iterator(store, key)
while(rkv_iterator_next(iterator)) {
    dkey <- rkv_iterator_get_key(iterator);    
    rkv_delete(store, dkey)    
}
rkv_release_key(key)
rkv_release_iterator(iterator)

# insert 10 records
cat("\nInsert 10 records\n")
for (i in 1:10)  {    
    uri <- paste("/user/smith/-/email/" , i, sep="")
    key <- rkv_create_key_from_uri(store, uri)
    email <- paste("user.smith", "@oracle.com", sep=as.character(i))
    value <- rkv_create_value(store, email)
    rkv_put(store, key, value)
    rvalue = rkv_get(store, key)
    print(paste("get value: ", rkv_get_value(rvalue)))
    rkv_release_value(rvalue)
    rkv_release_key(key)
    rkv_release_value(value)
}

# Retrieve the records matching the major path /user"
cat("\nRetrieve the records matching the major path /user\n")
key <- rkv_create_key_from_uri(store, "/user")
iterator <- rkv_store_iterator(store, key)
while(rkv_iterator_next(iterator)) {
    rkey <- rkv_iterator_get_key(iterator)
    rvalue <- rkv_iterator_get_value(iterator)
    print(rkv_get_key_uri(rkey))
    print(rkv_get_value(rvalue))
}
rkv_release_key(key)
rkv_release_iterator(iterator)

# Retrieve the records with key in range [/user/smith/-/email/2, /user/smith/-/email/6]
cat("\nRetrieve the records with key in range [/user/smith/-/email/2, /user/smith/-/email/6]\n")
key <- rkv_create_key_from_uri(store, "/user/smith/-/email")
iterator <- rkv_multiget_iterator(store, key, "2", "6")
while(rkv_iterator_next(iterator)) {
    rkey <- rkv_iterator_get_key(iterator)
    rvalue <- rkv_iterator_get_value(iterator)
    print(rkv_get_key_uri(rkey))    
    print(rkv_get_value(rvalue))
}
rkv_release_key(key)
rkv_release_iterator(iterator)

# Delete the records with key in range [/user/smith/-/email/2, /user/smith/-/email/6]\n
cat("\nDelete the records with key in range [/user/smith/-/email/2, /user/smith/-/email/6\n")
key <- rkv_create_key_from_uri(store, "/user/smith/-/email")
rkv_multi_delete(store, key, "2", "6")
rkv_release_key(key)

cat("\nRetrieve the left records matching the major path /user\n")
key <- rkv_create_key_from_uri(store, "/user")
iterator <- rkv_store_iterator(store, key)
while(rkv_iterator_next(iterator)) {
    rkey <- rkv_iterator_get_key(iterator)
    rvalue <- rkv_iterator_get_value(iterator)
    print(rkv_get_key_uri(rkey))
    print(rkv_get_value(rvalue))
}
rkv_release_key(key)
rkv_release_iterator(iterator)

cat("\nClose the kvstore\n")
# Close the kvstore
rkv_close_store(store)