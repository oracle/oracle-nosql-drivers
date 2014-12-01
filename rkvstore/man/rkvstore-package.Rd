\name{rkvstore-package}
\alias{rkvstore-package}
\alias{rkvstore}
\docType{package}
\title{
A simple Oracle NoSQL Databse client for R.
}
\description{
The functions in this package allow you access Oracle NoSQL Database from R.
}

\section{Overview}{
A typical workflow using rkvstore:
\enumerate{
\item Connect to a nosql database.\cr
\preformatted{
    store <- rkv_open_store("localhost", 5000, "kvstore"); 
}
\item Insert a record.\cr
This will insert a key/value pair{"/user/smith/-/email/01", "bob.smith@oracle.com"} to store.
\preformatted{
    major <- c("user", "smith")
    minor <- c("email", "01")
    email <- "bob.smith@oracle.com"
    key <- rkv_create_key(store, major, minor)    
    value <- rkv_create_value(store, email)        
    rkv_put(store, key, value)        
}
\item Retrieve a single record.\cr
This will retrieve the value associated with the key "/user/smith/-/email/01".
\preformatted{
    uri <- "/user/smith/-/email/01"
    key <- rkv_create_key_from_uri(store, uri)
    value <- rkv_get(store, key)
    print(rkv_get_value(value))    
    rkv_release_key(key)
    rkv_release_value(value)
}
\item Retrieve multiple records using rkv_multiget_iterator().\cr
This will retrieve all the descendant key/value pairs associated with the parent key, the major path components provide must represent a complete set of components.
\preformatted{
    key <- rkv_create_key_from_uri(store, "/user/smith")
    iterator <- rkv_multiget_iterator(store, key)
    while(rkv_iterator_next(iterator)) {
        rkey <- rkv_iterator_get_key(iterator)
        rvalue <- rkv_iterator_get_value(iterator)
        print(rkv_get_key_uri(rkey))
        print(rkv_get_value(rvalue))
    }
    rkv_release_key(key)
    rkv_release_iterator(iterator)
}
\item Retrieve multiple records using rkv_store_iterator().\cr
This will retrieve all the descendant key/value pairs associated with the parent key, the parent key provided could be null or partial set of major components.
\preformatted{                   
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
}
\item Delete a record.\cr
This will delete the key/value pair associated with the specifed key("/user/smith/-/email/01").
\preformatted{
    major <- c("user", "smith")
    minor <- c("email", "01")
    key <- rkv_create_key(store, major, minor)        
    rkv_delete(store, key)
    rkv_release_key(key)
}
\item Delete multiple records.\cr
This will delete all descendant key/value pairs associated with the parent key.
\preformatted{
    key <- rkv_create_key_from_uri(store, "/user/smith")
    nRec <- rkv_multi_delete(store, key)
    rkv_release_key(key)
}
\item Close the connection.\cr
\preformatted{
    rkv_close_store(store)
}
}
}
\author{
berkeleydb-info_us@oracle.com
}
\keyword{rnosql}
