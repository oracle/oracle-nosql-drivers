% File rnosql/man/rkv_put.Rd
\name{rkv_put}
\alias{rkv_put}
\title{Put a key/value pair, inserting or overwriting as appropriate}
\description{
Writes the key/value pair to the store, inserting or overwriting as appropriate. 
}
\usage{
rkv_put(store, key, value)
}
\arguments{
\item{store}{(kvStore object) The store parameter is the handle to the store, it is obtained using rkv_open_store(). }
\item{key}{(kvKey object) The key parameter is the key that you want to write to the store. It is created using rkv_create_key() or rkv_create_key_from_uri(). }
\item{value}{(kvValue object) The value parameter is the value that you want to write to the store. It is created using rkv_create_value(). }
}
\examples{
store <- rkv_open_store("localhost", 5000, "kvstore"); 
uri <- "/MajorPathPart1/MajorPathPart2/-/MinorPathPart1/MinorPathPart2"
key <- rkv_create_key_from_uri(store, uri)
data <- c("Hello from Oracle NoSQL R")
value <- rkv_create_value(store, data)
rkv_put(store, key, value)
}
\seealso{
\code{\link{rkv_create_key}},\cr
\code{\link{rkv_create_key_from_uri}},\cr
\code{\link{rkv_create_value}}.
}