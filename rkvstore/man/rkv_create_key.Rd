% File rnosql/man/rkv_create_key.Rd
\name{rkv_create_key}
\alias{rkv_create_key}
\title{Create a key using major and minor path components}
\description{
Creates a kvKey object using major and minor path components. To create a key based on a URI string, use rkv_creat_key_from_uri(), to release the resources used by this object, use rkv_release_key(). 
}
\usage{
rkv_create_key(store, major, minor=NULL)
}
\arguments{
\item{store}{(kvStore object) The store parameter is the handle to the store, it is obtained using rkv_open_store(). }
\item{major}{(string vector) The major parameter is an vector of strings, each element of which represents a major path component.}
\item{minor}{(string vector) The minor parameter is an vector of strings, each element of which represents a minor path component.}
}
\value{
(kvKey object) Return a kvKey object.
}
\examples{
\dontrun{
store <- rkv_open_store("localhost", 5000, "kvstore"); 
...
major <- c("user", "smith")
minor <- c("email")
key <- rkv_create_key(store, major, minor)
...
rkv_release_key(key)
}
}
\seealso{
\code{\link{rkv_create_key_from_uri}},\cr
\code{\link{rkv_release_key}}.
}