% File rnosql/man/rkv_delete.Rd
\name{rkv_delete}
\alias{rkv_delete}
\title{Delete the key/value pair associated with the key. }
\description{
Deleting a key/value pair with this method does not automatically delete its descendant key/value pairs. To delete its descendants, use rkv_multi_delete() instead. 
}
\usage{
rkv_delete(store, key)
}
\arguments{
\item{store}{(kvStore object) The store parameter is the handle to the store, it is obtained using rkv_open_store(). }
\item{key}{(kvKey object) The key parameter is the key used to look up the key/value pair to be deleted. }
}
\examples{
uri <- "/user/smith/-/email/01"
key <- rkv_create_key_from_uri(store, uri)
rkv_delete(store, key)
}
\seealso{
\code{\link{rkv_multi_delete}}.
}