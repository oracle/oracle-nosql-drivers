% File rnosql/man/rkv_multi_delete.Rd
\name{rkv_multi_delete}
\alias{rkv_multi_delete}
\title{Delete all descendant key/value pairs associated with the parent key.}
\description{
Deletes the descendent key/value pairs associated with the parent_key. 
}
\usage{
rkv_multi_delete(store, key, start=NULL, end=NULL)
}
\arguments{
\item{store}{(kvStore object) The store parameter is the handle to the store, it is obtained using rkv_open_store(). }
\item{key}{(kvKey object) The key parameter is the parent key whose "child" records are to be deleted. It must not be NULL. The major key path must be complete. The minor key path may be omitted or may be a partial path. }
\item{start}{(string) The start parameter defines the lower bound of the key range. If NULL, no lower bound is enforced. }
\item{end}{(string) The end parameter defines the upper bound of the key range. If NULL, no upper bound is enforced.  }
}
\value{
(integer) Returns the total number of keys deleted.
}
\examples{
key <- rkv_create_key_from_uri(store, "/user/smith/-/email")
nDel <- rkv_multi_delete(store, key, "2", "6")
rkv_release_key(key)
}
\seealso{
\code{\link{rkv_delete}}.
}