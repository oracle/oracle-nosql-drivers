% File rnosql/man/rkv_store_iterator.Rd
\name{rkv_store_iterator}
\alias{rkv_store_iterator}
\title{Returns an iterator that provides traversal of descendant key/value pairs.}
\description{
Returns an iterator that provides traversal of descendant key/value pairs associated with the parent_key. 
}
\usage{
rkv_store_iterator(store, key=NULL, start=NULL, end=NULL, keyonly=FALSE)
}
\arguments{
\item{store}{(kvStore object) The store parameter is the handle to the store, it is obtained using rkv_open_store(). }
\item{key}{(kvKey object) The key parameter is the parent key whose "child" records are to be retrieved. It may be null to fetch all records in the store. If non-null, the major key path must be a partial path and the minor key path must be empty.}
\item{start}{(string) The start parameter defines the lower bound of the key range. If NULL, no lower bound is enforced. }
\item{end}{(string) The end parameter defines the upper bound of the key range. If NULL, no upper bound is enforced.  }
\item{keyonly}{(logic) This flag indicates that if only return keys or key/value pairs: TRUE - keyOnly, FALSE - key/value pairs. By default, it is FALSE. }
}
\value{
(kvIterator boject) Return a kvIterator object.
}
\examples{
iterator <- rkv_store_iterator(store)
while(rkv_iterator_next(iterator)) {
    rkey <- rkv_iterator_get_key(iterator)
    rvalue <- rkv_iterator_get_value(iterator)
    print(rkv_get_key_uri(rkey))
    print(rkv_get_value(rvalue))
}
rvk_release_iterator(iterator)
}
\seealso{
\code{\link{rkv_multiget_iterator}}.
}