% File rnosql/man/rkv_multiget_iterator.Rd
\name{rkv_multiget_iterator}
\alias{rkv_multiget_iterator}
\title{Returns an iterator that provides traversal of descendant key/value pairs.}
\description{
Returns an iterator that permits an ordered traversal of the descendant key/value pairs associated with the parent_key. 
}
\usage{
rkv_multiget_iterator(store, key, start=NULL, end=NULL, keyonly=FALSE)
}
\arguments{
\item{store}{(kvStore object) The store parameter is the handle to the store, it is obtained using rkv_open_store(). }
\item{key}{(kvKey object) The parent_key parameter is the parent key whose "child" records are to be retrieved. It must not be NULL. The major key path must be complete. The minor key path may be omitted or may be a partial path.  }
\item{start}{(string) The start parameter defines the lower bound of the key range. If NULL, no lower bound is enforced. }
\item{end}{(string) The end parameter defines the upper bound of the key range. If NULL, no upper bound is enforced.  }
\item{keyonly}{(logic) This flag indicates that if return keys only or key/value pairs: TRUE - keyOnly, FALSE - key/value pairs. By default, it is FALSE. }
}
\value{
(kvIterator object) Return a kvIterator object.
}
\examples{
key <- rkv_create_key_from_uri(store, "/user/smith/-/email")
iterator <- rkv_multiget_iterator(store, key, "2", "4")
while(rkv_iterator_next(iterator)) {
    rkey <- rkv_iterator_get_key(iterator)
    rvalue <- rkv_iterator_get_value(iterator)
    print(rkv_get_key_uri(rkey))
    print(rkv_get_value(rvalue))
}
rkv_release_key(key)
rvk_release_iterator(iterator)
}
\seealso{
\code{\link{rkv_store_iterator}}.
}