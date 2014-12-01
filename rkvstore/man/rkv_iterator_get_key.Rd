% File rnosql/man/rkv_iterator_get_key.Rd
\name{rkv_iterator_get_key}
\alias{rkv_iterator_get_key}
\title{Get the current key from the store iterator.}
\description{
Get the current key from the store iterator, the key is owned by the iterator and released implicitly when it is released.
}
\usage{
rkv_iterator_get_key(iterator)
}
\arguments{
\item{iterator}{(kvIterator object) The iterator parameter is the handle to the iterator. It is allocated using one of functions that performs multiple reads of the store (such as rkv_multiget_iterator() or rkv_store_iterator()). It is released using rkv_release_iterator(). }
}
\value{
Return a kvKey object.
}
\examples{
\dontrun{
key <- rkv_create_key_from_uri(store, "/user")
iterator <- rkv_store_iterator(store, key)
while(rkv_iterator_next(iterator)) {
    rkey <- rkv_iterator_get_key(iterator)
    rvalue <- rkv_iterator_get_value(iterator)
}
...
rkv_release_key(key)
rvk_release_iterator(iterator)
}
}
\seealso{
\code{\link{rkv_iterator_get_value}},\cr
\code{\link{rkv_multiget_iterator}},\cr
\code{\link{rkv_store_iterator}}.
}