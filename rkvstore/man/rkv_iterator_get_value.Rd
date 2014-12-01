% File rnosql/man/rkv_iterator_get_value.Rd
\name{rkv_iterator_get_value}
\alias{rkv_iterator_get_value}
\title{Get the current value from the store iterator}
\description{
Get the current value object of the store iterator, the value is owned by the iterator and released implicitly when it is released.
}
\usage{
rkv_iterator_get_value(iterator)
}
\arguments{
\item{iterator}{(kvIterator object) The iterator parameter is the handle to the iterator. It is allocated using one of functions that performs multiple reads of the store (such as rkv_multiget_iterator() or rkv_store_iterator()). It is released using rkv_release_iterator(). }
}
\value{
Return the kvValue object.
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
\code{\link{rkv_iterator_get_key}},\cr
\code{\link{rkv_multiget_iterator}},\cr
\code{\link{rkv_store_iterator}}.
}