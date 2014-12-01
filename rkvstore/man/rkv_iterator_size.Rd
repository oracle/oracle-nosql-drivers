% File rnosql/man/rkv_iterator_size.Rd
\name{rkv_iterator_size}
\alias{rkv_iterator_size}
\title{Return the number of elements in the store iterator.}
\description{
Returns the number of items contained in the iterator. This function can only be used with iterators returned by the rkv_multiget_iterator(). The iterators returned by other functions, such as rkv_store_iterator(), are not usable by this function. 
}
\usage{
rkv_iterator_size(iterator)
}
\arguments{
\item{iterator}{(kvIterator object) The iterator parameter is the handle to the iterator for which you want sizing information. }
}
\value{
(int)The number of items contained in the iterator.
}
\examples{
key <- rkv_create_key_from_uri(store, "/user/smith")
iterator <- rkv_multiget_iterator(store, key)
nRec <- rkv_iterator_size(iterator)
rkv_release_iterator(iterator)
}
\seealso{
\code{\link{rkv_multiget_iterator}}.
}