% File rnosql/man/rkv_iterator_next.Rd
\name{rkv_iterator_next}
\alias{rkv_iterator_next}
\title{Advance the iterator to the next record.}
\description{
Step to the first or next record.
}
\usage{
rkv_iterator_next(iterator)
}
\arguments{
\item{iterator}{(kvIterator object) The iterator parameter is the handle to the iterator. It is allocated using one of functions that performs multiple reads of the store (such as rkv_multiget_iterator() or rkv_store_iterator()). It is released using rkv_release_iterator().}
}
\value{
(logic)TRUE if there is a next record; otherwise, FALSE.
}
\examples{
\dontrun{
key <- rkv_create_key_from_uri(store, "/user")
iterator <- rkv_store_iterator(store, key)
while(rkv_iterator_next(iterator)) {
   ...
}
...
}
}
\seealso{
\code{\link{rkv_iterator_get_key}},\cr
\code{\link{rkv_iterator_get_value}}.
}