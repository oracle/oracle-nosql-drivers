% File rnosql/man/rkv_release_iterator.Rd
\name{rkv_release_iterator}
\alias{rkv_release_iterator}
\title{Releases the resources used by the iterator object.}
\description{
Releases the resources used by the iterator object.
}
\usage{
rkv_release_iterator(iterator)
}
\arguments{
\item{iterator}{(kvIterator object) The iterator object that you want to release. }
}
\examples{
\dontrun{
iterator <- rkv_store_iterator(store)
while(rkv_iterator_next(iterator)) {
    ...
}
rkv_release_iterator(iterator)
}
}
\seealso{
\code{\link{rkv_multiget_iterator}},\cr
\code{\link{rkv_store_iterator}}.
}