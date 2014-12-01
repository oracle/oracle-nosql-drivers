% File rnosql/man/rkv_get_key_uri.Rd
\name{rkv_get_key_uri}
\alias{rkv_get_key_uri}
\title{Get the uri string of kvKey object}
\description{
Get the uri string of kvKey object.
}
\usage{
rkv_get_key_uri(key)
}
\arguments{
\item{key}{(kvKey object) The kvKey object. }
}
\value{
(string) Return the uri string of the kvKey object.
}
\examples{
\dontrun{
key <- rkv_create_key_from_uri(store, "/user")
iterator <- rkv_store_iterator(store, key)
while(rkv_iterator_next(iterator)) {
    rkey <- rkv_iterator_get_key(iterator)    
    print(rkv_get_key_uri(rkey))
    ...
}
}
}
\seealso{
\code{\link{rkv_get}}
}

