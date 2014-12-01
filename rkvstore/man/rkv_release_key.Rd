% File rnosql/man/rkv_release_key.Rd
\name{rkv_release_key}
\alias{rkv_release_key}
\title{Release the key object}
\description{
Releases the resources used by a key. The object was intially allocated using rkv_create_key() or rkv_create_key_from_uri(). 
}
\usage{
rkv_release_key(key)
}
\arguments{
\item{key}{(kvKey object) The key object that you want to release. }
}
\examples{
\dontrun{
major <- c("user", "smith")
minor <- c("email")
key <- rkv_create_key(store, major, minor)
...
rkv_release_key(key)
}
}
\seealso{
\code{\link{rkv_create_key}},\cr
\code{\link{rkv_create_key_from_uri}}.
}