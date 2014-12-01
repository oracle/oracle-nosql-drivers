% File rnosql/man/rkv_create_key_from_uri.Rd
\name{rkv_create_key_from_uri}
\alias{rkv_create_key_from_uri}
\title{Create a key based on a URI string}
\description{
Creates a kvkey object based on a URI string. To create a key object using major and minor path components, use rkv_create_key(), to release the resources used by this structure, use rkv_release_key(). 
}
\usage{
rkv_create_key_from_uri(store, uri)
}
\arguments{
\item{store}{(kvStore object) The store parameter is the handle to the store, it is obtained using rkv_open_store(). }
\item{uri}{(string) The uri parameter is the full key path, both major and minor components, described as a string.}
}
\value{
(kvKey object) Return a kvKey object.
}
\examples{
\dontrun{
uri <- "/user/smith/-/email/01"
key <- rkv_create_key_from_uri(store, uri)
...
rkv_release_key(key)
}
}
\seealso{
\code{\link{rkv_create_key}},\cr
\code{\link{rkv_release_key}}.
}