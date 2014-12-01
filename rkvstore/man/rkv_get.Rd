% File rnosql/man/rkv_get.Rd
\name{rkv_get}
\alias{rkv_get}
\title{Get the value associated with the given key}
\description{
Get the value associated with the given key. This function uses the store's default consistency policy and timeout value.
}
\usage{
rkv_get(store, key)
}
\arguments{
\item{store}{(kvStore object) The store parameter is the handle to the store, it is obtained using rkv_open_store(). }
\item{key}{(kvKey object) The key parameter is the key portion of the record that you want to read.  }
}
\value{
Return a kvValue object.
}
\examples{
uri <- "/user/smith/-/email/01"
key <- rkv_create_key(store, uri)
rkv_get(store, key)
}
\seealso{
\code{\link{rkv_multiget_iterator}}
}