% File rnosql/man/rkv_close_store.Rd
\name{rkv_close_store}
\alias{rkv_close_store}
\title{Close a store}
\description{
Closes the store handle, releasing all resources used by the handle. 
}
\usage{
rkv_close_store(store)
}
\arguments{
\item{store}{(kvStore object) The store parameter is the handle to the store, it is obtained using rkv_open_store(). }
}
\examples{
\dontrun{
store <- rkv_open_store("localhost", 5000, "kvstore"); 
...
rkv_close_store(store); 
}
}
\seealso{
\code{\link{rkv_open_store}}.
}