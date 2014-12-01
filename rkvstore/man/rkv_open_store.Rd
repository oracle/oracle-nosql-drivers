% File rnosql/man/rkv_open_store.Rd
\name{rkv_open_store}
\alias{rkv_open_store}
\title{Open an kvstore}
\description{
Opens an Oracle NoSQL Database store and create the kvstore object. Call rkv_close_store() to close the connection and release the resources allocated for this object. Please set the environment variable KVCLIENT_PATH_TO_JAR to the path to the kvclient.jar.
}
\usage{
rkv_open_store(host="localhost", port=5000, kvname="kvstore")
}
\arguments{
\item{host}{(string) The host parameter is the network name of a node belonging to the store. The node must be currently active because it is used by the application as a helper host to locate other nodes in the store. }
\item{port}{(integer) The port parameter is the helper host's port number. }
\item{kvname}{(string) The kvname parameter is the store name of the KVStore. }
}
\examples{
store <- rkv_open_store("localhost", 5000, "kvstore"); 
}
\seealso{
\code{\link{rkv_close_store}}.
}
