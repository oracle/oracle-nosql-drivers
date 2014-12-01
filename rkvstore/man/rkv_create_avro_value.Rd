% File rnosql/man/rkv_create_avro_value.Rd
\name{rkv_create_avro_value}
\alias{rkv_create_avro_value}
\title{Create an avro value for the given schema}
\description{
Create an avro value for the given schema.
}
\usage{
rkv_create_avro_value(store, schema)
}
\arguments{
\item{store}{(kvStore object) The store parameter is the handle to the store, it is obtained using rkv_open_store(). }
\item{schema}{(string) The schema name for the avro value.}
}
\value{
Return a kvAvroValue object.
}
\examples{
\dontrun{
store <- rkv_open_store("localhost", 5000, "kvstore"); 
avro_value <- rkv_create_avro_value(store, "schema.UserInfo")
...
rvk_release_avro_value(avro_value)
rkv_close_store(store)
}
}
\seealso{
\code{\link{rkv_release_avro_value}}.
}
