% File rnosql/man/rkv_release_avro_value.Rd
\name{rkv_release_avro_value}
\alias{rkv_release_avro_value}
\title{Release the resources used by the avro value.}
\description{
Release the resources used by the avro value. The object was intially allocated using rkv_create_avro_value(). 
}
\usage{
rkv_release_avro_value(avroValue)
}
\arguments{
\item{avroValue}{(kvAvroValue object) The avro value object that you want to release. }
}
\examples{
\dontrun{
avro_value <- rkv_create_avro_value(store, "schema.myTest")
...
rkv_release_avro_value(avro_value)
}
}
\seealso{
\code{\link{rkv_create_avro_value}}.
}