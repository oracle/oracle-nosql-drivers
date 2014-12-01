% File rnosql/man/rkv_avro_value_get_string.Rd
\name{rkv_avro_value_get_string}
\alias{rkv_avro_value_get_string}
\title{Get value of a string field of avro value.}
\description{
Get value of a string field of avro value.
}
\usage{
rkv_avro_value_get_string(avroValue, name)
}
\arguments{
\item{value}{(kvAvroValue object) The avro value object. }
\item{name}{(string) The name of the field to get. }
}
\examples{
\dontrun{
avroValue <- rkv_get_avro_value(value)  
rkv_avro_value_get_string(avroValue, "name")
...
rvk_release_avro_value(avroValue)
}
}
\seealso{
\code{\link{rkv_avro_value_get_int}},\cr
\code{\link{rkv_avro_value_get_long}},\cr
\code{\link{rkv_avro_value_get_double}},\cr
\code{\link{rkv_avro_value_get_boolean}}.
}