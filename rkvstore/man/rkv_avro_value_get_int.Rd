% File rnosql/man/rkv_avro_value_get_int.Rd
\name{rkv_avro_value_get_int}
\alias{rkv_avro_value_get_int}
\title{Get value of an integer field of avro value object.}
\description{
Get value of an integer field of avro value object.
}
\usage{
rkv_avro_value_get_int(avroValue, name)
}
\arguments{
\item{value}{(kvAvroValue object) The avro value object. }
\item{name}{(string) The name of the field to get. }
}
\examples{
\dontrun{
avroValue <- rkv_get_avro_value(value)  
rkv_avro_value_get_int(avroValue, "age")
...
rvk_release_avro_value(avroValue)
}
}
\seealso{
\code{\link{rkv_avro_value_get_long}},\cr
\code{\link{rkv_avro_value_get_string}},\cr
\code{\link{rkv_avro_value_get_double}},\cr
\code{\link{rkv_avro_value_get_boolean}}.
}