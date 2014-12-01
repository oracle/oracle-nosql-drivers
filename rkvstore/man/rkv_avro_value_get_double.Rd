% File rnosql/man/rkv_avro_value_get_double.Rd
\name{rkv_avro_value_get_double}
\alias{rkv_avro_value_get_double}
\title{Get value of a double field of avro value object.}
\description{
Get value of a double field of avro value object.
}
\usage{
rkv_avro_value_get_double(avroValue, name)
}
\arguments{
\item{avroValue}{(kvAvroValue object) The avro value object. }
\item{name}{(string) The name of the field to get. }
}
\examples{
\dontrun{
avroValue <- rkv_get_avro_value(value)  
rkv_avro_value_get_double(avroValue, "income")
...
rvk_release_avro_value(avroValue)
}
}
\seealso{
\code{\link{rkv_avro_value_get_int}},\cr
\code{\link{rkv_avro_value_get_long}},\cr
\code{\link{rkv_avro_value_get_boolean}},\cr
\code{\link{rkv_avro_value_get_string}}.
}