% File rnosql/man/rkv_avro_value_set_long.Rd
\name{rkv_avro_value_set_long}
\alias{rkv_avro_value_set_long}
\title{Set value for a long field of avro value object.}
\description{
Set value for a long field of avro value object.
}
\usage{
rkv_avro_value_set_long(avroValue, name, value)
}
\arguments{
\item{avroValue}{(kvAvroValue object) The avro value object. }
\item{name}{(string) The name of the field to set. }
\item{value}{(integer) The long value. }
}
\examples{
\dontrun{
avroValue <- rkv_create_avroValue(store, "schema.myTest")
avroValue <- rkv_avro_value_set_long(avroValue, "longF", 4314314)
...
rkv_release_avroValue(avroValue)
}
}
\seealso{
\code{\link{rkv_avro_value_set_int}},\cr
\code{\link{rkv_avro_value_set_string}},\cr
\code{\link{rkv_avro_value_set_double}},\cr
\code{\link{rkv_avro_value_set_boolean}}.
}