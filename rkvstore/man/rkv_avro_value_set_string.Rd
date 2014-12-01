% File rnosql/man/rkv_avro_value_set_string.Rd
\name{rkv_avro_value_set_string}
\alias{rkv_avro_value_set_string}
\title{Set value for a string field of avro value object.}
\description{
Set value for a string field of avro value object.
}
\usage{
rkv_avro_value_set_string(avroValue, name, value)
}
\arguments{
\item{avroValue}{(kvAvroValue object) The avro value object. }
\item{name}{(string) The name of the field to set. }
\item{value}{(string) The string value. }
}
\examples{
\dontrun{
avroValue <- rkv_create_avroValue(store, "schema.myTest")
avroValue <- rkv_avro_value_set_string(avroValue, "stringF", "bob.smith")
...
rkv_release_avroValue(avroValue)
}
}
\seealso{
\code{\link{rkv_avro_value_set_int}},\cr
\code{\link{rkv_avro_value_set_long}},\cr
\code{\link{rkv_avro_value_set_double}},\cr
\code{\link{rkv_avro_value_set_boolean}}.
}