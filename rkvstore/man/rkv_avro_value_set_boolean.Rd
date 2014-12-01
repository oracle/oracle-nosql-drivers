% File rnosql/man/rkv_avro_value_set_boolean.Rd
\name{rkv_avro_value_set_boolean}
\alias{rkv_avro_value_set_boolean}
\title{Set value for a boolean field of avro value object.}
\description{
Set value for a boolean field of avro value object.
}
\usage{
rkv_avro_value_set_boolean(avroValue, name, value)
}
\arguments{
\item{avrovalue}{(kvAvroValue object) The avro value object. }
\item{name}{(string) The name of the field to set. }
\item{value}{(logic) The logic value. }
}
\examples{
\dontrun{
avro_value <- rkv_create_avro_value(store, "schema.myTest")
avro_value <- rkv_avro_value_set_boolean(avroValue, "booleanF", TRUE)
...
rkv_release_avro_value(avroValue)
}
}
\seealso{
\code{\link{rkv_avro_value_set_int}},\cr
\code{\link{rkv_avro_value_set_long}},\cr
\code{\link{rkv_avro_value_set_string}},\cr
\code{\link{rkv_avro_value_set_double}}.
}