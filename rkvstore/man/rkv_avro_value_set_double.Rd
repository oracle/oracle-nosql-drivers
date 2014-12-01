% File rnosql/man/rkv_avro_value_set_double.Rd
\name{rkv_avro_value_set_double}
\alias{rkv_avro_value_set_double}
\title{Set value for a double field of avro value object}
\description{
Set value for a double field of avro value object.
}
\usage{
rkv_avro_value_set_double(avroValue, name, value)
}
\arguments{
\item{avrovalue}{(kvAvroValue object) The avro value object. }
\item{name}{(string) The name of the field to set. }
\item{value}{(real) The double value. }
}
\examples{
\dontrun{
avroValue <- rkv_create_avroValue(store, "schema.myTest")
avroValue <- rkv_avro_value_set_double(avroValue, "doubleF", 12313.1)
...
rkv_release_avroValue(avroValue)
}
}
\seealso{
\code{\link{rkv_avro_value_set_int}},\cr
\code{\link{rkv_avro_value_set_long}},\cr
\code{\link{rkv_avro_value_set_string}},\cr
\code{\link{rkv_avro_value_set_boolean}}.
}