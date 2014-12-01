% File rnosql/man/rkv_avro_value_set_int.Rd
\name{rkv_avro_value_set_int}
\alias{rkv_avro_value_set_int}
\title{Set value for an integer field of avro value object}
\description{
Set value for an integer field of avro value object.
}
\usage{
rkv_avro_value_set_int(avroValue, name, value)
}
\arguments{
\item{avrovalue}{(kvAvroValue object) The avro value object. }
\item{name}{(string) The name of the field to set. }
\item{value}{(integer) The integer value. }
}
\examples{
\dontrun{
avroValue <- rkv_create_avroValue(store, "schema.UserInfo")
avroValue <- kv_avro_value_set_int(avroValue, "age", 30)
...
rvk_release_avroValue(avroValue)
}
}
\seealso{
\code{\link{rkv_avro_value_set_long}},\cr
\code{\link{rkv_avro_value_set_string}},\cr
\code{\link{rkv_avro_value_set_double}},\cr
\code{\link{rkv_avro_value_set_boolean}}.
}