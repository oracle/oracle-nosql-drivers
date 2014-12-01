% File rnosql/man/rkv_avro_value_print.Rd
\name{rkv_avro_value_print}
\alias{rkv_avro_value_print}
\title{Print the avro value in JSON format.}
\description{
Prints the avro value in JSON format.
}
\usage{
rkv_avro_value_print(avroValue)
}
\arguments{
\item{avroValue}{(kvAvroValue object) The avro value object you want to print. }
}
\examples{
\dontrun{
avroValue <- rkv_create_avroValue(store, "schema.UserInfo")
avroValue <- rkv_avro_value_set_int(avroValue, "age", 30)
avroValue <- rkv_avro_value_set_string(avroValue, "name", "bob.smith")
avroValue <- rkv_avro_value_set_string(avroValue, "phone", "400-6700-514")
avroValue <- rkv_avro_value_set_string(avroValue, "address", "Beijing China")
...
rkv_avro_value_print(avroValue)
}
}
\seealso{
\code{\link{rkv_avro_value_set_int}},\cr
\code{\link{rkv_avro_value_set_long}},\cr
\code{\link{rkv_avro_value_set_string}},\cr
\code{\link{rkv_avro_value_set_double}},\cr
\code{\link{rkv_avro_value_set_boolean}}.
}