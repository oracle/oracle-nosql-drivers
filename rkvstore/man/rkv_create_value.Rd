% File rnosql/man/rkv_create_value.Rd
\name{rkv_create_value}
\alias{rkv_create_value}
\title{Create a value object}
\description{
Creates a kvValue object. To release the resources used by this object, use rkv_release_value().  
}
\usage{
rkv_create_value(store, data)
}
\arguments{
\item{store}{(kvStore object) The store parameter is the handle to the store, it is obtained using rkv_open_store(). }
\item{data}{(string or kvAvroValue object) The data parameter is a string or an avro value object that containing the data to be contained in the new value.}
}
\value{
(kvValue object) Return a kvValue object.
}
\examples{
\dontrun{
#String value
data <- "Hello world"
value <- rkv_create_value(store, data)
...
rkv_release_value(value)

#Avro value
avroValue <- rkv_create_avro_value(store, "UserInfo")
avroValue <- rkv_avro_value_set_int(avroValue, "id", 1);
avroValue <- rkv_avro_value_set_string(avroValue, "name", "bob.smith");
...
value <- rkv_create_value(store, avroValue)
...
rkv_release_value(value)
}
}
\seealso{
\code{\link{rkv_release_value}}.
}