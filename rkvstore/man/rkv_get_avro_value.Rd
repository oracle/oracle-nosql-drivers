% File rnosql/man/rkv_get_avro_value.Rd
\name{rkv_get_avro_value}
\alias{rkv_get_avro_value}
\title{Converts a kvValue object to an Avro value object.}
\description{
Converts a kvValue object to an Avro value object.
}
\usage{
rkv_get_avro_value(value)
}
\arguments{
\item{value}{(kvValue object) The kvValue object. }
}
\value{
Return a kvAvroValue object.
}
\examples{
key <- rkv_create_key_from_uri(store, "/user")
iterator <- rkv_store_iterator(store, key)
while(rkv_iterator_next(iterator)) {
    rkey <- rkv_iterator_get_key(iterator)
    rvalue <- rkv_iterator_get_value(iterator)    
    rAvroValue <- rkv_get_avro_value(rvalue)    
    rkv_avro_value_print(rAvroValue)
    rkv_release_avro_value(rAvroValue);
}
}
\seealso{
\code{\link{rkv_get}}
}

