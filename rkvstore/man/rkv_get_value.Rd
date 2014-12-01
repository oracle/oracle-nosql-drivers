% File rnosql/man/rkv_get_value.Rd
\name{rkv_get_value}
\alias{rkv_get_value}
\title{Get the string value of kvValue object.}
\description{
Get the string value of kvValue object.
}
\usage{
rkv_get_value(value)
}
\arguments{
\item{value}{(kvValue object) The kvValue object.}
}
\value{
(string) Return the string value of kvValue object. If value is AVRO type, then return a JSON format string. Otherwise, a raw string.
}
\examples{
\dontrun{
...
value <- rkv_get(store, key)
print(rkv_get_value(value))
}
}
\seealso{
\code{\link{rkv_get}}
}

