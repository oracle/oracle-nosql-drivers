% File rnosql/man/rkv_release_key.Rd
\name{rkv_release_value}
\alias{rkv_release_value}
\title{Release the kvValue object.}
\description{
Releases the resources used by a kvValue object. The object was intially allocated using rkv_create_value(). 
}
\usage{
rkv_release_value(value)
}
\arguments{
\item{value}{(kvValue object) The value object that you want to release. }
}
\examples{
\dontrun{
data <- "Hello world"
value <- rkv_create_value(store, data)
...
rkv_release_value(value)
}
}
\seealso{
\code{\link{rkv_create_value}}.
}