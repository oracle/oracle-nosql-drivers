% File rnosql/man/rkv_multiget_iterator.Rd
\name{rkv_multiget_values}
\alias{rkv_multiget_values}
\title{Fetch all the descendant values associated with the parent_key for a specified schema.}
\description{
Fetch all the descendant values associated with the parent_key for a specified schema.
}
\usage{
rkv_multiget_values(store, schema, key, start=NULL, end=NULL)
}
\arguments{
\item{store}{(kvStore object) The store parameter is the handle to the store, it is obtained using rkv_open_store(). }
\item{schema}{(string) The schema name.}
\item{key}{(kvKey object) The parent_key parameter is the parent key whose "child" records are to be retrieved. It must not be NULL. The major key path must be complete. The minor key path may be omitted or may be a partial path.  }
\item{start}{(string) The start parameter defines the lower bound of the key range. If NULL, no lower bound is enforced. }
\item{end}{(string) The end parameter defines the upper bound of the key range. If NULL, no upper bound is enforced.  }
}
\value{
(data frame)R dataframe structure that is populated with values.
}
\examples{
key <- rkv_create_key_from_uri(store, "/avrotest/user")
df <- rkv_multiget_values(store, "schema.UserInfo", key)
print(df)
rkv_release_key(key)
}
\seealso{
\code{\link{rkv_multiget_iterator}}.
}