% File rnosql/man/rkv_get_sample_for_keyspace.Rd
\name{rkv_get_sample_for_keyspace}
\alias{rkv_get_sample_for_keyspace}
\title{Random sampling from the specified keyspace.}
\description{
Perform an Oracle NoSQL Database (ONDB) query and only return back certain random samples from the key range specified.
}
\usage{
rkv_get_sample_for_keyspace(store, schema, majorKey, percentage=1, limit=1000)
}
\arguments{
\item{store}{(kvStore object) The store parameter is the handle to the store, it is obtained using rkv_open_store(). }
\item{schema}{(string) The schema name.}
\item{majorKey}{(kvKey object) The major key path of the keyspace of interest, which must be the complete major key path.}
\item{percentage}{(integer) The number of samples returned from the keyspace as a percentage of the keyspace count. }
\item{limit}{(integer) Limits the resultset size to keep from overwhelming cache. }
}
\value{
(data frame)R dataframe structure that is populated with the sampling Key/Value pairs. 
}
\examples{
\dontrun{
key <- rkv_create_key_from_uri(store, "/segments/pets")
ret <- rkv_get_sample_for_keyspace(store, "schema.UserInfo", key)
...
rkv_release_key(key)
}
}
