# Introduction


A fast, light-weight, easy to use, easy to expand ETL tool for migrating data between RDBMS with JDBC connection. 


# How to Use

## Step1: Prepare Configurations

Maximum example:
```
{
  "source" : {
    "name" : "DB_FROM",
    "url" : "jdbc_url",
    "driver" : "driver class",
    "user" : "username",
    "password" : "password",
    "table" : "table name",
    "query" : "query",
    "catalog" : "catalog/database",
    "schema" : "schema",
    "columns" : [
      {
        "name" : "column_name",
        "type" : "column_type"
      }
    ]
  },
  "destination" : {
    "name" : "DB_TO",
    "url" : "jdbc_url",
    "driver" : "driver class",
    "user" : "username",
    "password" : "password",
    "table" : "table name",
    "catalog" : "catalog/database",
    "schema" : "schema",
    "columns" : [
      {
        "srcName" : "source table column to map",
        "name" : "column_name",
        "type" : "column_type"
      }
    ]
  },
  "setting" : {
    "channel" : 5,
    "pagingStrategy" : "DEPENDENT/DISTRIBUTE/GENERATE/CURSOR/KEYWORD",
    "conflictStrategy" : "DROP/TRUNCATE/EXIT/CONTINUE",
    "insertStrategy" : "JDBC/SPRING",
    "extractChunkSize" : 10000,
    "loadChunkSize" : 10000,
    "replicaDatabase" : "replica",
    "clear" : true,
    "retry" : true,
    "maxRetry" : 3,
    "retryIntervalMs" : 60000
  }
}
```
To remove some optional configs, a minimum example may look like:

```
{
  "source" : {
    "url" : "jdbc_url",
    "user" : "username",
    "password" : "password",
    "query" : "query",
  },
  "destination" : {
    "url" : "jdbc_url",
    "user" : "username",
    "password" : "password",
    "table" : "table name",
  }
}
```
It will automatically find a suitable way to do the ETL, like,
find a primary key or index as pagination key, find all the source columns' type and transfer to correspondent type in destination database.

## Step2: Call API

### Http request <br>
`
curl -X POST -d <config> http://localhost:8080/etl
`
### Executable Jar <br>
`
java -jar easy-etl.jar <filepath>
`