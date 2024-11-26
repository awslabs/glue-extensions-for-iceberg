<!--
  - Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
  -
  - Licensed under the Apache License, Version 2.0 (the "License").
  - You may not use this file except in compliance with the License.
  - A copy of the License is located at
  -
  -  http://aws.amazon.com/apache2.0
  -
  - or in the "license" file accompanying this file. This file is distributed
  - on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  - express or implied. See the License for the specific language governing
  - permissions and limitations under the License.
  -->

# AWS Glue Extensions For Apache Iceberg

This package contains AWS Glue extensions for use with Apache Iceberg.

## Getting Started

Here are the instructions to start using the library on Apache Spark against Glue catalog `123456789012:rmscatalog/rmsdatabase`.
For more details and explanations about the configurations used, see later sections of the doc.

### On Amazon EMR Starting 7.5

Enable Iceberg Spark cluster using instructions [here](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-iceberg-use-spark-cluster.html#emr-iceberg-create-cluster), 
and launch Spark session with:

```shell
spark-sql \
 --packages software.amazon.glue:glue-extensions-for-iceberg-spark:0.1.0:runtime \
 --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
 --conf spark.sql.catalog.my_catalog.type=glue \
 --conf spark.sql.catalog.my_catalog.glue.id=123456789012:rmscatalog/rmsdatabase \
 --conf spark.sql.defaultCatalog=my_catalog \
 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

### On Open Source Spark

```shell
spark-sql \
 --packages org.apache.icebergsoftware.amazon.glue:glue-extensions-for-iceberg-spark:0.1.0:runtime \
 --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
 --conf spark.sql.catalog.my_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
 --conf spark.sql.catalog.my_catalog.glue.id=123456789012:rmscatalog/rmsdatabase \
 --conf spark.sql.defaultCatalog=my_catalog \
 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,software.amazon.glue.GlueIcebergSparkExtensions \
 --conf spark.hadoop.fs.s3a.impl=software.amazon.glue.s3a.S3AFileSystem \
 --conf spark.hadoop.fs.s3a.credentials.resolver=software.amazon.glue.GlueTableCredentialsResolver \
 --conf spark.hadoop.glue.id=123456789012:rmscatalog/rmsdatabase
```

## AWS Glue Extensions API

The `glue-extensions-api.yaml` file contains the AWS Glue catalog extensions API OpenAPI specification.

The Glue extensions endpoints are located at `<glue-api-endpoint>/extensions`, e.g. `https://glue.us-east-1.amazonaws.com/extensions`.
See [this page](https://docs.aws.amazon.com/general/latest/gr/glue.html) for all the AWS Glue API endpoints across AWS regions.

## GlueCatalog Extensions

The `catalog` folder contains code for MavenCentral library `glue-catalog-extensions-for-iceberg`.
It provides a `software.amazon.glue.GlueCatalogExtensions` class, which is an implementation of Iceberg's `org.apache.iceberg.Catalog` Java interface 
that leverages the AWS Glue Extensions APIs.

### Importing the library

If you are working on library integration with it, use:

```groovy
dependencies {
    implementation "software.amazon.glue:glue-catalog-extensions-for-iceberg:0.1.0"
}
```

If you are working on engine integration, you might need to use the shaded runtime jar.
It provides the same class path shading as the ones used in Iceberg engine runtime jars:

```groovy
dependencies {
    implementation "software.amazon.glue:glue-catalog-extensions-for-iceberg-runtime:0.1.0"
}
```

## Apache Spark Integration

The `spark` folder contains code for Maven library `glue-extensions-for-iceberg-spark-3.5_2.12`.

### Importing the library

It is recommended to use the runtime version of the library as a dependency,
which provides the same class path shading as the ones used in Iceberg engine runtime jars:

```groovy
// without shading
dependencies {
    implementation "software.amazon.glue:glue-extensions-for-iceberg-spark-3.5_2.12:0.1.0"
}

// with shading
dependencies {
    implementation "software.amazon.glue:glue-extensions-for-iceberg-spark-runtime-3.5_2.12:0.1.0"
}
```

You can also directly use it in your Spark application using:

```shell
spark-sql \
 --packages software.amazon.glue:glue-extensions-for-iceberg-spark-runtime-3.5_2.12:0.1.0 \
 --conf ...
```

See later sections of the doc for all required and optional configurations.

### Hadoop S3A Resolver

The package is bundled with a forked version of [Hadoop S3AFileSystem](https://github.com/apache/hadoop/blob/trunk/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/S3AFileSystem.java)
that contains an additional `S3CredentialsResolver` pluggable interface.
`software.amazon.glue.GlueTableCredentialsResolver` implements the `S3CredentialsResolver` to provide access to data in RMS catalogs.
To use this resolver, set the following Spark configurations:

```
spark.hadoop.fs.s3a.impl = software.amazon.glue.s3a.S3AFileSystem  
spark.hadoop.fs.s3a.credentials.resolver = software.amazon.glue.GlueTableCredentialsResolver
spark.hadoop.glue.id = <your Glue catalog ID>
```

See [Hadoop S3A documentation](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html)
for other S3A configurations you can set for other aspects of S3A.

We are working with the Apache Hadoop community to add this feature officially in the Hadoop AWS module.

### Spark Redshift Connector Extensions

The package is bundled with a forked version of [Redshift Spark connector](https://github.com/spark-redshift-community/spark-redshift) 
that allows interaction with the AWS Glue RMS Catalogs and conversion of Iceberg data sources to Redshift data sources.
`software.amazon.glue.GlueIcebergSparkExtensions` class implements the Spark extensions plugin interface and
allows automatic query acceleration fo RMS table queries using this forked Spark Iceberg connector.
To use the extension, set the following Spark configurations:

```
spark.sql.extensions = software.amazon.glue.GlueIcebergSparkExtensions
```

We are working with the Spark Redshift community to add this feature officially in the codebase.

## Using GlueCatalogExtensions with Apache Iceberg GlueCatalog

`GlueCatalogExtensions` is supposed to be used together with [Apache Iceberg GlueCatalog](https://iceberg.apache.org/docs/nightly/aws/#glue-catalog) 
to offer richer capabilities to it.
In Amazon EMR 7.5 and later, this library is automatically bundled as a part of the [EMR Spark Iceberg integration](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-iceberg-use-spark-cluster.html).
When you start an Iceberg GlueCatalog session in Spark, extensions will automatically be initialized behind the scene:

```
spark.sql.catalog.my_catalog = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.my_catalog.type = glue
spark.sql.catalog.my_catalog.glue.id = 123456789012:rmscatalog/rmsdatabase
```

We are actively working with the Apache Iceberg open source community to add this functionality to 
the open source version of Iceberg GlueCatalog library.

## Using GlueCatalogExtensions independently

You can use the extensions just like any other Iceberg Java Catalogs. For example in Spark:

```
spark.sql.catalog.my_catalog = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.my_catalog.catalog-impl = software.amazon.glue.GlueCatalogExtensions
spark.sql.catalog.my_catalog.glue.id = 123456789012:rmscatalog/rmsdatabase
```

### Configurations

The following catalog properties used by [Iceberg GlueCatalog](https://iceberg.apache.org/docs/nightly/aws/#glue-catalog) 
that are respected by `GlueCatalogExtensions`:
- `glue.id` for Glue catalog ID, when not set, default to the root catalog of the calling account
- `client.region` for overriding to use a specific Glue region. When not set, the default region provider chain is used to discover the region to use.
- `glue.endpoint` for overriding to use a specific Glue endpoint. When not set, the official Glue regional endpoint will be use for GlueCatalog.

The extensions has its own set of configurations as well,
see `software.amazon.glue.GlueExtensionsProperties` for a full list.

### Supported Operations

The extensions only supports a limited set of operations including:
- `CreateNamesapce` and `DeleteNamespaces` asynchronously
- `CreateTable`, `DeleteTable` and `UpdateTable` asynchronously
- `LoadTable` with staging location and credentials
- `PreplanTable` and `PlanTable` for server side table scan planning

If you execute any other operation (e.g. GetNamespace, ListTables), you will hit `UnsupportedOperationsException`.

### Delegating Catalog

To have a full catalog experience, you could set a delegating Apache Iceberg catalog to fulfill the operations 
that extensions do not support. For examples, you can delegate operations to an AWS Glue Iceberg REST Catalog 
endpoint like the following:

```
spark.sql.catalog.my_catalog = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.my_catalog.catalog-impl = software.amazon.glue.GlueCatalogExtensions
spark.sql.catalog.my_catalog.glue.id = 123456789012:rmscatalog/rmsdatabase
spark.sql.catalog.my_catalog.glue.extensions.delegate-catalog-impl = org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.my_catalog.glue.extensions.delegate-catalog.uri = https://glue.us-east-1.amazonaws.com/iceberg
spark.sql.catalog.my_catalog.glue.extensions.delegate-catalog.rest.signing-name = glue
spark.sql.catalog.my_catalog.glue.extensions.delegate-catalog.rest.signing-region = us-east-1
```

### FileIO Setting

By default, the Glue extensions uses a specific `S3FileIO` that uses the extensions LoadTable API to 
refresh API to refresh credentials. To override that behavior, you can provide a custom FileIO implementation through:

```
spark.sql.catalog.my_catalog = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.my_catalog.catalog-impl = software.amazon.glue.GlueCatalogExtensions
spark.sql.catalog.my_catalog.glue.id = 123456789012:rmscatalog/rmsdatabase
spark.sql.catalog.my_catalog.glue.extensions.file-io-impl = my.company.MyCustomFileIO
spark.sql.catalog.my_catalog.glue.extensions.file-io.property1 = value1
```

## Using GlueCatalogSessionExtensions

The `GlueCatalogSessionExtensions` class can be used independently outside `GlueCatalogExtensions`,
and can be used inject session-specific information like AWS credentials.
For example:

```java 
GlueCatalogSessionExtensions sessionExtensions = new GlueCatalogSessionExtensions();
sessionExtensions.initialize(...);

SessionCatalog.SessionContext sessionContext = new SessionCatalog.SessionContext(
    "id",
    "user",
    ImmutableMap.of(
    GlueExtensionsSessionProperties.CREDENTIALS_AWS_ACCESS_KEY_ID, "access-key-id",
    GlueExtensionsSessionProperties.CREDENTIALS_AWS_SECRET_ACCESS_KEY,
    "secret-access-key",
    GlueExtensionsSessionProperties.CREDENTIALS_AWS_SESSION_TOKEN, "session-token"),
    ImmutableMap.of());

Table table = sessionExtensions.loadTable(sessionContext, TableIdentifier.of("ns1", "table1"));
```

## Dev Setup

### Building the library

Supported JDK version: 11, 17

* To invoke a build and run tests: `./gradlew build`
* To skip tests: `./gradlew build -x test`
* To fix code style: `./gradlew spotlessApply`

See [CONTRIBUTING.md](./CONTRIBUTING.md) for more details about the AWS open source contribution guidelines. 