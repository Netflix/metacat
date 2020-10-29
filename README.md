# Metacat

[![Download](https://api.bintray.com/packages/netflixoss/maven/metacat/images/download.svg)](https://bintray.com/netflixoss/maven/metacat/_latestVersion)
[![License](https://img.shields.io/github/license/Netflix/metacat.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Issues](https://img.shields.io/github/issues/Netflix/metacat.svg)](https://github.com/Netflix/metacat/issues)
[![NetflixOSS Lifecycle](https://img.shields.io/osslifecycle/Netflix/metacat.svg)]()

## Introduction

Metacat is a unified metadata exploration API service. You can explore Hive, RDS, Teradata, Redshift, S3 and Cassandra.
Metacat provides you information about what data you have, where it resides and how to process it. Metadata in the end 
is really data about the data. So the primary purpose of Metacat is to give a place to describe the data so that we 
could do more useful things with it. 

Metacat focusses on solving these three problems:

* Federate views of metadata systems.
* Allow arbitrary metadata storage about data sets.
* Metadata discovery

## Documentation

TODO

## Releases

[Releases](https://github.com/Netflix/metacat/releases/)

## Builds

Metacat builds are run on Travis CI [here](https://travis-ci.com/Netflix/metacat).
[![Build Status](https://travis-ci.com/Netflix/metacat.svg?branch=master)](https://travis-ci.com/Netflix/metacat)

## Getting Started

```
git clone git@github.com:Netflix/metacat.git
cd metacat
./gradlew clean build
```

Once the build is completed, the metacat WAR file is generated under `metacat-war/build/libs` directory. Metacat needs 
two basic configurations:

* `metacat.plugin.config.location`: Path to the directory containing the catalog configuration. Please look at 
catalog [samples](https://github.com/Netflix/metacat/tree/master/metacat-functional-tests/metacat-test-cluster/etc-metacat/catalog) used for functional testing.
* `metacat.usermetadata.config.location`: Path to the configuration file containing the connection properties to store 
user metadata. Please look at this [sample](https://github.com/Netflix/metacat/blob/master/metacat-functional-tests/metacat-test-cluster/etc-metacat/usermetadata.properties).

### Running Locally

Take the build WAR in `metacat-war/build/libs` and deploy it to an existing Tomcat as `ROOT.war`.

The REST API can be accessed @ [http://localhost:8080/mds/v1/catalog](http://localhost:8080/mds/v1/catalog)

Swagger API documentation can be accessed @ [http://localhost:8080/swagger-ui.html](http://localhost:8080/swagger-ui.html)

### Docker Compose Example

**Pre-requisite: Docker compose is installed**

To start a self contained Metacat environment with some sample catalogs run the command below. 
This will start a `docker-compose` cluster containing a Metacat container, a Hive Metastore Container, a Cassandra 
container and a PostgreSQL container.

```
./gradlew metacatPorts
```

* `metacatPorts` - Prints out what exposed ports are mapped to the internal container ports.
Look for the mapped port (`MAPPED_PORT`) to port 8080.

REST API can be accessed @ `http://localhost:<MAPPED_PORT>/mds/v1/catalog`

Swagger API documentation can be accessed @ `http://localhost:<MAPPED_PORT>/swagger-ui.html`

To stop the docker compose cluster:

```
./gradlew stopMetacatCluster
```
