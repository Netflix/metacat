# Metacat

[![Download](https://api.bintray.com/packages/netflixoss/maven/metacat/images/download.svg)](https://bintray.com/netflixoss/maven/metacat/_latestVersion)
[![License](https://img.shields.io/github/license/Netflix/metacat.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Issues](https://img.shields.io/github/issues/Netflix/metacat.svg)](https://github.com/Netflix/metacat/issues)
[![NetflixOSS Lifecycle](https://img.shields.io/osslifecycle/Netflix/metacat.svg)]()

## Introduction

Metacat is a unified metadata exploration API service.  You can explore Hive, RDS, Teradata, Redshift, S3 and Aegisthus.
Metacat is a metadata service that provides you information about what data you have, where it resides and how to process it.
But metadata in the end is really data about the data.  And from that point of view, the purpose of Metacat is to give us a place to
describe the data so that we could do more useful things with it.  So at the most simple, the desire for Metacat are these three concerns:

* Federate views of metadata systems.
* Allow arbitrary metadata storage about datasets.
* Metadata discovery

## Documentation

TODO

## Releases

[Releases](https://github.com/Netflix/metacat/releases/)

## Builds

Metacat builds are run on Travis CI [here](https://travis-ci.org/Netflix/metacat).
[![Build Status](https://travis-ci.org/Netflix/metacat.svg?branch=master)](https://travis-ci.org/Netflix/meatcat)

## Getting Started
```
git clone git@github.com:Netflix/metacat.git
cd metacat
./gradlew clean build
```
Once the build is completed, the metacat WAR file is generated under `metacat-server/build/libs` directory. Metacat needs two basic configurations:

* `metacat.plugin.config.location`: Path to the directory containing the catalog configuration. Please look at catalog [samples](https://github.com/Netflix/metacat/tree/master/metacat-functional-tests/metacat-test-cluster/etc-metacat/catalog) used by demo.
* `metacat.usermetadata.config.location`: Path to the configuration file containing the connection properties to store user metadata. Please look at this [sample](https://github.com/Netflix/metacat/blob/master/metacat-functional-tests/metacat-test-cluster/etc-metacat/usermetadata.properties).

### Using Jetty locally
```
./gradlew -Dmetacat.plugin.config.location=./metacat-functional-tests/metacat-test-cluster/etc-metacat/catalog -Dmetacat.usermetadata.config.location=./metacat-functional-tests/metacat-test-cluster/etc-metacat/usermetadata.properties jettyRunWar
```
REST API can be accessed @ [http://localhost:8080/mds/v1/catalog](http://localhost:8080/mds/v1/catalog)
Swagger API documentation can be accessed @ [http://localhost:8080/docs/api/index.html](http://localhost:8080/docs/api/index.html)

### Using Docker
To start a docker demo, run the command below. This will start a container running the Metacat service, Hive Metastore and Mysql instance.
```
./gradlew buildWarImage startMetacatCluster metacatPorts
```
* `buildWarImage` - Creates a tomcat war docker image that can be used to start a tomcat container
* `startMetacatCluster` - Starts the hive metastore, MySql instance and the Metacat server
* `metacatPorts` - Prints out what exposed ports are mapped to the internal container ports.
Look for the mapped port (`MAPPED_PORT`) to port 8080.

REST API can be accessed @ [http://localhost:<MAPPED_PORT>/mds/v1/catalog](http://localhost:<MAPPED_PORT>/mds/v1/catalog)
Swagger API documentation can be accessed @ [http://localhost:<MAPPED_PORT>/docs/api/index.html](http://localhost:<MAPPED_PORT>/docs/api/index.html)

To stop the docker demo:
```
./gradlew stopMetacatCluster
```
