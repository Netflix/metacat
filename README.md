# Metacat

Forked from https://github.com/Netflix/metacat

## Goal 
We work with multiple clients, their data needs to be cataloged and arranged. Most MDM are geared towards governance, wheras our goal is data understanding.

Eventually the whole python ecosystem for data exploration should be able to leverage this. 


Main deviations form netflix version
1. Spring boot the only way, no tomcat war
1. Connectors do not belong to this repo, move to a plugin style
1. Elasticsearch will be the only store for all metadata
1. Closer relation betweeen user meta and catalog meta

## Introduction

Metacat focusses on solving these three problems:

* Federate views of metadata systems.
* Allow arbitrary metadata storage about data sets.
* Metadata discovery

## Getting Started

```
./gradlew clean build -x test
```
The ES upgrade broke the existing local ES tests. We still need the mysql store.

```
docker pull mysql
docker run -p 3306:3306 --name mysql -e MYSQL_ROOT_PASSWORD=vz -d mysql


CREATE USER 'metacat_user' IDENTIFIED BY 'vz';
GRANT ALL PRIVILEGES ON * . * TO 'metacat_user';
FLUSH PRIVILEGES;

java -Dmetacat.plugin.config.location=./local/catalog/ -Dmetacat.usermetadata.config.location=./local/usermetadata.properties -jar  metacat-app/build/libs/metacat-app-1.3.0-SNAPSHOT.jar


http://localhost:8080/swagger-ui.html

http://localhost:8080/mds/v1/catalog

```

## Working Example
```
Catalog [mysql-56-db] ->  database [sys] -> table [host_summary] -> fields 
```

Metacat is federated Metadata meaning, it asks underlying catalog engines for their own data, augments it with user metadata
