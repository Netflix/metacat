# Metacat

Fork from netflix metacat. Major differences
1. Connectors not part of repo
1. ES as primary storage

## Introduction

Metacat is a unified metadata exploration API service. 
Metacat focusses on solving these three problems:

* Federate views of metadata systems.
* Allow arbitrary metadata storage about data sets.
* Metadata discovery

## Getting Started

Sadly still uses mysql, so get the docker image for mysql, create the user, use the properties file from local folder
```
docker pull mysql
docker run -p 3306:3306 --name mysql -e MYSQL_ROOT_PASSWORD=vz -d mysql


CREATE USER 'metacat_user' IDENTIFIED BY 'vz';
GRANT ALL PRIVILEGES ON * . * TO 'metacat_user';
FLUSH PRIVILEGES;


./gradlew build -x test
java -Dmetacat.plugin.config.location=./local/catalog/ -Dmetacat.usermetadata.config.location=./local/usermetadata.properties -jar  metacat-app/build/libs/metacat-app-1.3.0-SNAPSHOT.jar


http://localhost:8080/swagger-ui.html

http://localhost:8080/mds/v1/catalog
```

We broke the ES tests in the ES upgrade process, yet to fix it.


## UI setup
Caddy config
```

others.local.ai:80 {
    log /data/tools/caddy/logs/others-subdomain.log

    # root points to the dist folder of yarn build
    root /data/work/voicezen/code/misc/

    proxy /api http://localhost:8080 {
        without /api
    }
    header /reports Access-Control-Allow-Origin *
    header /reports Access-Control-Allow-Headers content-type,authorization
    header /reports/* Access-Control-Allow-Origin *
    header /reports/* Access-Control-Allow-Headers content-type,authorization

}
```
UI is in metacat-ui repo, clone the repo and symbolic link metacat-ui to /data/work/voicezen/code/misc/metacat
others.local.ai is just an alias to localhost via hosts file
and we can now browse to 
```
http://others.local.ai/metacat/
```

