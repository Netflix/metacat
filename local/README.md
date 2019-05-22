docker pull mysql
docker run -p 3306:3306 --name mysql -e MYSQL_ROOT_PASSWORD=vz -d mysql


CREATE USER 'metacat_user' IDENTIFIED BY 'vz';
GRANT ALL PRIVILEGES ON * . * TO 'metacat_user';
FLUSH PRIVILEGES;

java -Dmetacat.plugin.config.location=./local/catalog/ -Dmetacat.usermetadata.config.location=./local/usermetadata.properties -jar  metacat-app/build/libs/metacat-app-1.3.0-SNAPSHOT.jar


http://localhost:8080/swagger-ui.html

http://localhost:8080/mds/v1/catalog



## Working Example
```
Catalog [mysql-56-db] ->  database [sys] -> table [host_summary] -> fields 
```

Metacat is federated Metadata meaning, it asks underlying catalog engines for their own data, augments it with user metadata
