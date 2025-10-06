-- Enable the uuid-ossp extension for UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

DROP TABLE IF EXISTS TBLS;
DROP TABLE IF EXISTS DBS;

create user replicator with replication encrypted password 'replicator_password';
select pg_create_physical_replication_slot('replication_slot');

CREATE TABLE DBS (
                   version bigint NOT NULL,
                   id uuid DEFAULT uuid_generate_v4() NOT NULL PRIMARY KEY,
                   catalog_name varchar(255),
                   name varchar(255) NOT NULL,
                   location varchar(8192),
                   params text,
                   created_by varchar(255),
                   created_date TIMESTAMP NOT NULL,
                   last_updated_by varchar(255),
                   last_updated_date TIMESTAMP NOT NULL,
                   CONSTRAINT unique_catalog_name_db UNIQUE (catalog_name, name)
);

CREATE TABLE TBLS (
                    version bigint NOT NULL,
                    id uuid DEFAULT uuid_generate_v4() NOT NULL PRIMARY KEY,
                    catalog_name varchar(255),
                    db_name varchar(255) NOT NULL,
                    tbl_name varchar(255) NOT NULL,
                    previous_metadata_location varchar(8192),
                    metadata_location varchar(8192),
                    params text,
                    created_by varchar(255),
                    created_date TIMESTAMP NOT NULL,
                    last_updated_by varchar(255),
                    last_updated_date TIMESTAMP NOT NULL,
                    CONSTRAINT unique_catalog_db_tbl UNIQUE (catalog_name, db_name, tbl_name),
                    CONSTRAINT fk_tbls_db FOREIGN KEY (catalog_name, db_name) REFERENCES DBS(catalog_name, name) ON DELETE RESTRICT ON UPDATE CASCADE
);
