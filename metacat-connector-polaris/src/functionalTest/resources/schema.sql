DROP TABLE IF EXISTS TBLS;
DROP TABLE IF EXISTS DBS;

CREATE TABLE DBS (
                   version BIGINT NOT NULL,
                   id UUID DEFAULT gen_random_uuid() NOT NULL PRIMARY KEY,
                   catalog_name VARCHAR(255),
                   name VARCHAR(255) NOT NULL,
                   location VARCHAR(8192),
                   params text,
                   created_by VARCHAR(255),
                   created_date TIMESTAMP NOT NULL,
                   last_updated_by VARCHAR(255),
                   last_updated_date TIMESTAMP NOT NULL,
                   CONSTRAINT unique_catalog_name_db UNIQUE (catalog_name, name)
);

CREATE TABLE TBLS (
                    version BIGINT NOT NULL,
                    id UUID DEFAULT gen_random_uuid() NOT NULL PRIMARY KEY,
                    catalog_name VARCHAR(255),
                    db_name VARCHAR(255) NOT NULL,
                    tbl_name VARCHAR(255) NOT NULL,
                    previous_metadata_location VARCHAR(8192),
                    metadata_location VARCHAR(8192),
                    params TEXT,
                    created_by VARCHAR(255),
                    created_date TIMESTAMP NOT NULL,
                    last_updated_by VARCHAR(255),
                    last_updated_date TIMESTAMP NOT NULL,
                    CONSTRAINT unique_catalog_db_tbl UNIQUE (catalog_name, db_name, tbl_name),
                    CONSTRAINT fk_tbls_db FOREIGN KEY (catalog_name, db_name) REFERENCES DBS(catalog_name, name) ON DELETE CASCADE ON UPDATE CASCADE
);
