-- Enable the uuid-ossp extension for UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

drop table IF EXISTS TBLS;
drop table IF EXISTS DBS;

create user replicator with replication encrypted password 'replicator_password';
select pg_create_physical_replication_slot('replication_slot');

create table DBS (
                   version bigint not null,
                   id uuid default uuid_generate_v4() not null primary key,
                   name varchar(255) not null unique,
                   location varchar(8192),
                   created_by varchar(255),
                   created_date TIMESTAMP not null,
                   last_updated_by varchar(255),
                   last_updated_date TIMESTAMP not null
);

create table TBLS (
                    version bigint not null,
                    id uuid default uuid_generate_v4() not null primary key,
                    db_name varchar(255) not null,
                    tbl_name varchar(255) not null,
                    previous_metadata_location varchar(8192),
                    metadata_location varchar(8192),
                    params text,
                    created_by varchar(255),
                    created_date TIMESTAMP not null,
                    last_updated_by varchar(255),
                    last_updated_date TIMESTAMP not null,
                    constraint uniq_name unique(db_name, tbl_name),
                    foreign key (db_name) references DBS(name) ON DELETE RESTRICT ON UPDATE CASCADE
);
