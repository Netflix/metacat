drop table IF EXISTS TBLS;
drop table IF EXISTS DBS;

create table DBS (
    version bigint not null,
    id uuid default gen_random_uuid() not null primary key,
    name varchar(255) not null unique,
    location varchar(8192),
    created_by STRING(255),
    created_date TIMESTAMP not null,
    last_updated_by STRING(255),
    last_updated_date TIMESTAMP not null
);

create table TBLS (
    version bigint not null,
    id uuid default gen_random_uuid() not null primary key,
    db_name varchar(255) not null,
    tbl_name varchar(255) not null,
    previous_metadata_location varchar(8192),
    metadata_location varchar(8192),
    params text,
    created_by STRING(255),
    created_date TIMESTAMP not null,
    last_updated_by STRING(255),
    last_updated_date TIMESTAMP not null,
    constraint uniq_name unique(db_name, tbl_name),
    foreign key (db_name) references DBS(name) ON DELETE CASCADE ON UPDATE CASCADE
);
