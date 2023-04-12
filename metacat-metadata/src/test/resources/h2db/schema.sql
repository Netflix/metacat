drop table if exists definition_metadata;
drop table if exists data_metadata;

create table definition_metadata (
    id varchar(255) not null primary key,
    name varchar(1024) not null,
    data json,
    version bigint not null,
    is_deleted bool default false not null,
    created_by varchar(255) not null,
    created_date timestamp(3) not null,
    last_updated_by varchar(255) not null,
    last_updated_date timestamp(3) not null,
    constraint def_metadata_uq unique (name)
);

create table data_metadata (
    id varchar(255) not null primary key,
    uri varchar(4000) not null,
    data json,
    version bigint not null,
    is_deleted bool default false not null,
    created_by varchar(255) not null,
    created_date timestamp(3) not null,
    last_updated_by varchar(255) not null,
    last_updated_date timestamp(3) not null,
    constraint data_uq unique (uri)
);

