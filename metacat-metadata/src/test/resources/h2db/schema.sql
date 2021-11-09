drop table definition_metadata if exists;
drop table data_metadata if exists;

create table definition_metadata (
    id IDENTITY not null primary key,
    name varchar(1024) not null,
    data json,
    version bigint not null,
    is_deleted bit default false not null,
    created_by varchar(255) not null,
    created_date timestamp(3) not null,
    last_updated_by varchar(255) not null,
    last_updated_date timestamp(3) not null,
    primary key (id),
    constraint def_metadata_uq unique (name)
);

create table data_metadata (
    id IDENTITY not null primary key,
    uri varchar(4000) not null,
    data json,
    version bigint not null,
    is_deleted bit default false not null,
    created_by varchar(255) not null,
    created_date timestamp(3) not null,
    last_updated_by varchar(255) not null,
    last_updated_date timestamp(3) not null,
    primary key (id),
    constraint data_uq unique (uri)
);

