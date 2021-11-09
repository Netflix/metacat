drop table if exists definition_metadata;
drop table if exists data_metadata;

create table definition_metadata (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    name STRING(1024) not null,
    data JSON,
    version INT not null,
    is_deleted BOOL default FALSE not null,
    created_by STRING(255) not null,
    created_date TIMESTAMP not null,
    last_updated_by STRING(255) not null,
    last_updated_date TIMESTAMP not null,
    primary key (id),
    constraint definition_metadata_uq unique (name)
);

create table data_metadata (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    uri STRING(4096) not null,
    data JSON,
    version INT not null,
    is_deleted BOOL default FALSE not null,
    created_by STRING(255) not null,
    created_date TIMESTAMP not null,
    last_updated_by STRING(255) not null,
    last_updated_date TIMESTAMP not null,
    primary key (id),
    constraint data_metadata_uq unique (uri)
);