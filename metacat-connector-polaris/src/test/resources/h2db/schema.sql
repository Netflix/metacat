drop table if exists TBLS;
drop table if exists DBS;
drop table if exists TBL_PARAMS;

create table DBS (
  version bigint not null,
  id varchar(255) default random_uuid() not null primary key,
  name varchar(255) not null unique,
  location varchar(1024),
  created_by varchar(255),
  created_date TIMESTAMP not null,
  last_updated_by varchar(255),
  last_updated_date TIMESTAMP not null
);

create table TBLS (
  version bigint not null,
  id varchar(255) default random_uuid() not null primary key,
  db_name varchar(255) not null,
  tbl_name varchar(255) not null,
  previous_metadata_location varchar(1024),
  metadata_location varchar(1024),
  params TEXT,
  constraint uniq_name unique(db_name, tbl_name),
  created_by varchar(255),
  created_date TIMESTAMP not null,
  last_updated_by varchar(255),
  last_updated_date TIMESTAMP not null,
  foreign key (db_name) references DBS(name) ON DELETE CASCADE ON UPDATE CASCADE
);

create table TBL_PARAMS (
  polaris_table_entity_id varchar(255) not null,
  param_key varchar(255) not null,
  param_value varchar(255) not null
);

CREATE INDEX DB_NAME_IDX ON TBLS(db_name);
