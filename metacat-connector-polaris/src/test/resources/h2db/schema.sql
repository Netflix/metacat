drop table TBLS if exists;
drop table DBS if exists;

create table DBS (
  version bigint not null,
  id IDENTITY not null primary key,
  name varchar(255) not null unique,
  location varchar(1024),
  created_by varchar(255),
  created_date TIMESTAMP not null,
  last_updated_by varchar(255),
  last_updated_date TIMESTAMP not null
);

create table TBLS (
  version bigint not null,
  id IDENTITY not null primary key,
  db_name varchar(255) not null,
  tbl_name varchar(255) not null,
  previous_metadata_location varchar(1024),
  metadata_location varchar(1024),
  constraint uniq_name unique(db_name, tbl_name),
  created_by varchar(255),
  created_date TIMESTAMP not null,
  last_updated_by varchar(255),
  last_updated_date TIMESTAMP not null,
  foreign key (db_name) references DBS(name) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE INDEX DB_NAME_IDX ON TBLS(db_name);
