drop table TBLS if exists;
drop table DBS if exists;

create table DBS (
  version bigint not null,
  id IDENTITY not null primary key,
  name varchar(255) not null unique
);

create table TBLS (
  version bigint not null,
  id IDENTITY not null primary key,
  db_name varchar(255) not null,
  tbl_name varchar(255) not null,
  metadata_location varchar(1024),
  constraint uniq_name unique(db_name, tbl_name),
  foreign key (db_name) references DBS(name)
);

CREATE INDEX DB_NAME_IDX ON TBLS(db_name);
