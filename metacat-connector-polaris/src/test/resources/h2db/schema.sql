drop table action_config if exists;

create table DBS (
                 version bigint not null,
                 id IDENTITY not null primary key,
                 name varchar(255) not null
);
