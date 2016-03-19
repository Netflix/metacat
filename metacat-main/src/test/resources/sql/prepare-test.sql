CREATE SCHEMA if not exists example;
CREATE TABLE example.numbers(text varchar(20) primary key, value bigint);
INSERT INTO example.numbers(text, value) VALUES ('one', 1),('two', 2),('three', 3),('ten', 10),('eleven', 11),('twelve', 12);
CREATE SCHEMA if not exists tpch;
CREATE TABLE tpch.orders(orderkey bigint primary key, custkey bigint);
CREATE TABLE tpch.lineitem(orderkey bigint primary key, partkey bigint);
