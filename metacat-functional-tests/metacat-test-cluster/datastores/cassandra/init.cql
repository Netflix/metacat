CREATE KEYSPACE IF NOT EXISTS bills WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE bills;

CREATE TABLE IF NOT EXISTS bills_compress (
    user text,
    balance int static,
    expense_id int,
    amount int,
    name text,
    PRIMARY KEY (user, expense_id)
);


CREATE TABLE IF NOT EXISTS bills_nc (
    user text,
    balance int static,
    expense_id int,
    amount int,
    name text,
    PRIMARY KEY (user, expense_id)
);

INSERT INTO bills_nc (user, balance, expense_id, amount, name) VALUES ('user3', 100, 11, 88, 'minh');

INSERT INTO bills_nc (user, balance, expense_id, amount, name) VALUES ('user3', 100, 12, 10, 'minh');

INSERT INTO bills_nc (user, balance, expense_id, amount, name) VALUES ('user1', 100, 12, 10, 'minh');

INSERT INTO bills_nc (user, balance, expense_id, amount, name) VALUES ('user1', 100, 22, 10, 'minh');

INSERT INTO bills_nc (user, balance, expense_id, amount, name) VALUES ('user1', 120, 22, 10, 'minh');


INSERT INTO bills_compress (user, balance, expense_id, amount, name) VALUES ('user3', 100, 11, 88, 'minh');

INSERT INTO bills_compress (user, balance, expense_id, amount, name) VALUES ('user3', 100, 12, 10, 'minh');

INSERT INTO bills_compress (user, balance, expense_id, amount, name) VALUES ('user1', 100, 12, 10, 'minh');

INSERT INTO bills_compress (user, balance, expense_id, amount, name) VALUES ('user1', 100, 22, 10, 'minh');

INSERT INTO bills_compress (user, balance, expense_id, amount, name) VALUES ('user1', 120, 22, 10, 'minh');

CREATE KEYSPACE IF NOT EXISTS real_estate WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE real_estate;

CREATE TABLE IF NOT EXISTS  houses (
    owner text,
    sq_ft int,
    lot_size int,
    cost bigint,
    address text,
    apn text,
    PRIMARY KEY (apn)
);


CREATE TABLE IF NOT EXISTS  apartments (
    renter text,
    complex_name text,
    sq_ft int,
    rent int,
    apt_number int,
    has_washer_dryer boolean,
    PRIMARY KEY (complex_name, apt_number)
);
