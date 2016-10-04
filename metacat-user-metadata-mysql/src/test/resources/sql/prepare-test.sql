CREATE SCHEMA if not exists metacat;
-- Create syntax for TABLE 'data_metadata'
CREATE TABLE metacat.data_metadata (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  version bigint(20) NOT NULL,
  created_by varchar(255) NOT NULL,
  data longtext NOT NULL,
  date_created datetime NOT NULL,
  last_updated datetime NOT NULL,
  last_updated_by varchar(255) NOT NULL,
  uri varchar(4000) NOT NULL DEFAULT '',
  PRIMARY KEY (id),
  KEY uri (uri(767))
) DEFAULT CHARSET=latin1;

-- Create syntax for TABLE 'data_metadata_delete'
CREATE TABLE metacat.data_metadata_delete (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  uri varchar(4000) NOT NULL DEFAULT '',
  created_by varchar(255) NOT NULL,
  date_created datetime NOT NULL,
  PRIMARY KEY (id),
  KEY uri(uri(767)),
  KEY date_created (date_created)
) DEFAULT CHARSET=latin1;

-- Create syntax for TABLE 'definition_metadata'
CREATE TABLE metacat.definition_metadata (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  version bigint(20) NOT NULL,
  created_by varchar(255) NOT NULL,
  data longtext NOT NULL,
  date_created datetime NOT NULL,
  last_updated datetime NOT NULL,
  last_updated_by varchar(255) NOT NULL,
  name varchar(255) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY name (name)
) DEFAULT CHARSET=latin1;

-- Create syntax for TABLE 'lookup'
CREATE TABLE metacat.lookup (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  version bigint(20) NOT NULL,
  created_by varchar(255) NOT NULL,
  date_created datetime NOT NULL,
  last_updated datetime NOT NULL,
  last_updated_by varchar(255) NOT NULL,
  name varchar(255) NOT NULL,
  type varchar(255) NOT NULL,
  value varchar(4000) NOT NULL DEFAULT '',
  PRIMARY KEY (id),
  UNIQUE KEY name (name)
) DEFAULT CHARSET=latin1;

-- Create syntax for TABLE 'lookup_values'
CREATE TABLE metacat.lookup_values (
  lookup_id bigint(20) DEFAULT NULL,
  values_string varchar(255) DEFAULT NULL,
  KEY lookup_values_fk_looup_id (lookup_id),
  CONSTRAINT lookup_values_fk_looup_id FOREIGN KEY (lookup_id) REFERENCES lookup (id)
) DEFAULT CHARSET=latin1;

-- Create syntax for TABLE 'tag_item'
CREATE TABLE metacat.tag_item (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  version bigint(20) NOT NULL,
  created_by varchar(255) NOT NULL,
  date_created datetime NOT NULL,
  last_updated datetime NOT NULL,
  last_updated_by varchar(255) NOT NULL,
  name varchar(255) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY name (name)
) DEFAULT CHARSET=latin1;

-- Create syntax for TABLE 'tag_item_tags'
CREATE TABLE metacat.tag_item_tags (
  tag_item_id bigint(20) DEFAULT NULL,
  tags_string varchar(255) NOT NULL DEFAULT '',
  KEY tag_item_tags_fk (tag_item_id),
  KEY tags_string (tags_string),
  CONSTRAINT tag_item_tags_fk FOREIGN KEY (tag_item_id) REFERENCES tag_item (id)
) DEFAULT CHARSET=latin1;
