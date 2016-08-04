-- MySQL dump 10.13  Distrib 5.6.29, for osx10.11 (x86_64)

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

USE metacat;

--
-- Table structure for table `database_object`
--

DROP TABLE IF EXISTS `database_object`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `database_object` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `version` bigint(20) NOT NULL,
  `date_created` datetime NOT NULL,
  `last_updated` datetime NOT NULL,
  `name` varchar(255) NOT NULL,
  `source_id` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `source_id` (`source_id`,`name`),
  KEY `FK36D2FE837DA82D09` (`source_id`),
  CONSTRAINT `FK36D2FE837DA82D09` FOREIGN KEY (`source_id`) REFERENCES `source` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=343 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `field`
--

DROP TABLE IF EXISTS `field`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `field` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `version` bigint(20) NOT NULL,
  `comment` varchar(255) DEFAULT NULL,
  `date_created` datetime NOT NULL,
  `last_updated` datetime NOT NULL,
  `name` varchar(255) NOT NULL,
  `partition_key` bit(1) NOT NULL,
  `pos` int(11) NOT NULL,
  `schema_id` bigint(20) NOT NULL,
  `source_type` varchar(1024) DEFAULT NULL,
  `type` varchar(1024) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `schema_id` (`schema_id`,`pos`),
  UNIQUE KEY `schema_id_2` (`schema_id`,`name`),
  KEY `FK5CEA0FAD915F749` (`schema_id`),
  CONSTRAINT `FK5CEA0FAD915F749` FOREIGN KEY (`schema_id`) REFERENCES `schema_object` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=98177 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `info`
--

DROP TABLE IF EXISTS `info`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `info` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `version` bigint(20) NOT NULL,
  `date_created` datetime NOT NULL,
  `input_format` varchar(255) DEFAULT NULL,
  `last_updated` datetime NOT NULL,
  `location_id` bigint(20) NOT NULL,
  `output_format` varchar(255) DEFAULT NULL,
  `owner` varchar(255) DEFAULT NULL,
  `serialization_lib` varchar(255) DEFAULT NULL,
  `last_updated_by` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `location_id` (`location_id`),
  KEY `FK3164AEE3863F09` (`location_id`),
  CONSTRAINT `FK3164AEE3863F09` FOREIGN KEY (`location_id`) REFERENCES `location` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2625 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `info_parameters`
--

DROP TABLE IF EXISTS `info_parameters`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `info_parameters` (
  `info_id` bigint(20) NOT NULL,
  `parameters_idx` varchar(255) DEFAULT NULL,
  `parameters_elt` varchar(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `location`
--

DROP TABLE IF EXISTS `location`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `location` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `version` bigint(20) NOT NULL,
  `date_created` datetime NOT NULL,
  `last_updated` datetime NOT NULL,
  `table_id` bigint(20) NOT NULL,
  `uri` varchar(4000) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `table_id` (`table_id`),
  KEY `FK714F9FB53512DFEB` (`table_id`),
  CONSTRAINT `FK714F9FB53512DFEB` FOREIGN KEY (`table_id`) REFERENCES `table_object` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3019 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `partition_table`
--

DROP TABLE IF EXISTS `partition_table`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `partition_table` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `version` bigint(20) NOT NULL,
  `date_created` datetime NOT NULL,
  `last_updated` datetime NOT NULL,
  `name` varchar(255) NOT NULL,
  `table_id` bigint(20) NOT NULL,
  `uri` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `table_id` (`table_id`,`name`),
  KEY `FK99425A393512DFEB` (`table_id`),
  CONSTRAINT `FK99425A393512DFEB` FOREIGN KEY (`table_id`) REFERENCES `table_object` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2480148 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `schema_object`
--

DROP TABLE IF EXISTS `schema_object`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `schema_object` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `version` bigint(20) NOT NULL,
  `date_created` datetime NOT NULL,
  `last_updated` datetime NOT NULL,
  `location_id` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `location_id` (`location_id`),
  KEY `FKDE57039DE3863F09` (`location_id`),
  CONSTRAINT `FKDE57039DE3863F09` FOREIGN KEY (`location_id`) REFERENCES `location` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2734 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `source`
--

DROP TABLE IF EXISTS `source`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `source` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `version` bigint(20) NOT NULL,
  `date_created` datetime NOT NULL,
  `disabled` bit(1) NOT NULL,
  `last_updated` datetime NOT NULL,
  `name` varchar(255) NOT NULL,
  `thrift_uri` varchar(255) DEFAULT NULL,
  `type` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

-- 
-- Insert into `source`
--
INSERT INTO `source`
(`id`, `version`, `date_created`, `disabled`, `last_updated`, `name`, `thrift_uri`, `type`)
VALUES
(1, 0, NOW(), 0, NOW(), 's3-mysql-db', NULL, 's3');

--
-- Table structure for table `table_object`
--

DROP TABLE IF EXISTS `table_object`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `table_object` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `version` bigint(20) NOT NULL,
  `database_id` bigint(20) DEFAULT NULL,
  `date_created` datetime NOT NULL,
  `last_updated` datetime NOT NULL,
  `name` varchar(255) NOT NULL,
  `class` varchar(255) NOT NULL DEFAULT 'franklin.Table',
  `promoted` datetime DEFAULT NULL,
  `table_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `database_id` (`database_id`,`name`),
  KEY `FK586A20D0EC9EA149` (`database_id`),
  KEY `FK586A20D03512DFEB` (`table_id`),
  CONSTRAINT `FK586A20D03512DFEB` FOREIGN KEY (`table_id`) REFERENCES `table_object` (`id`),
  CONSTRAINT `FK586A20D0EC9EA149` FOREIGN KEY (`database_id`) REFERENCES `database_object` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=427439 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2016-03-02 14:42:42
