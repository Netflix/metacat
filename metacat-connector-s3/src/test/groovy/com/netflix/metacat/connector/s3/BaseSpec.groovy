/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.connector.s3

import com.google.inject.persist.PersistService
import com.netflix.metacat.common.server.CommonModule
import com.netflix.metacat.connector.s3.model.Database
import com.netflix.metacat.connector.s3.model.Field
import com.netflix.metacat.connector.s3.model.Info
import com.netflix.metacat.connector.s3.model.Location
import com.netflix.metacat.connector.s3.model.Partition
import com.netflix.metacat.connector.s3.model.Schema
import com.netflix.metacat.connector.s3.model.Source
import com.netflix.metacat.connector.s3.model.Table
import io.airlift.testing.mysql.TestingMySqlServer
import spock.guice.UseModules
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification

import javax.inject.Inject
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement

@UseModules([
        CommonModule.class,
        S3TestModule.class
])
@Ignore
class BaseSpec extends Specification {
    @Shared @Inject TestingMySqlServer mysqlServer
    @Shared @Inject PersistService persistService
    @Shared Map<String, Source> sources
    @Shared Map<String, Database> databases
    @Shared Map<String, Table> tables
    @Shared Map<String, Partition> partitions

    def setupSpec() {
        setupMysql()
        setModels()
    }

    def setModels() {
        // source
        def source = new Source(name:'s3', type:'s3')
        // databases
        def database = new Database(name: 'test', source: source)
        def database1 = new Database(name: 'test1', source: source)
        // Table 1
        def location = new Location(uri:'s3://')
        def info = new Info(owner: 'amajumdar', inputFormat: 'text', location: location)
        def schema = new Schema(location: location)
        def field = new Field(name:'a', type:'chararray', partitionKey: true, schema: schema)
        schema.setFields([field])
        location.setInfo(info)
        location.setSchema(schema)
        def table = new Table(name: 'part', location: location, database: database)
        location.setTable(table)
        // Table 2
        def location1 = new Location(uri:'s3://')
        def info1 = new Info(owner: 'amajumdar', inputFormat: 'text', location: location1)
        def schema1 = new Schema(location: location1)
        def field1 = new Field(name:'a', type:'chararray', partitionKey: true, schema: schema1)
        def field2 = new Field(name:'b', type:'chararray', partitionKey: true, schema: schema1)
        schema1.setFields([field1, field2])
        location1.setInfo(info1)
        location1.setSchema(schema1)
        def table1 = new Table(name: 'part1', location: location1, database: database)
        location1.setTable(table1)
        //Partitions
        def partition = new Partition(name:'dateint=20171212', uri:'s3://part/dateint=20171212', table: table)
        def partition1 = new Partition(name:'dateint=20171213', uri:'s3://part/dateint=20171213', table: table)
        def partition2 = new Partition(name:'dateint=20171214', uri:'s3://part/dateint=20171214', table: table)

        sources = ['s3': source]
        databases = ['test': database, 'test1': database1]
        tables = ['part': table, 'part1': table1]
        partitions = ['dateint=20171212': partition, 'dateint=20171213': partition1, 'dateint=20171214': partition2]
    }

    def setupMysql() {
        File prepareFile = new File('src/test/resources/sql/prepare-test.sql')
        if( !prepareFile.exists()){
            prepareFile = new File('metacat-s3-connector/src/test/resources/sql/prepare-test.sql')
        }
        runScript(DriverManager.getConnection(mysqlServer.getJdbcUrl()), new FileReader(prepareFile), ';')

        persistService.start()
    }

    def runScript(Connection conn, Reader reader, String delimiter) throws IOException,
            SQLException {
        StringBuffer command = null;
        try {
            LineNumberReader lineReader = new LineNumberReader(reader);
            String line = null;
            while ((line = lineReader.readLine()) != null) {
                if (command == null) {
                    command = new StringBuffer();
                }
                String trimmedLine = line.trim();
                if (trimmedLine.startsWith("--")) {
                    println(trimmedLine);
                } else if (trimmedLine.length() < 1
                        || trimmedLine.startsWith("//")) {
                    // Do nothing
                } else if (trimmedLine.length() < 1
                        || trimmedLine.startsWith("--")) {
                    // Do nothing
                } else if (trimmedLine.endsWith(delimiter)) {
                    command.append(line.substring(0, line
                            .lastIndexOf(delimiter)));
                    command.append(" ");
                    Statement statement = conn.createStatement();

                    println(command);
                    statement.execute(command.toString());

                    command = null;
                    try {
                        statement.close();
                    } catch (Exception e) {
                        // Ignore to workaround a bug in Jakarta DBCP
                    }
                    Thread.yield();
                } else {
                    command.append(line);
                    command.append(" ");
                }
            }
        } catch (Exception e) {
            throw e;
        }
    }

    def cleanupSpec() {
        if( persistService != null){
            persistService.stop()
        }
        if (mysqlServer != null) {
            mysqlServer.close()
        }
    }
}
