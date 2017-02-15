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

package com.netflix.metacat.main.manager

import com.google.inject.Inject
import com.netflix.metacat.main.init.MetacatInitializationService
import com.netflix.metacat.main.init.MetacatServletModule
import com.netflix.metacat.usermetadata.mysql.MysqlUserMetadataModule
import io.airlift.testing.mysql.TestingMySqlServer
import spock.guice.UseModules
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification

import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement
import java.util.concurrent.atomic.AtomicBoolean

@UseModules([
        MetacatServletModule.class,
        MysqlUserMetadataModule.class,
])
@Ignore
class BaseSpec extends Specification {
    private static final AtomicBoolean initialized = new AtomicBoolean();
    @Inject
    @Shared
    MetacatInitializationService metacatInitializationService
    @Shared
    TestingMySqlServer mysqlServer;

    def setupSpec() {
        if (!initialized.compareAndSet(false, true)) {
            return;
        }
        setupMysql()
        metacatInitializationService.start()
    }

    def setupMysql() {
        mysqlServer = new TestingMySqlServer("test", "test", "example", "tpch")
        Properties props = new Properties()
        props.setProperty('connector.name', 'mysql')
        props.setProperty('connection-url', mysqlServer.getJdbcUrl())
        props.setProperty('connection-user', mysqlServer.getUser())
        props.setProperty('connection-password', mysqlServer.getPassword())

        File defaultFile = new File('build/resources/test/etc/catalog/default.properties')
        props.store(new FileOutputStream(defaultFile), "test")

        props.setProperty('javax.jdo.option.url', mysqlServer.getJdbcUrl())
        props.setProperty('javax.jdo.option.username', mysqlServer.getUser())
        props.setProperty('javax.jdo.option.password', mysqlServer.getPassword())
        File metadataFile = new File('build/resources/test/usermetadata.properties')
        props.store(new FileOutputStream(metadataFile), "test")


        File prepareFile = new File('build/resources/test/sql/prepare-test.sql')
        runScript(DriverManager.getConnection(mysqlServer.getJdbcUrl()), new FileReader(prepareFile), ';')
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
        if (mysqlServer != null) {
            mysqlServer.close()
        }
    }
}
