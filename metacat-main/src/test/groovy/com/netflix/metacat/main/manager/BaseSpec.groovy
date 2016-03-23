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

import com.facebook.presto.Session
import com.facebook.presto.metadata.SessionPropertyManager
import com.facebook.presto.spi.security.Identity
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

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY
import static java.util.Locale.ENGLISH

@UseModules([
        MetacatServletModule.class,
        MysqlUserMetadataModule.class,
])
@Ignore
class BaseSpec extends Specification {

    public static final Session TEST_SESSION = Session.builder(new SessionPropertyManager())
            .setIdentity(new Identity("user", Optional.empty()))
            .setSource("source")
            .setCatalog("example")
            .setSchema("tiny")
            .setTimeZoneKey(UTC_KEY)
            .setLocale(ENGLISH)
            .setRemoteUserAddress("address")
            .setUserAgent("agent")
            .build();
    public static final Session TEST_MYSQL_SESSION = Session.builder(new SessionPropertyManager())
            .setIdentity(new Identity("user", Optional.empty()))
            .setSource("test")
            .setCatalog("mysql")
            .setSchema("metacat")
            .setTimeZoneKey(UTC_KEY)
            .setLocale(ENGLISH)
            .build();
    public static final Session TEST_HIVE_SESSION = Session.builder(new SessionPropertyManager())
            .setIdentity(new Identity("user", Optional.empty()))
            .setSource("test")
            .setCatalog("hive")
            .setSchema("metacat")
            .setTimeZoneKey(UTC_KEY)
            .setLocale(ENGLISH)
            .build();
    public static final Session TEST_FRANKLIN_SESSION = Session.builder(new SessionPropertyManager())
            .setIdentity(new Identity("user", Optional.empty()))
            .setSource("test")
            .setCatalog("franklin")
            .setSchema("metacat")
            .setTimeZoneKey(UTC_KEY)
            .setLocale(ENGLISH)
            .build();
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

        File defaultFile = new File('src/test/resources/etc/catalog/default.properties')
        if( !defaultFile.exists()){
            defaultFile = new File('metacat-main/src/test/resources/etc/catalog/default.properties')
        }
        props.store(new FileOutputStream(defaultFile), "test")

        props.setProperty('javax.jdo.option.url', mysqlServer.getJdbcUrl())
        props.setProperty('javax.jdo.option.username', mysqlServer.getUser())
        props.setProperty('javax.jdo.option.password', mysqlServer.getPassword())
        File metadataFile = new File('src/test/resources/usermetadata.properties')
        if( !metadataFile.exists()){
            metadataFile = new File('metacat-main/src/test/resources/usermetadata.properties')
        }
        props.store(new FileOutputStream(metadataFile), "test")


        File prepareFile = new File('src/test/resources/sql/prepare-test.sql')
        if( !prepareFile.exists()){
            prepareFile = new File('metacat-main/src/test/resources/sql/prepare-test.sql')
        }
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
