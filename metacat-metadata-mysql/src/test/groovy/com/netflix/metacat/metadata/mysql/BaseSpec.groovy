/*
 *       Copyright 2017 Netflix, Inc.
 *          Licensed under the Apache License, Version 2.0 (the "License");
 *          you may not use this file except in compliance with the License.
 *          You may obtain a copy of the License at
 *              http://www.apache.org/licenses/LICENSE-2.0
 *          Unless required by applicable law or agreed to in writing, software
 *          distributed under the License is distributed on an "AS IS" BASIS,
 *          WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *          See the License for the specific language governing permissions and
 *          limitations under the License.
 */

package com.netflix.metacat.metadata.mysql

import com.netflix.metacat.common.json.MetacatJsonLocator
import com.netflix.metacat.common.server.properties.DefaultConfigImpl
import com.netflix.metacat.common.server.properties.MetacatProperties
import com.netflix.metacat.common.server.util.DataSourceManager
import io.airlift.testing.mysql.TestingMySqlServer
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement
import java.util.concurrent.atomic.AtomicBoolean

import static java.lang.String.format

@Ignore
class BaseSpec extends Specification {
    private static final AtomicBoolean initialized = new AtomicBoolean();
    @Shared
    TestingMySqlServer mysqlServer
    @Shared
    MysqlUserMetadataService mysqlUserMetadataService

    def setupSpec() {
        if (!initialized.compareAndSet(false, true)) {
            return
        }
        setupMysql()

        // TODO: Perhaps this should be mocked?
        MySqlServiceUtil.loadMySqlDataSource(DataSourceManager.get(), "usermetadata.properties");
        mysqlUserMetadataService = new MysqlUserMetadataService(
            DataSourceManager.get().get(MysqlUserMetadataService.NAME_DATASOURCE),
            new MetacatJsonLocator(),
            new DefaultConfigImpl(
                new MetacatProperties()
            )
        )
    }

    def setupMysql() {
        mysqlServer = new TestingMySqlServer("test", "test", "metacat")
        Properties props = new Properties()
        props.setProperty('javax.jdo.option.url', format("jdbc:mysql://localhost:%d/%s?user=%s&password=%s", mysqlServer.port, "metacat", mysqlServer.user, mysqlServer.password))
        props.setProperty('javax.jdo.option.username', mysqlServer.getUser())
        props.setProperty('javax.jdo.option.password', mysqlServer.getPassword())
        props.setProperty('javax.jdo.option.defaultTransactionIsolation', 'READ_COMMITTED')
        props.setProperty('javax.jdo.option.defaultAutoCommit', 'false');
        URL url = Thread.currentThread().getContextClassLoader().getResource("usermetadata.properties")
        Path filePath
        if (url != null) {
            filePath = Paths.get(url.toURI());
        } else {
            File metadataFile = new File('src/test/resources/usermetadata.properties')
            if (!metadataFile.exists()) {
                metadataFile = new File('metacat-user-metadata-mysql/src/test/resources/usermetadata.properties')
            }
            filePath = Paths.get(metadataFile.getPath())
        }
        props.store(Files.newOutputStream(filePath), "test")

        File prepareFile = new File('src/test/resources/sql/prepare-test.sql')
        if (!prepareFile.exists()) {
            prepareFile = new File('metacat-user-metadata-mysql/src/test/resources/sql/prepare-test.sql')
        }
        runScript(DriverManager.getConnection(mysqlServer.getJdbcUrl()), new FileReader(prepareFile), ';')
        //forcing loading the mysql properties
        MySqlServiceUtil.loadMySqlDataSource(DataSourceManager.get(), "usermetadata.properties")
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
