package com.netflix.metacat.s3.connector

import com.google.inject.Inject
import com.google.inject.persist.PersistService
import com.netflix.metacat.common.server.CommonModule
import com.netflix.metacat.converters.ConvertersModule
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
        CommonModule.class,
        S3TestModule.class,
        ConvertersModule.class
])
@Ignore
class BaseSpec extends Specification {
    private static final AtomicBoolean initialized = new AtomicBoolean();
    @Shared @Inject
    TestingMySqlServer mysqlServer;
    @Shared @Inject
    PersistService persistService

    def setupSpec() {
        if (!initialized.compareAndSet(false, true)) {
            return;
        }
        setupMysql()
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
