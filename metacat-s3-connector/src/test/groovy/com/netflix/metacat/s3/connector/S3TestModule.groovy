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

package com.netflix.metacat.s3.connector

import com.facebook.presto.hive.HiveClientModule
import com.facebook.presto.type.TypeRegistry
import com.google.common.collect.Maps
import com.google.inject.Binder
import com.google.inject.Module
import com.google.inject.persist.jpa.JpaPersistModule
import com.google.inject.util.Modules
import io.airlift.configuration.ConfigurationFactory
import io.airlift.testing.mysql.TestingMySqlServer

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import static java.lang.String.format

/**
 * Created by amajumdar on 10/12/15.
 */
class S3TestModule implements Module{
    @Override
    void configure(Binder binder) {
        TestingMySqlServer mysqlServer = new TestingMySqlServer("test", "test", "metacat")
        Properties props = new Properties()
        props.setProperty('javax.persistence.jdbc.url', format("jdbc:mysql://localhost:%d/%s?user=%s&password=%s", mysqlServer.port, "metacat", mysqlServer.user, mysqlServer.password))
        props.setProperty('javax.persistence.jdbc.user', mysqlServer.getUser())
        props.setProperty('javax.persistence.jdbc.password', mysqlServer.getPassword())
        props.setProperty('javax.persistence.jdbc.driver', 'com.mysql.jdbc.Driver')
        props.setProperty('javax.jdo.option.defaultTransactionIsolation','READ_COMMITTED')
        props.setProperty('javax.jdo.option.defaultAutoCommit', 'false');
        props.setProperty('javax.persistence.schema-generation.database.action', 'drop-and-create')

        URL url = Thread.currentThread().getContextClassLoader().getResource("s3.properties")
        Path filePath
        if( url != null) {
            filePath = Paths.get(url.toURI());
        } else {
            File metadataFile = new File('src/test/resources/s3.properties')
            if( !metadataFile.exists()){
                metadataFile = new File('metacat-s3-connector/src/test/resources/s3.properties')
            }
            filePath = Paths.get(metadataFile.getPath())
        }
        props.store(Files.newOutputStream(filePath), "test")
        new JpaPersistModule("s3").properties(props).configure(binder)
        binder.bind(TestingMySqlServer.class).toInstance(mysqlServer)

        binder.bind(ConfigurationFactory.class).toInstance(new ConfigurationFactory(Maps.newHashMap()))
        HiveClientModule hiveClientModule = new HiveClientModule("s3", null, new TypeRegistry())
        Module module = Modules.override(hiveClientModule).with(new S3Module());
        module.configure(binder)
    }
}
