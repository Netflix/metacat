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

import com.google.common.collect.Maps
import com.google.inject.Binder
import com.google.inject.Module
import com.google.inject.persist.jpa.JpaPersistModule
import com.google.inject.persist.jpa.JpaPersistOptions

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

/**
 * Created by amajumdar on 10/12/15.
 */
class S3TestModule implements Module{
    @Override
    void configure(Binder binder) {
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
        Properties props = new Properties()
        props.load(Files.newBufferedReader(filePath))
        final JpaPersistOptions options = JpaPersistOptions.builder()
            .setAutoBeginWorkOnEntityManagerCreation(true)
            .build();
        binder.install(new JpaPersistModule("s3", options).properties(Maps.newHashMap(props)))
        S3ConnectorPlugin plugin = new S3ConnectorPlugin();
        Module module = new S3Module("s3", null, (S3ConnectorInfoConverter)plugin.getInfoConverter());
        module.configure(binder)
    }
}
