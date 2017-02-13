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

package com.netflix.metacat.connector.s3;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorInfoConverter;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.connector.s3.dao.DatabaseDao;
import com.netflix.metacat.connector.s3.dao.FieldDao;
import com.netflix.metacat.connector.s3.dao.PartitionDao;
import com.netflix.metacat.connector.s3.dao.SourceDao;
import com.netflix.metacat.connector.s3.dao.TableDao;
import com.netflix.metacat.connector.s3.dao.impl.DatabaseDaoImpl;
import com.netflix.metacat.connector.s3.dao.impl.FieldDaoImpl;
import com.netflix.metacat.connector.s3.dao.impl.PartitionDaoImpl;
import com.netflix.metacat.connector.s3.dao.impl.SourceDaoImpl;
import com.netflix.metacat.connector.s3.dao.impl.TableDaoImpl;

import java.util.Map;

/**
 * Guice module.
 */
public class S3Module implements Module {
    private final String catalogName;
    private final Map<String, String> configuration;
    private final S3ConnectorInfoConverter infoConverter;

    /**
     * Constructor.
     * @param catalogName catalog name.
     * @param configuration configuration properties
     * @param infoConverter S3 info converter
     */
    public S3Module(final String catalogName, final Map<String, String> configuration,
        final S3ConnectorInfoConverter infoConverter) {
        this.catalogName = catalogName;
        this.configuration = configuration;
        this.infoConverter = infoConverter;
    }

    @Override
    public void configure(final Binder binder) {
        binder.bind(String.class).annotatedWith(Names.named("catalogName")).toInstance(catalogName);
        binder.bind(ConnectorInfoConverter.class).toInstance(infoConverter);
        binder.bind(S3ConnectorInfoConverter.class).toInstance(infoConverter);
        binder.bind(ConnectorDatabaseService.class).to(S3ConnectorDatabaseService.class);
        binder.bind(ConnectorTableService.class).to(S3ConnectorTableService.class);
        binder.bind(ConnectorPartitionService.class).to(S3ConnectorPartitionService.class);
        binder.bind(DatabaseDao.class).to(DatabaseDaoImpl.class);
        binder.bind(PartitionDao.class).to(PartitionDaoImpl.class);
        binder.bind(SourceDao.class).to(SourceDaoImpl.class);
        binder.bind(TableDao.class).to(TableDaoImpl.class);
        binder.bind(FieldDao.class).to(FieldDaoImpl.class);
    }
}
