/*
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.netflix.metacat.connector.hive;


import com.google.common.collect.Lists;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.exception.DatabaseAlreadyExistsException;
import com.netflix.metacat.common.server.exception.DatabaseNotFoundException;
import com.netflix.metacat.connector.hive.converters.HiveMetacatConverters;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.List;

/**
 * HiveConnectorDatabaseService.
 *
 * @author zhenl
 */
public class HiveConnectorDatabaseService implements ConnectorDatabaseService {
    private final MetacatHiveClient metacatHiveClient;
    private final HiveMetacatConverters hiveMetacatConverters;

    /**
     * Constructor.
     *
     * @param metacatHiveClient     hiveclient
     * @param hiveMetacatConverters hive converter
     */
    @Inject
    public HiveConnectorDatabaseService(@Nonnull final MetacatHiveClient metacatHiveClient,
                                        @Nonnull final HiveMetacatConverters hiveMetacatConverters) {
        this.metacatHiveClient = metacatHiveClient;
        this.hiveMetacatConverters = hiveMetacatConverters;
    }

    /**
     * Create a resource.
     *
     * @param requestContext The request context
     * @param databaseDto    The resource metadata
     */
    public void create(@Nonnull final MetacatRequestContext requestContext, @Nonnull final DatabaseDto databaseDto) {
        try {
            this.metacatHiveClient.createDatabase(hiveMetacatConverters.metacatToHiveDatabase(databaseDto));
        } catch (TException exception) {
            throw new DatabaseAlreadyExistsException(databaseDto.getName(), exception);
        }
    }

    /**
     * Delete a database with the given qualified name.
     *
     * @param requestContext The request context
     * @param name           The qualified name of the resource to delete
     */
    public void delete(@Nonnull final MetacatRequestContext requestContext, @Nonnull final QualifiedName name) {
        try {
            this.metacatHiveClient.dropDatabase(name.getDatabaseName());
        } catch (TException exception) {
            throw new DatabaseNotFoundException(name, exception);
        }
    }

    /**
     * Return a resource with the given name.
     *
     * @param requestContext The request context
     * @param name           The qualified name of the resource to get
     * @return The resource metadata.
     */
    public DatabaseDto get(@Nonnull final MetacatRequestContext requestContext, @Nonnull final QualifiedName name) {
        try {
            final Database database = this.metacatHiveClient.getDatabase(name.getDatabaseName());
            return hiveMetacatConverters.hiveToMetacatDatabase(name, database);
        } catch (TException exception) {
            throw new DatabaseNotFoundException(name, exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<QualifiedName> listNames(
        @Nonnull final MetacatRequestContext requestContext,
        @Nonnull final QualifiedName name,
        @Nullable final QualifiedName prefix,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {
        try {
            List<QualifiedName> qualifiedNames = Lists.newArrayList();
            for (String databaseName : metacatHiveClient.getAllDatabases()) {
                qualifiedNames.add(QualifiedName.ofDatabase(name.getCatalogName(), databaseName));
            }
            if (null != pageable && pageable.isPageable()) {
                final int limit = Math.min(pageable.getOffset() + pageable.getLimit(), qualifiedNames.size());
                qualifiedNames = (pageable.getOffset() > limit) ? Lists.newArrayList()
                    : qualifiedNames.subList(pageable.getOffset(), limit);
            }
            return  qualifiedNames;
        } catch (MetaException exception) {
            throw new DatabaseNotFoundException(name, exception);
        }
    }

}
