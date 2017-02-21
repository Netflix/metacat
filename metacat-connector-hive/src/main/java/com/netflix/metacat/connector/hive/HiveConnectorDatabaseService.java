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
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.exception.MetacatNotSupportedException;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo;
import com.netflix.metacat.common.server.exception.ConnectorException;
import com.netflix.metacat.common.server.exception.DatabaseAlreadyExistsException;
import com.netflix.metacat.common.server.exception.DatabaseNotFoundException;
import com.netflix.metacat.common.server.exception.InvalidMetaException;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;

/**
 * HiveConnectorDatabaseService.
 *
 * @author zhenl
 */
public class HiveConnectorDatabaseService implements ConnectorDatabaseService {
    private final MetacatHiveClient metacatHiveClient;
    private final HiveConnectorInfoConverter hiveMetacatConverters;
    private final String catalogName;

    /**
     * Constructor.
     *
     * @param catalogName           catalogname
     * @param metacatHiveClient     hiveclient
     * @param hiveMetacatConverters hive converter
     */
    @Inject
    public HiveConnectorDatabaseService(@Named("catalogName") final String catalogName,
                                        @Nonnull final MetacatHiveClient metacatHiveClient,
                                        @Nonnull final HiveConnectorInfoConverter hiveMetacatConverters) {
        this.metacatHiveClient = metacatHiveClient;
        this.hiveMetacatConverters = hiveMetacatConverters;
        this.catalogName = catalogName;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void create(@Nonnull final ConnectorContext requestContext, @Nonnull final DatabaseInfo databaseInfo) {
        try {
            this.metacatHiveClient.createDatabase(hiveMetacatConverters.fromDatabaseInfo(databaseInfo));
        } catch (AlreadyExistsException exception) {
            throw new DatabaseAlreadyExistsException(databaseInfo.getName(), exception);
        } catch (MetaException exception) {
            throw new InvalidMetaException(databaseInfo.getName(), exception);
        } catch (TException exception) {
            throw new ConnectorException(databaseInfo.getName().toString(), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void delete(@Nonnull final ConnectorContext requestContext, @Nonnull final QualifiedName name) {
        try {
            this.metacatHiveClient.dropDatabase(name.getDatabaseName());
        } catch (NoSuchObjectException exception) {
            throw new DatabaseNotFoundException(name, exception);
        } catch (MetaException exception) {
            throw new InvalidMetaException(name, exception);
        } catch (InvalidOperationException exception) {
            throw new MetacatNotSupportedException(exception.getMessage());
        } catch (TException exception) {
            throw new ConnectorException(name.toString(), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public DatabaseInfo get(@Nonnull final ConnectorContext requestContext, @Nonnull final QualifiedName name) {
        try {
            final Database database = metacatHiveClient.getDatabase(name.getDatabaseName());
            return hiveMetacatConverters.toDatabaseInfo(name, database);
        } catch (NoSuchObjectException exception) {
            throw new DatabaseNotFoundException(name, exception);
        } catch (MetaException exception) {
            throw new InvalidMetaException(name, exception);
        } catch (TException exception) {
            throw new ConnectorException(name.toString(), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public boolean exists(@Nonnull final ConnectorContext requestContext, @Nonnull final QualifiedName name) {
        try {
            final Database database = this.metacatHiveClient.getDatabase(name.getDatabaseName());
            return database != null;
        } catch (TException exception) {
        }
        return false;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<QualifiedName> listNames(
        @Nonnull final ConnectorContext requestContext,
        @Nonnull final QualifiedName name,
        @Nullable final QualifiedName prefix,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {
        try {
            List<QualifiedName> qualifiedNames = Lists.newArrayList();
            final String databaseFilter = (prefix != null) ? prefix.getDatabaseName() : null;
            for (String databaseName : metacatHiveClient.getAllDatabases()) {
                final QualifiedName qualifiedName = QualifiedName.ofDatabase(name.getCatalogName(), databaseName);
                if (databaseFilter != null && !databaseName.startsWith(databaseFilter)) {
                    continue;
                }
                qualifiedNames.add(qualifiedName);
            }
            if (null != pageable && pageable.isPageable()) {
                final int limit = Math.min(pageable.getOffset() + pageable.getLimit(), qualifiedNames.size());
                qualifiedNames = (pageable.getOffset() > limit) ? Lists.newArrayList()
                    : qualifiedNames.subList(pageable.getOffset(), limit);
            }
            return qualifiedNames;
        } catch (MetaException exception) {
            throw new InvalidMetaException(name, exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<DatabaseInfo> list(
        @Nonnull final ConnectorContext requestContext,
        @Nonnull final QualifiedName name,
        @Nullable final QualifiedName prefix,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {

        try {
            List<DatabaseInfo> databaseInfos = Lists.newArrayList();
            for (String databaseName : metacatHiveClient.getAllDatabases()) {
                final QualifiedName qualifiedName = QualifiedName.ofDatabase(name.getCatalogName(), databaseName);
                if (!qualifiedName.toString().startsWith(prefix.toString())) {
                    continue;
                }
                databaseInfos.add(DatabaseInfo.builder().name(qualifiedName).build());
            }
            if (null != pageable && pageable.isPageable()) {
                final int limit = Math.min(pageable.getOffset() + pageable.getLimit(), databaseInfos.size());
                databaseInfos = (pageable.getOffset() > limit) ? Lists.newArrayList()
                    : databaseInfos.subList(pageable.getOffset(), limit);
            }
            return databaseInfos;
        } catch (MetaException exception) {
            throw new InvalidMetaException(name, exception);
        }
    }
}
