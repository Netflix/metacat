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
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.ConnectorUtils;
import com.netflix.metacat.common.server.connectors.exception.ConnectorException;
import com.netflix.metacat.common.server.connectors.exception.DatabaseAlreadyExistsException;
import com.netflix.metacat.common.server.connectors.exception.DatabaseNotFoundException;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException;
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TException;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;

/**
 * HiveConnectorDatabaseService.
 *
 * @author zhenl
 * @since 1.0.0
 */
public class HiveConnectorDatabaseService implements ConnectorDatabaseService {
    private final IMetacatHiveClient metacatHiveClient;
    private final HiveConnectorInfoConverter hiveMetacatConverters;

    /**
     * Constructor.
     *
     * @param metacatHiveClient     hive client
     * @param hiveMetacatConverters hive converter
     */
    public HiveConnectorDatabaseService(
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverters
    ) {
        this.metacatHiveClient = metacatHiveClient;
        this.hiveMetacatConverters = hiveMetacatConverters;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void create(final ConnectorRequestContext requestContext, final DatabaseInfo databaseInfo) {
        final QualifiedName databaseName = databaseInfo.getName();
        try {
            this.metacatHiveClient.createDatabase(hiveMetacatConverters.fromDatabaseInfo(databaseInfo));
        } catch (AlreadyExistsException exception) {
            throw new DatabaseAlreadyExistsException(databaseName, exception);
        } catch (MetaException | InvalidObjectException exception) {
            throw new InvalidMetaException(databaseName, exception);
        } catch (TException exception) {
            throw new ConnectorException(
                String.format("Failed creating hive database %s", databaseName), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void delete(final ConnectorRequestContext requestContext, final QualifiedName name) {
        try {
            this.metacatHiveClient.dropDatabase(name.getDatabaseName());
        } catch (NoSuchObjectException exception) {
            throw new DatabaseNotFoundException(name, exception);
        } catch (MetaException exception) {
            throw new InvalidMetaException(name, exception);
        } catch (InvalidOperationException exception) {
            throw new MetacatNotSupportedException(exception.getMessage());
        } catch (TException exception) {
            throw new ConnectorException(String.format("Failed delete hive database %s", name), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void update(final ConnectorRequestContext context, final DatabaseInfo databaseInfo) {
        final QualifiedName databaseName = databaseInfo.getName();
        try {
            this.metacatHiveClient.alterDatabase(databaseName.getDatabaseName(),
                hiveMetacatConverters.fromDatabaseInfo(databaseInfo));
        } catch (NoSuchObjectException exception) {
            throw new DatabaseNotFoundException(databaseName, exception);
        } catch (MetaException | InvalidObjectException exception) {
            throw new InvalidMetaException(databaseName, exception);
        } catch (TException exception) {
            throw new ConnectorException(
                String.format("Failed updating hive database %s", databaseName), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public DatabaseInfo get(final ConnectorRequestContext requestContext, final QualifiedName name) {
        try {
            final Database database = metacatHiveClient.getDatabase(name.getDatabaseName());
            if (database != null) {
                return hiveMetacatConverters.toDatabaseInfo(name, database);
            } else {
                throw new DatabaseNotFoundException(name);
            }
        } catch (NoSuchObjectException exception) {
            throw new DatabaseNotFoundException(name, exception);
        } catch (MetaException exception) {
            throw new InvalidMetaException(name, exception);
        } catch (TException exception) {
            throw new ConnectorException(String.format("Failed get hive database %s", name), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public boolean exists(final ConnectorRequestContext requestContext, final QualifiedName name) {
        boolean result;
        try {
            result = metacatHiveClient.getDatabase(name.getDatabaseName()) != null;
        } catch (NoSuchObjectException exception) {
            result = false;
        } catch (TException exception) {
            throw new ConnectorException(String.format("Failed to check hive database %s exists", name), exception);
        }
        return result;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<QualifiedName> listNames(
        final ConnectorRequestContext requestContext,
        final QualifiedName name,
        @Nullable final QualifiedName prefix,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {
        try {
            final List<QualifiedName> qualifiedNames = Lists.newArrayList();
            final String databaseFilter = (prefix != null) ? prefix.getDatabaseName() : null;
            for (String databaseName : metacatHiveClient.getAllDatabases()) {
                final QualifiedName qualifiedName = QualifiedName.ofDatabase(name.getCatalogName(), databaseName);
                if (databaseFilter != null && !databaseName.startsWith(databaseFilter)) {
                    continue;
                }
                qualifiedNames.add(qualifiedName);
            }
            //supporting sort by qualified name only
            if (sort != null) {
                ConnectorUtils.sort(qualifiedNames, sort, Comparator.comparing(QualifiedName::toString));
            }
            return ConnectorUtils.paginate(qualifiedNames, pageable);
        } catch (MetaException exception) {
            throw new InvalidMetaException(name, exception);
        } catch (TException exception) {
            throw new ConnectorException(String.format("Failed listName hive database %s", name), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<DatabaseInfo> list(
        final ConnectorRequestContext requestContext,
        final QualifiedName name,
        @Nullable final QualifiedName prefix,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {

        try {
            final List<DatabaseInfo> databaseInfos = Lists.newArrayList();
            for (String databaseName : metacatHiveClient.getAllDatabases()) {
                final QualifiedName qualifiedName = QualifiedName.ofDatabase(name.getCatalogName(), databaseName);
                if (prefix != null && !qualifiedName.toString().startsWith(prefix.toString())) {
                    continue;
                }
                databaseInfos.add(DatabaseInfo.builder().name(qualifiedName).build());
            }
            //supporting sort by name only
            if (sort != null) {
                ConnectorUtils.sort(databaseInfos, sort, Comparator.comparing(p -> p.getName().getDatabaseName()));
            }
            return ConnectorUtils.paginate(databaseInfos, pageable);
        } catch (MetaException exception) {
            throw new InvalidMetaException(name, exception);
        } catch (TException exception) {
            throw new ConnectorException(String.format("Failed list hive database %s", name), exception);
        }
    }
}
