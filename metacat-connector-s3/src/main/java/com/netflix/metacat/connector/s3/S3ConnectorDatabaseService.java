/*
 *
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
 *
 */
package com.netflix.metacat.connector.s3;

import com.google.common.base.Preconditions;
import com.google.inject.persist.Transactional;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo;
import com.netflix.metacat.common.server.exception.ConnectorException;
import com.netflix.metacat.common.server.exception.DatabaseAlreadyExistsException;
import com.netflix.metacat.common.server.exception.DatabaseNotFoundException;
import com.netflix.metacat.connector.s3.dao.DatabaseDao;
import com.netflix.metacat.connector.s3.dao.SourceDao;
import com.netflix.metacat.connector.s3.model.Database;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.stream.Collectors;

/**
 * S3 Connector Database Service implementation.
 * @author amajumdar
 */
@Transactional
public class S3ConnectorDatabaseService implements ConnectorDatabaseService {
    private final SourceDao sourceDao;
    private final DatabaseDao databaseDao;
    private final S3ConnectorInfoConverter infoConverter;
    private final String catalogName;

    /**
     * Constructor.
     * @param catalogName catalog name
     * @param databaseDao database DAO impl
     * @param sourceDao catalog/source DAO impl
     * @param infoConverter Converter for the S3 resources
     */
    @Inject
    public S3ConnectorDatabaseService(@Named("catalogName") final String catalogName, final DatabaseDao databaseDao,
        final SourceDao sourceDao, final S3ConnectorInfoConverter infoConverter) {
        this.databaseDao = databaseDao;
        this.sourceDao = sourceDao;
        this.infoConverter = infoConverter;
        this.catalogName = catalogName;
    }

    @Override
    public List<QualifiedName> listViewNames(@Nonnull final ConnectorContext context,
        @Nonnull final QualifiedName databaseName) {
        return null;
    }

    @Override
    public void create(@Nonnull final ConnectorContext context, @Nonnull final DatabaseInfo databaseInfo) {
        final String databaseName = databaseInfo.getName().getDatabaseName();
        Preconditions.checkNotNull(databaseName, "Schema name is null");
        if (databaseDao.getBySourceDatabaseName(catalogName, databaseName) != null) {
            throw new DatabaseAlreadyExistsException(databaseInfo.getName());
        }
        final Database database = new Database();
        database.setName(databaseName);
        database.setSource(sourceDao.getByName(catalogName));
        databaseDao.save(database);
    }

    @Override
    public void update(@Nonnull final ConnectorContext context, @Nonnull final DatabaseInfo databaseInfo) {
        // no op
    }

    @Override
    public void delete(@Nonnull final ConnectorContext context, @Nonnull final QualifiedName name) {
        final String databaseName = name.getDatabaseName();
        Preconditions.checkNotNull(databaseName, "Schema name is null");
        final Database database = databaseDao.getByName(databaseName);
        if (database == null) {
            throw new DatabaseNotFoundException(name);
        } else if (database.getTables() != null && !database.getTables().isEmpty()) {
            throw new ConnectorException("Database " + databaseName + " is not empty. One or more tables exist.", null);
        }
        databaseDao.delete(database);
    }

    @Override
    public DatabaseInfo get(@Nonnull final ConnectorContext context, @Nonnull final QualifiedName name) {
        final String databaseName = name.getDatabaseName();
        Preconditions.checkNotNull(databaseName, "Schema name is null");
        final Database database = databaseDao.getBySourceDatabaseName(catalogName, name.getDatabaseName());
        return infoConverter.toDatabaseInfo(QualifiedName.ofCatalog(catalogName), database);
    }

    @Override
    public boolean exists(@Nonnull final ConnectorContext context, @Nonnull final QualifiedName name) {
        return databaseDao.getByName(name.getDatabaseName()) != null;
    }

    @Override
    public List<DatabaseInfo> list(@Nonnull final ConnectorContext context, @Nonnull final QualifiedName name,
        @Nullable final QualifiedName prefix, @Nullable final Sort sort, @Nullable final Pageable pageable) {
        return databaseDao.searchBySourceDatabaseName(catalogName, prefix == null ? "" : prefix.getTableName(),
            sort, pageable).stream().map(d -> infoConverter.toDatabaseInfo(name, d)).collect(
            Collectors.toList());
    }

    @Override
    public List<QualifiedName> listNames(@Nonnull final ConnectorContext context, @Nonnull final QualifiedName name,
        @Nullable final QualifiedName prefix, @Nullable final Sort sort, @Nullable final Pageable pageable) {
        return databaseDao.searchBySourceDatabaseName(catalogName, prefix == null ? "" : prefix.getTableName(),
            sort, pageable).stream().map(d -> QualifiedName.ofDatabase(catalogName, d.getName())).collect(
            Collectors.toList());
    }

    @Override
    public void rename(@Nonnull final ConnectorContext context, @Nonnull final QualifiedName oldName,
        @Nonnull final QualifiedName newName) {
        final String newDatabaseName = newName.getDatabaseName();
        Preconditions.checkNotNull(newDatabaseName, "Schema name is null");
        final Database oldDatabase = databaseDao.getBySourceDatabaseName(catalogName, oldName.getDatabaseName());
        if (oldDatabase == null) {
            throw new DatabaseNotFoundException(oldName);
        }
        if (databaseDao.getBySourceDatabaseName(catalogName, newDatabaseName) != null) {
            throw new DatabaseAlreadyExistsException(newName);
        }
        oldDatabase.setName(newDatabaseName);
        databaseDao.save(oldDatabase);
    }
}
