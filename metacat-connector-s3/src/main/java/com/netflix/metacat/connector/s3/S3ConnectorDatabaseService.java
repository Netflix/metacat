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
import com.google.common.collect.Lists;
import com.google.inject.persist.Transactional;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo;
import com.netflix.metacat.common.server.connectors.exception.ConnectorException;
import com.netflix.metacat.common.server.connectors.exception.DatabaseAlreadyExistsException;
import com.netflix.metacat.common.server.connectors.exception.DatabaseNotFoundException;
import com.netflix.metacat.connector.s3.dao.DatabaseDao;
import com.netflix.metacat.connector.s3.dao.SourceDao;
import com.netflix.metacat.connector.s3.model.Database;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.stream.Collectors;

/**
 * S3 Connector Database Service implementation.
 *
 * @author amajumdar
 */
@Transactional
@Slf4j
public class S3ConnectorDatabaseService implements ConnectorDatabaseService {
    private final SourceDao sourceDao;
    private final DatabaseDao databaseDao;
    private final S3ConnectorInfoConverter infoConverter;
    private final String catalogName;

    /**
     * Constructor.
     *
     * @param catalogName   catalog name
     * @param databaseDao   database DAO impl
     * @param sourceDao     catalog/source DAO impl
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
    public List<QualifiedName> listViewNames(@Nonnull final ConnectorRequestContext context,
                                             @Nonnull final QualifiedName databaseName) {
        return Lists.newArrayList();
    }

    @Override
    public void create(@Nonnull final ConnectorRequestContext context, @Nonnull final DatabaseInfo databaseInfo) {
        final String databaseName = databaseInfo.getName().getDatabaseName();
        log.debug("Start: Create database {}", databaseInfo.getName());
        Preconditions.checkNotNull(databaseName, "Database name is null");
        if (databaseDao.getBySourceDatabaseName(catalogName, databaseName) != null) {
            log.warn("Database {} already exists", databaseName);
            throw new DatabaseAlreadyExistsException(databaseInfo.getName());
        }
        final Database database = new Database();
        database.setName(databaseName);
        database.setSource(sourceDao.getByName(catalogName));
        databaseDao.save(database);
        log.debug("End: Create database {}", databaseInfo.getName());
    }

    @Override
    public void update(@Nonnull final ConnectorRequestContext context, @Nonnull final DatabaseInfo databaseInfo) {
        // no op
    }

    @Override
    public void delete(@Nonnull final ConnectorRequestContext context, @Nonnull final QualifiedName name) {
        log.debug("Start: Delete database {}", name);
        final String databaseName = name.getDatabaseName();
        Preconditions.checkNotNull(databaseName, "Database name is null");
        final Database database = databaseDao.getBySourceDatabaseName(catalogName, databaseName);
        if (database == null) {
            throw new DatabaseNotFoundException(name);
        } else if (database.getTables() != null && !database.getTables().isEmpty()) {
            throw new ConnectorException("Database " + databaseName + " is not empty. One or more tables exist.", null);
        }
        databaseDao.delete(database);
        log.debug("End: Delete database {}", name);
    }

    @Override
    public DatabaseInfo get(@Nonnull final ConnectorRequestContext context, @Nonnull final QualifiedName name) {
        final String databaseName = name.getDatabaseName();
        Preconditions.checkNotNull(databaseName, "Database name is null");
        log.debug("Get database {}", name);
        final Database database = databaseDao.getBySourceDatabaseName(catalogName, databaseName);
        if (database == null) {
            throw new DatabaseNotFoundException(name);
        }
        return infoConverter.toDatabaseInfo(QualifiedName.ofCatalog(catalogName), database);
    }

    @Override
    public boolean exists(@Nonnull final ConnectorRequestContext context, @Nonnull final QualifiedName name) {
        return databaseDao.getBySourceDatabaseName(catalogName, name.getDatabaseName()) != null;
    }

    @Override
    public List<DatabaseInfo> list(@Nonnull final ConnectorRequestContext context,
                                   @Nonnull final QualifiedName name,
                                   @Nullable final QualifiedName prefix,
                                   @Nullable final Sort sort,
                                   @Nullable final Pageable pageable) {
        log.debug("List databases for catalog {} and database with prefix {}", name, prefix);
        return databaseDao.searchBySourceDatabaseName(catalogName, prefix == null ? "" : prefix.getTableName(),
            sort, pageable).stream().map(d -> infoConverter.toDatabaseInfo(name, d)).collect(
            Collectors.toList());
    }

    @Override
    public List<QualifiedName> listNames(@Nonnull final ConnectorRequestContext context,
                                         @Nonnull final QualifiedName name,
                                         @Nullable final QualifiedName prefix,
                                         @Nullable final Sort sort,
                                         @Nullable final Pageable pageable) {
        log.debug("List database names for catalog {} and database with prefix {}", name, prefix);
        return databaseDao.searchBySourceDatabaseName(catalogName, prefix == null ? "" : prefix.getTableName(),
            sort, pageable).stream().map(d -> QualifiedName.ofDatabase(catalogName, d.getName())).collect(
            Collectors.toList());
    }

    @Override
    public void rename(@Nonnull final ConnectorRequestContext context, @Nonnull final QualifiedName oldName,
                       @Nonnull final QualifiedName newName) {
        log.debug("Start: Rename database {} with {}", oldName, newName);
        final String newDatabaseName = newName.getDatabaseName();
        Preconditions.checkNotNull(newDatabaseName, "Database name is null");
        final Database oldDatabase = databaseDao.getBySourceDatabaseName(catalogName, oldName.getDatabaseName());
        if (oldDatabase == null) {
            throw new DatabaseNotFoundException(oldName);
        }
        if (databaseDao.getBySourceDatabaseName(catalogName, newDatabaseName) != null) {
            throw new DatabaseAlreadyExistsException(newName);
        }
        oldDatabase.setName(newDatabaseName);
        databaseDao.save(oldDatabase);
        log.debug("End: Rename database {} with {}", oldName, newName);
    }
}
