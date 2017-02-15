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
import com.google.common.base.Strings;
import com.google.inject.persist.Transactional;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.server.exception.DatabaseNotFoundException;
import com.netflix.metacat.common.server.exception.TableAlreadyExistsException;
import com.netflix.metacat.common.server.exception.TableNotFoundException;
import com.netflix.metacat.connector.s3.dao.DatabaseDao;
import com.netflix.metacat.connector.s3.dao.FieldDao;
import com.netflix.metacat.connector.s3.dao.TableDao;
import com.netflix.metacat.connector.s3.model.Database;
import com.netflix.metacat.connector.s3.model.Field;
import com.netflix.metacat.connector.s3.model.Info;
import com.netflix.metacat.connector.s3.model.Location;
import com.netflix.metacat.connector.s3.model.Schema;
import com.netflix.metacat.connector.s3.model.Table;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * S3 Connector implementation for tables.
 *
 * @author amajumdar
 */
@Transactional
@Slf4j
public class S3ConnectorTableService implements ConnectorTableService {
    private final DatabaseDao databaseDao;
    private final TableDao tableDao;
    private final FieldDao fieldDao;
    private final S3ConnectorInfoConverter infoConverter;
    private final String catalogName;

    /**
     * Constructor.
     * @param catalogName catalog name
     * @param databaseDao database DAO impl
     * @param tableDao table DAO impl
     * @param fieldDao field DAO impl
     * @param infoConverter Converter for the S3 resources
     */
    @Inject
    public S3ConnectorTableService(@Named("catalogName") final String catalogName, final DatabaseDao databaseDao,
        final TableDao tableDao, final FieldDao fieldDao, final S3ConnectorInfoConverter infoConverter) {
        this.databaseDao = databaseDao;
        this.tableDao = tableDao;
        this.fieldDao = fieldDao;
        this.infoConverter = infoConverter;
        this.catalogName = catalogName;
    }
    @Override
    public void create(@Nonnull final ConnectorContext context, @Nonnull final TableInfo tableInfo) {
        log.debug("Start: Create table {}", tableInfo.getName());
        Preconditions.checkArgument(tableInfo.getSerde() == null
            || !Strings.isNullOrEmpty(tableInfo.getSerde().getOwner()), "Table owner is null or empty");

        final QualifiedName tableName = tableInfo.getName();
        if (tableDao.getBySourceDatabaseTableName(catalogName, tableName.getDatabaseName(),
            tableName.getTableName()) != null) {
            throw new TableAlreadyExistsException(tableName);
        }
        final Database database = databaseDao
            .getBySourceDatabaseName(catalogName, tableName.getDatabaseName());
        if (database == null) {
            throw new DatabaseNotFoundException(QualifiedName.ofDatabase(catalogName, tableName.getDatabaseName()));
        }
        tableDao.save(infoConverter.fromTableInfo(database, tableInfo));
        log.debug("End: Create table {}", tableInfo.getName());
    }

    @Override
    public void update(@Nonnull final ConnectorContext context, @Nonnull final TableInfo tableInfo) {
        log.debug("Start: Update table {}", tableInfo.getName());
        final QualifiedName tableName = tableInfo.getName();
        final Table table = tableDao
            .getBySourceDatabaseTableName(catalogName, tableName.getDatabaseName(), tableName.getTableName());
        if (table == null) {
            throw new TableNotFoundException(tableName);
        }
        //we can update the fields, the uri, or the full serde
        final Location newLocation = infoConverter.toLocation(tableInfo);
        Location location = table.getLocation();
        if (location == null) {
            location = new Location();
            location.setTable(table);
            table.setLocation(location);
        }
        if (newLocation.getUri() != null) {
            location.setUri(newLocation.getUri());
        }
        final Info newInfo = newLocation.getInfo();
        if (newInfo != null) {
            final Info info = location.getInfo();
            if (info == null) {
                location.setInfo(newInfo);
                newInfo.setLocation(location);
            } else {
                if (newInfo.getInputFormat() != null) {
                    info.setInputFormat(newInfo.getInputFormat());
                }
                if (newInfo.getOutputFormat() != null) {
                    info.setOutputFormat(newInfo.getOutputFormat());
                }
                if (newInfo.getOwner() != null) {
                    info.setOwner(newInfo.getOwner());
                }
                if (newInfo.getSerializationLib() != null) {
                    info.setSerializationLib(newInfo.getSerializationLib());
                }
                if (newInfo.getParameters() != null && !newInfo.getParameters().isEmpty()) {
                    info.setParameters(newInfo.getParameters());
                }
            }
        }
        final Schema newSchema = newLocation.getSchema();
        if (newSchema != null) {
            final List<Field> newFields = newSchema.getFields();
            if (newFields != null && !newFields.isEmpty()) {
                final Schema schema = location.getSchema();
                if (schema == null) {
                    location.setSchema(newSchema);
                    newSchema.setLocation(location);
                } else {
                    final List<Field> fields = schema.getFields();
                    if (fields.isEmpty()) {
                        newFields.forEach(field -> {
                            field.setSchema(schema);
                            fields.add(field);
                        });
                    } else {
                        for (int i = 0; i < newFields.size(); i++) {
                            final Field newField = newFields.get(i);
                            newField.setPos(i);
                            newField.setSchema(schema);
                            if (newField.getType() == null) {
                                newField.setType(newField.getSourceType());
                            }
                        }
                        schema.setFields(null);
                        fieldDao.delete(fields);
                        tableDao.save(table, true);
                        schema.setFields(newFields);
                    }
                }
            }
        }
        log.debug("End: Update table {}", tableInfo.getName());
    }

    @Override
    public void delete(@Nonnull final ConnectorContext context, @Nonnull final QualifiedName name) {
        log.debug("Start: Delete table {}", name);
        final Table table = tableDao.getBySourceDatabaseTableName(catalogName,
            name.getDatabaseName(), name.getTableName());
        if (table == null) {
            throw new TableNotFoundException(name);
        }
        tableDao.delete(table);
        log.debug("End: Delete table {}", name);
    }

    @Override
    public TableInfo get(@Nonnull final ConnectorContext context, @Nonnull final QualifiedName name) {
        final Table table = tableDao.getBySourceDatabaseTableName(catalogName,
            name.getDatabaseName(), name.getTableName());
        if (table == null) {
            throw new TableNotFoundException(name);
        }
        log.debug("Get table {}", name);
        return infoConverter.toTableInfo(name, table);
    }

    @Override
    public boolean exists(@Nonnull final ConnectorContext context, @Nonnull final QualifiedName name) {
        return tableDao.getBySourceDatabaseTableName(catalogName, name.getDatabaseName(), name.getTableName()) != null;
    }

    @Override
    public List<TableInfo> list(@Nonnull final ConnectorContext context, @Nonnull final QualifiedName name,
        @Nullable final QualifiedName prefix, @Nullable final Sort sort, @Nullable final Pageable pageable) {
        log.debug("List tables for database {} with table name prefix {}", name, prefix);
        return tableDao.searchBySourceDatabaseTableName(catalogName, name.getDatabaseName(),
            prefix == null ? null : prefix.getTableName(), sort, pageable).stream()
            .map(t -> infoConverter.toTableInfo(QualifiedName.ofTable(catalogName, name.getDatabaseName(), t.getName()),
                t)).collect(Collectors.toList());
    }

    @Override
    public List<QualifiedName> listNames(@Nonnull final ConnectorContext context, @Nonnull final QualifiedName name,
        @Nullable final QualifiedName prefix, @Nullable final Sort sort, @Nullable final Pageable pageable) {
        log.debug("List table names for database {} with table name prefix {}", name, prefix);
        return tableDao.searchBySourceDatabaseTableName(catalogName, name.getDatabaseName(),
            prefix == null ? null : prefix.getTableName(), sort, pageable).stream()
            .map(t -> QualifiedName.ofTable(catalogName, name.getDatabaseName(), t.getName()))
            .collect(Collectors.toList());
    }

    @Override
    public void rename(@Nonnull final ConnectorContext context, @Nonnull final QualifiedName oldName,
        @Nonnull final QualifiedName newName) {
        log.debug("Start: Rename table {} with {}", oldName, newName);
        final Table oldTable = tableDao.getBySourceDatabaseTableName(catalogName,
            oldName.getDatabaseName(), oldName.getTableName());
        if (oldTable == null) {
            throw new TableNotFoundException(oldName);
        }
        final Table newTable = tableDao.getBySourceDatabaseTableName(catalogName,
            newName.getDatabaseName(), newName.getTableName());
        if (newTable == null) {
            oldTable.setName(newName.getTableName());
            tableDao.save(oldTable);
        } else {
            throw new TableAlreadyExistsException(newName);
        }
        log.debug("End: Rename table {} with {}", oldName, newName);
    }

    @Override
    public Map<String, List<QualifiedName>> getTableNames(@Nonnull final ConnectorContext context,
        @Nonnull final List<String> uris, final boolean prefixSearch) {
        return tableDao.getByUris(catalogName, uris, prefixSearch);
    }
}
