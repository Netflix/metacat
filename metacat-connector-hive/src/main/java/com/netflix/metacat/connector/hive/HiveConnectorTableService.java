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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.exception.MetacatBadRequestException;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.connectors.ConnectorUtils;
import com.netflix.metacat.common.server.connectors.exception.ConnectorException;
import com.netflix.metacat.common.server.connectors.exception.DatabaseNotFoundException;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException;
import com.netflix.metacat.common.server.connectors.exception.TableAlreadyExistsException;
import com.netflix.metacat.common.server.connectors.exception.TableNotFoundException;
import com.netflix.metacat.common.server.connectors.model.FieldInfo;
import com.netflix.metacat.common.server.connectors.model.StorageInfo;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.util.HiveConfigConstants;
import com.netflix.metacat.connector.hive.util.HiveTableUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Hive base connector base service impl.
 *
 * @author zhenl]
 * @since 1.0.0
 */
@Getter
@Slf4j
public class HiveConnectorTableService implements ConnectorTableService {
    private static final String PARAMETER_EXTERNAL = "EXTERNAL";
    protected final HiveConnectorInfoConverter hiveMetacatConverters;
    protected final ConnectorContext connectorContext;
    private final String catalogName;
    private final IMetacatHiveClient metacatHiveClient;
    private final HiveConnectorDatabaseService hiveConnectorDatabaseService;
    private final boolean allowRenameTable;
    private final boolean onRenameConvertToExternal;

    /**
     * Constructor.
     *
     * @param catalogName                  catalog name
     * @param metacatHiveClient            hive client
     * @param hiveConnectorDatabaseService hive database service
     * @param hiveMetacatConverters        converter
     * @param connectorContext             the connector context
     */
    public HiveConnectorTableService(
        final String catalogName,
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorDatabaseService hiveConnectorDatabaseService,
        final HiveConnectorInfoConverter hiveMetacatConverters,
        final ConnectorContext connectorContext
    ) {
        this.metacatHiveClient = metacatHiveClient;
        this.hiveMetacatConverters = hiveMetacatConverters;
        this.hiveConnectorDatabaseService = hiveConnectorDatabaseService;
        this.catalogName = catalogName;
        this.allowRenameTable = Boolean.parseBoolean(
            connectorContext.getConfiguration().getOrDefault(HiveConfigConstants.ALLOW_RENAME_TABLE, "false")
        );
        this.onRenameConvertToExternal = Boolean.parseBoolean(
            connectorContext.getConfiguration().getOrDefault(HiveConfigConstants.ON_RENAME_CONVERT_TO_EXTERNAL,
                "true")
        );
        this.connectorContext = connectorContext;
    }

    /**
     * getTable.
     *
     * @param requestContext The request context
     * @param name           The qualified name of the resource to get
     * @return table dto
     */
    @Override
    public TableInfo get(final ConnectorRequestContext requestContext, final QualifiedName name) {
        try {
            final Table table = metacatHiveClient.getTableByName(name.getDatabaseName(), name.getTableName());
            return hiveMetacatConverters.toTableInfo(name, table);
        } catch (NoSuchObjectException exception) {
            throw new TableNotFoundException(name, exception);
        } catch (MetaException exception) {
            throw new InvalidMetaException(name, exception);
        } catch (TException exception) {
            throw new ConnectorException(String.format("Failed get hive table %s", name), exception);
        }
    }

    /**
     * Create a table.
     *
     * @param requestContext The request context
     * @param tableInfo      The resource metadata
     */
    @Override
    public void create(final ConnectorRequestContext requestContext, final TableInfo tableInfo) {
        final QualifiedName tableName = tableInfo.getName();
        try {
            final Table table = hiveMetacatConverters.fromTableInfo(tableInfo);
            updateTable(requestContext, table, tableInfo);
            metacatHiveClient.createTable(table);
        } catch (AlreadyExistsException exception) {
            throw new TableAlreadyExistsException(tableName, exception);
        } catch (MetaException | InvalidObjectException exception) {
            //the NoSuchObjectException is converted into InvalidObjectException in hive client
            if (exception.getMessage().startsWith(tableName.getDatabaseName())) {
                throw new DatabaseNotFoundException(
                    QualifiedName.ofDatabase(tableName.getCatalogName(),
                        tableName.getDatabaseName()), exception);
            } else {
                //table name or column invalid defintion exception
                throw new InvalidMetaException(tableName, exception);
            }
        } catch (TException exception) {
            throw new ConnectorException(String.format("Failed create hive table %s", tableName), exception);
        }
    }

    void updateTable(
        final ConnectorRequestContext requestContext,
        final Table table,
        final TableInfo tableInfo
    ) throws MetaException {
        if (table.getParameters() == null || table.getParameters().isEmpty()) {
            table.setParameters(Maps.newHashMap());
        }
        //if this a type of table, we all mark it external table
        //otherwise leaves it as such as VIRTUAL_VIEW
        if (!isVirtualView(table)) {
            table.getParameters().putIfAbsent(PARAMETER_EXTERNAL, "TRUE");
        } else {
            validAndUpdateVirtualView(table);
        }

        if (tableInfo.getMetadata() != null) {
            table.getParameters().putAll(tableInfo.getMetadata());
        }
        //no other information is needed for iceberg table
        if (connectorContext.getConfig().isIcebergEnabled() && HiveTableUtil.isIcebergTable(tableInfo)) {
            table.setPartitionKeys(Collections.emptyList());
            log.debug("Skipping seder and set partition key to empty when updating iceberg table in hive");
            return;
        }

        //storage
        final StorageDescriptor sd = table.getSd() != null ? table.getSd() : new StorageDescriptor();

        String inputFormat = null;
        String outputFormat = null;

        Map<String, String> sdParameters = Maps.newHashMap();
        final String location =
            tableInfo.getSerde() == null ? null : tableInfo.getSerde().getUri();
        if (location != null) {
            sd.setLocation(location);
        } else if (sd.getLocation() == null) {
            final String locationStr = hiveConnectorDatabaseService.get(requestContext,
                QualifiedName.ofDatabase(tableInfo.getName().
                    getCatalogName(), tableInfo.getName().getDatabaseName())).getUri();
            final Path databasePath = new Path(locationStr);
            final Path targetPath = new Path(databasePath, tableInfo.getName().getTableName());
            sd.setLocation(targetPath.toString());
        }
        if (sd.getSerdeInfo() == null) {
            sd.setSerdeInfo(new SerDeInfo());
        }
        final SerDeInfo serdeInfo = sd.getSerdeInfo();
        serdeInfo.setName(tableInfo.getName().getTableName());
        final StorageInfo storageInfo = tableInfo.getSerde();
        if (storageInfo != null) {
            if (!Strings.isNullOrEmpty(storageInfo.getSerializationLib())) {
                serdeInfo.setSerializationLib(storageInfo.getSerializationLib());
            }
            if (storageInfo.getSerdeInfoParameters() != null && !storageInfo.getSerdeInfoParameters().isEmpty()) {
                serdeInfo.setParameters(storageInfo.getSerdeInfoParameters());
            }
            inputFormat = storageInfo.getInputFormat();
            outputFormat = storageInfo.getOutputFormat();
            if (storageInfo.getParameters() != null && !storageInfo.getParameters().isEmpty()) {
                sdParameters = storageInfo.getParameters();
            }
        } else if (table.getSd() != null) {
            final HiveStorageFormat hiveStorageFormat = this.extractHiveStorageFormat(table);
            serdeInfo.setSerializationLib(hiveStorageFormat.getSerde());
            serdeInfo.setParameters(ImmutableMap.of());
            inputFormat = hiveStorageFormat.getInputFormat();
            outputFormat = hiveStorageFormat.getOutputFormat();
        }

        final ImmutableList.Builder<FieldSchema> columnsBuilder = ImmutableList.builder();
        final ImmutableList.Builder<FieldSchema> partitionKeysBuilder = ImmutableList.builder();
        if (tableInfo.getFields() != null) {
            for (FieldInfo column : tableInfo.getFields()) {
                final FieldSchema field = hiveMetacatConverters.metacatToHiveField(column);
                if (column.isPartitionKey()) {
                    partitionKeysBuilder.add(field);
                } else {
                    columnsBuilder.add(field);
                }
            }
        }
        final ImmutableList<FieldSchema> columns = columnsBuilder.build();
        if (!columns.isEmpty()) {
            sd.setCols(columns);
        }
        if (!Strings.isNullOrEmpty(inputFormat)) {
            sd.setInputFormat(inputFormat);
        }
        if (!Strings.isNullOrEmpty(outputFormat)) {
            sd.setOutputFormat(outputFormat);
        }
        if (sd.getParameters() == null) {
            sd.setParameters(sdParameters);
        }

        //partition keys
        final ImmutableList<FieldSchema> partitionKeys = partitionKeysBuilder.build();
        if (!partitionKeys.isEmpty()) {
            table.setPartitionKeys(partitionKeys);
        }
        table.setSd(sd);
    }

    private void validAndUpdateVirtualView(final Table table) {
        if (isVirtualView(table)
            && Strings.isNullOrEmpty(table.getViewOriginalText())) {
            throw new MetacatBadRequestException(
                String.format("Invalid view creation for %s/%s. Missing viewOrginialText",
                    table.getDbName(),
                    table.getDbName()));
        }

        if (Strings.isNullOrEmpty(table.getViewExpandedText())) {
            //set viewExpandedText to viewOriginalTest
            table.setViewExpandedText(table.getViewOriginalText());
        }
        //setting dummy string to view to avoid dropping view issue in hadoop Path org.apache.hadoop.fs
        if (Strings.isNullOrEmpty(table.getSd().getLocation())) {
            table.getSd().setLocation("file://tmp/" + table.getDbName() + "/" + table.getTableName());
        }
    }

    private boolean isVirtualView(final Table table) {
        return null != table.getTableType()
            && table.getTableType().equals(TableType.VIRTUAL_VIEW.toString());
    }

    /**
     * Delete a table with the given qualified name.
     *
     * @param requestContext The request context
     * @param name           The qualified name of the resource to delete
     */
    @Override
    public void delete(final ConnectorRequestContext requestContext, final QualifiedName name) {
        try {
            metacatHiveClient.dropTable(name.getDatabaseName(), name.getTableName());
        } catch (NoSuchObjectException exception) {
            throw new TableNotFoundException(name, exception);
        } catch (MetaException exception) {
            throw new InvalidMetaException(name, exception);
        } catch (TException exception) {
            throw new ConnectorException(String.format("Failed delete hive table %s", name), exception);
        }
    }

    /**
     * Update a resource with the given metadata.
     *
     * @param requestContext The request context
     * @param tableInfo      The resource metadata
     */
    @Override
    public void update(final ConnectorRequestContext requestContext, final TableInfo tableInfo) {
        final Table existingTable = hiveMetacatConverters.fromTableInfo(get(requestContext, tableInfo.getName()));
        update(requestContext, existingTable, tableInfo);
    }

    protected void update(final ConnectorRequestContext requestContext,
                          final Table existingTable, final TableInfo tableInfo) {
        final QualifiedName tableName = tableInfo.getName();
        try {
            updateTable(requestContext, existingTable, tableInfo);
            metacatHiveClient.alterTable(tableName.getDatabaseName(),
                tableName.getTableName(),
                existingTable);
        } catch (NoSuchObjectException exception) {
            throw new TableNotFoundException(tableName, exception);
        } catch (MetaException exception) {
            throw new InvalidMetaException(tableName, exception);
        } catch (TException exception) {
            throw new ConnectorException(String.format("Failed update hive table %s", tableName), exception);
        }
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

            final String tableFilter = (prefix != null && prefix.isTableDefinition()) ? prefix.getTableName() : null;
            for (String tableName : metacatHiveClient.getAllTables(name.getDatabaseName())) {
                if (tableFilter == null || tableName.startsWith(tableFilter)) {
                    final QualifiedName qualifiedName =
                        QualifiedName.ofTable(name.getCatalogName(), name.getDatabaseName(), tableName);
                    if (prefix != null && !qualifiedName.toString().startsWith(prefix.toString())) {
                        continue;
                    }
                    qualifiedNames.add(qualifiedName);
                }
            }
            ////supporting sort by qualified name only
            if (sort != null) {
                ConnectorUtils.sort(qualifiedNames, sort, Comparator.comparing(QualifiedName::toString));
            }
            return ConnectorUtils.paginate(qualifiedNames, pageable);
        } catch (MetaException exception) {
            throw new InvalidMetaException(name, exception);
        } catch (NoSuchObjectException exception) {
            throw new DatabaseNotFoundException(name, exception);
        } catch (TException exception) {
            throw new ConnectorException(String.format("Failed listNames hive table %s", name), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<TableInfo> list(
        final ConnectorRequestContext requestContext,
        final QualifiedName name,
        @Nullable final QualifiedName prefix,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {

        try {
            final List<TableInfo> tableInfos = Lists.newArrayList();
            for (String tableName : metacatHiveClient.getAllTables(name.getDatabaseName())) {
                final QualifiedName qualifiedName = QualifiedName.ofDatabase(name.getCatalogName(), tableName);
                if (prefix != null && !qualifiedName.toString().startsWith(prefix.toString())) {
                    continue;
                }
                final Table table = metacatHiveClient.getTableByName(name.getDatabaseName(), tableName);
                tableInfos.add(hiveMetacatConverters.toTableInfo(name, table));
            }
            //supporting sort by name only
            if (sort != null) {
                ConnectorUtils.sort(tableInfos, sort, Comparator.comparing(p -> p.getName().getTableName()));
            }
            return ConnectorUtils.paginate(tableInfos, pageable);
        } catch (MetaException exception) {
            throw new DatabaseNotFoundException(name, exception);
        } catch (TException exception) {
            throw new ConnectorException(String.format("Failed list hive table %s", name), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public boolean exists(final ConnectorRequestContext requestContext, final QualifiedName name) {
        boolean result;
        try {
            result = metacatHiveClient.getTableByName(name.getDatabaseName(), name.getTableName()) != null;
        } catch (NoSuchObjectException exception) {
            result = false;
        } catch (TException exception) {
            throw new ConnectorException(String.format("Failed exists hive table %s", name), exception);
        }
        return result;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void rename(
        final ConnectorRequestContext context,
        final QualifiedName oldName,
        final QualifiedName newName
    ) {
        if (!allowRenameTable) {
            throw new ConnectorException(
                "Renaming tables is disabled in catalog " + catalogName, null);
        }
        try {
            if (onRenameConvertToExternal) {
                //
                // If this is a managed table(EXTERNAL=FALSE), then convert it to an external table before renaming it.
                // We do not want the metastore to move the location/data.
                //
                final Table table = metacatHiveClient.getTableByName(oldName.getDatabaseName(), oldName.getTableName());
                Map<String, String> parameters = table.getParameters();
                if (parameters == null) {
                    parameters = Maps.newHashMap();
                    table.setParameters(parameters);
                }
                if (!parameters.containsKey(PARAMETER_EXTERNAL)
                    || parameters.get(PARAMETER_EXTERNAL).equalsIgnoreCase("FALSE")) {
                    parameters.put(PARAMETER_EXTERNAL, "TRUE");
                    metacatHiveClient.alterTable(oldName.getDatabaseName(), oldName.getTableName(), table);
                }
            }
            metacatHiveClient.rename(oldName.getDatabaseName(), oldName.getTableName(),
                newName.getDatabaseName(), newName.getTableName());
        } catch (NoSuchObjectException exception) {
            throw new TableNotFoundException(oldName, exception);
        } catch (MetaException exception) {
            throw new InvalidMetaException(newName, exception);
        } catch (TException exception) {
            throw new ConnectorException(
                "Failed renaming from hive table" + oldName.toString()
                    + " to hive talbe " + newName.toString(), exception);
        }
    }

    private HiveStorageFormat extractHiveStorageFormat(final Table table) throws MetaException {
        final StorageDescriptor descriptor = table.getSd();
        if (descriptor == null) {
            throw new MetaException("Table is missing storage descriptor");
        }
        final SerDeInfo serdeInfo = descriptor.getSerdeInfo();
        if (serdeInfo == null) {
            throw new MetaException(
                "Table storage descriptor is missing SerDe info");
        }
        final String outputFormat = descriptor.getOutputFormat();
        final String serializationLib = serdeInfo.getSerializationLib();

        for (HiveStorageFormat format : HiveStorageFormat.values()) {
            if (format.getOutputFormat().equals(outputFormat) && format.getSerde().equals(serializationLib)) {
                return format;
            }
        }
        throw new MetaException(
            String.format("Output format %s with SerDe %s is not supported", outputFormat, serializationLib));
    }

    @Override
    public List<QualifiedName> getTableNames(
        final ConnectorRequestContext context,
        final QualifiedName name,
        final String filter,
        @Nullable final Integer limit) {
        try {
            if (name.isDatabaseDefinition()) {
                return metacatHiveClient.getTableNames(name.getDatabaseName(), filter, limit == null ? -1 : limit)
                    .stream()
                    .map(n -> QualifiedName.ofTable(name.getCatalogName(), name.getDatabaseName(), n))
                    .collect(Collectors.toList());
            } else {
                int limitSize = limit == null || limit < 0 ? Integer.MAX_VALUE : limit;
                final List<String> databaseNames = metacatHiveClient.getAllDatabases();
                final List<QualifiedName> result = Lists.newArrayList();
                for (int i = 0; i < databaseNames.size() && limitSize > 0; i++) {
                    final String databaseName = databaseNames.get(i);
                    final List<String> tableNames =
                        metacatHiveClient.getTableNames(databaseName, filter, limitSize);
                    limitSize = limitSize - tableNames.size();
                    result.addAll(tableNames.stream()
                        .map(n -> QualifiedName.ofTable(name.getCatalogName(), databaseName, n))
                        .collect(Collectors.toList()));
                }
                return result;
            }
        } catch (TException e) {
            final String message = String.format("Failed getting the table names for database %s", name);
            log.error(message);
            throw new ConnectorException(message);
        }
    }
}
