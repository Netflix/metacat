package com.netflix.metacat.connector.polaris;

import com.google.common.collect.Lists;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.connectors.ConnectorUtils;
import com.netflix.metacat.common.server.connectors.exception.ConnectorException;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException;
import com.netflix.metacat.common.server.connectors.exception.TableAlreadyExistsException;
import com.netflix.metacat.common.server.connectors.exception.TableNotFoundException;
import com.netflix.metacat.common.server.connectors.exception.TablePreconditionFailedException;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableHandler;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableWrapper;
import com.netflix.metacat.connector.hive.sql.DirectSqlTable;
import com.netflix.metacat.connector.hive.util.HiveTableUtil;
import com.netflix.metacat.connector.polaris.mappers.PolarisTableMapper;
import com.netflix.metacat.connector.polaris.store.PolarisStoreService;
import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.dao.DataIntegrityViolationException;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * table service for polaris connector.
 */
@Slf4j
public class PolarisConnectorTableService implements ConnectorTableService {
    protected final PolarisStoreService polarisStoreService;
    protected final PolarisConnectorDatabaseService polarisConnectorDatabaseService;
    protected final HiveConnectorInfoConverter connectorConverter;
    protected final ConnectorContext connectorContext;
    protected final IcebergTableHandler icebergTableHandler;
    protected final PolarisTableMapper polarisTableMapper;
    protected final String catalogName;

    /**
     * Constructor.
     *
     * @param polarisStoreService               polaris connector
     * @param catalogName                       catalog name
     * @param polarisConnectorDatabaseService   connector database service
     * @param connectorConverter                converter
     * @param icebergTableHandler               iceberg table handler
     * @param polarisTableMapper                polaris table polarisTableMapper
     * @param connectorContext                  the connector context
     */
    public PolarisConnectorTableService(
        final PolarisStoreService polarisStoreService,
        final String catalogName,
        final PolarisConnectorDatabaseService polarisConnectorDatabaseService,
        final HiveConnectorInfoConverter connectorConverter,
        final IcebergTableHandler icebergTableHandler,
        final PolarisTableMapper polarisTableMapper,
        final ConnectorContext connectorContext
    ) {
        this.polarisStoreService = polarisStoreService;
        this.polarisConnectorDatabaseService = polarisConnectorDatabaseService;
        this.connectorConverter = connectorConverter;
        this.connectorContext = connectorContext;
        this.icebergTableHandler = icebergTableHandler;
        this.polarisTableMapper = polarisTableMapper;
        this.catalogName = catalogName;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void create(final ConnectorRequestContext requestContext, final TableInfo tableInfo) {
        final QualifiedName name = tableInfo.getName();
        // check exists then create in non-transactional optimistic manner
        if (exists(requestContext, name)) {
            throw new TableAlreadyExistsException(name);
        }
        try {
            final PolarisTableEntity entity = polarisTableMapper.toEntity(tableInfo);
            polarisStoreService.createTable(entity.getDbName(), entity.getTblName(), entity.getMetadataLocation());
        } catch (DataIntegrityViolationException | InvalidMetaException exception) {
            throw new InvalidMetaException(name, exception);
        } catch (Exception exception) {
            final String msg = String.format("Failed creating polaris table %s", name);
            log.error(msg, exception);
            throw new ConnectorException(msg, exception);
        }
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
        // check exists then rename in non-transactional optimistic manner
        if (exists(context, newName)) {
            throw new TableAlreadyExistsException(newName);
        }
        try {
            final PolarisTableEntity table = polarisStoreService
                .getTable(oldName.getDatabaseName(), oldName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(oldName));
            polarisStoreService.saveTable(table.toBuilder().tblName(newName.getTableName()).build());
        } catch (TableNotFoundException exception) {
            log.error(String.format("Not found exception for polaris table %s", oldName), exception);
            throw exception;
        } catch (DataIntegrityViolationException exception) {
            throw new InvalidMetaException(oldName, exception);
        } catch (Exception exception) {
            final String msg = String.format("Failed renaming polaris table %s", oldName);
            log.error(msg, exception);
            throw new ConnectorException(msg, exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public TableInfo get(final ConnectorRequestContext requestContext, final QualifiedName name) {
        try {
            final PolarisTableEntity polarisTableEntity = polarisStoreService
                .getTable(name.getDatabaseName(), name.getTableName())
                .orElseThrow(() -> new TableNotFoundException(name));
            final TableInfo info = polarisTableMapper.toInfo(polarisTableEntity);
            final String tableLoc = HiveTableUtil.getIcebergTableMetadataLocation(info);
            return getIcebergTable(name, tableLoc, info,
                requestContext.isIncludeMetadata(), connectorContext.getConfig().isIcebergCacheEnabled());
        } catch (TableNotFoundException | IllegalArgumentException exception) {
            log.error(String.format("Not found exception for polaris table %s", name), exception);
            throw exception;
        } catch (ConnectorException connectorException) {
            log.error("Encountered connector exception for polaris table {}. {}", name, connectorException);
            throw connectorException;
        } catch (Exception exception) {
            final String msg = String.format("Failed getting polaris table %s", name);
            log.error(msg, exception);
            throw exception;
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
            final String tableFilter = (prefix != null && prefix.isTableDefinition()) ? prefix.getTableName() : "";
            for (String tableName : polarisStoreService.getTables(name.getDatabaseName(), tableFilter)) {
                final QualifiedName qualifiedName =
                    QualifiedName.ofTable(name.getCatalogName(), name.getDatabaseName(), tableName);
                if (prefix != null && !qualifiedName.toString().startsWith(prefix.toString())) {
                    continue;
                }
                qualifiedNames.add(qualifiedName);
            }
            if (sort != null) {
                ConnectorUtils.sort(qualifiedNames, sort, Comparator.comparing(QualifiedName::toString));
            }
            return ConnectorUtils.paginate(qualifiedNames, pageable);
        } catch (Exception exception) {
            final String msg = String.format("Failed polaris list table names %s using prefix %s", name, prefix);
            log.error(msg, exception);
            throw new ConnectorException(msg, exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void update(final ConnectorRequestContext requestContext, final TableInfo tableInfo) {
        final QualifiedName name = tableInfo.getName();
        final Config conf = connectorContext.getConfig();
        try {
            final Map<String, String> newTableMetadata = tableInfo.getMetadata();
            if (MapUtils.isEmpty(newTableMetadata)) {
                final String message = String.format("No parameters defined for iceberg table %s", name);
                log.warn(message);
                throw new InvalidMetaException(name, message, null);
            }
            final String prevLoc = newTableMetadata.get(DirectSqlTable.PARAM_PREVIOUS_METADATA_LOCATION);
            final String newLoc = newTableMetadata.get(DirectSqlTable.PARAM_METADATA_LOCATION);
            if (StringUtils.isBlank(prevLoc) || StringUtils.isBlank(newLoc)) {
                final String message = String.format(
                        "Invalid metadata for %s. Provided previous %s or new %s location is empty.",
                        name, prevLoc, newLoc);
                log.error(message);
                throw new InvalidMetaException(name, message, null);
            }
            if (conf.isIcebergPreviousMetadataLocationCheckEnabled()
                    && !icebergTableHandler.doesMetadataLocationExist(name, prevLoc)) {
                final String message = String.format(
                        "Provided previous metadata location: %s for table: %s does not exist.",
                        name, prevLoc);
                log.error(message);
                throw new InvalidMetaException(name, message, null);
            }
            // optimistically attempt to update metadata location
            final boolean updated = polarisStoreService.updateTableMetadataLocation(
                    name.getDatabaseName(), name.getTableName(), prevLoc, newLoc);
            // if succeeded then done, else try to figure out why and throw corresponding exception
            if (updated) {
                requestContext.setIgnoreErrorsAfterUpdate(true);
                return;
            }
            final PolarisTableEntity table = polarisStoreService
                    .getTable(name.getDatabaseName(), name.getTableName())
                    .orElseThrow(() -> new TableNotFoundException(name));
            final String existingLoc = table.getMetadataLocation();
            if (StringUtils.isBlank(existingLoc)) {
                final String message = String.format(
                        "Invalid metadata location for %s existing location is empty.", name);
                log.error(message);
                throw new TablePreconditionFailedException(name, message, existingLoc, prevLoc);
            }
            if (StringUtils.equalsIgnoreCase(existingLoc, newLoc)) {
                log.warn("Existing metadata location is the same as new. Existing: {}, New: {}",
                        existingLoc, newLoc);
                return;
            }
            if (!Objects.equals(existingLoc, prevLoc)) {
                final String message = String.format(
                        "Invalid metadata location for %s expected: %s, provided: %s", name, existingLoc, prevLoc);
                log.error(message);
                throw new TablePreconditionFailedException(name, message, existingLoc, prevLoc);
            }
        } catch (TableNotFoundException | InvalidMetaException | TablePreconditionFailedException exception) {
            throw exception;
        } catch (DataIntegrityViolationException exception) {
            throw new InvalidMetaException(name, exception);
        } catch (Exception exception) {
            final String msg = String.format("Failed updating polaris table %s", tableInfo.getName());
            log.error(msg, exception);
            throw new ConnectorException(msg, exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public boolean exists(final ConnectorRequestContext requestContext, final QualifiedName name) {
        try {
            return polarisStoreService.tableExists(name.getDatabaseName(), name.getTableName());
        } catch (Exception exception) {
            final String msg = String.format("Failed exists polaris table %s", name);
            log.error(msg, exception);
            throw new ConnectorException(msg, exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void delete(final ConnectorRequestContext requestContext, final QualifiedName name) {
        // check exists then delete in non-transactional optimistic manner
        if (!exists(requestContext, name)) {
            throw new TableNotFoundException(name);
        }
        try {
            polarisStoreService.deleteTable(name.getDatabaseName(), name.getTableName());
        } catch (DataIntegrityViolationException exception) {
            throw new InvalidMetaException(name, exception);
        } catch (Exception exception) {
            final String msg = String.format("Failed deleting polaris table %s", name);
            log.error(msg, exception);
            throw new ConnectorException(msg, exception);
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
            final String tableFilter = (prefix != null && prefix.isTableDefinition()) ? prefix.getTableName() : "";
            final List<PolarisTableEntity> tbls =
                polarisStoreService.getTableEntities(name.getDatabaseName(), tableFilter);
            if (sort != null) {
                ConnectorUtils.sort(tbls, sort, Comparator.comparing(t -> t.getTblName()));
            }
            return ConnectorUtils.paginate(tbls, pageable).stream()
                .map(t -> polarisTableMapper.toInfo(t)).collect(Collectors.toList());
        } catch (Exception exception) {
            final String msg = String.format("Failed polaris list tables %s using prefix %s", name, prefix);
            log.error(msg, exception);
            throw new ConnectorException(msg, exception);
        }
    }

    /**
     * Return the table metadata from cache if exists else make the iceberg call and refresh it.
     * @param tableName             table name
     * @param tableMetadataLocation table metadata location
     * @param info                  table info stored in hive metastore
     * @param includeInfoDetails    if true, will include more details like the manifest file content
     * @param useCache              true, if table can be retrieved from cache
     * @return TableInfo
     */
    @Cacheable(key = "'iceberg.table.' + #includeInfoDetails + '.' + #tableMetadataLocation", condition = "#useCache")
    public TableInfo getIcebergTable(final QualifiedName tableName,
                                     final String tableMetadataLocation,
                                     final TableInfo info,
                                     final boolean includeInfoDetails,
                                     final boolean useCache) {
        final IcebergTableWrapper icebergTable =
            this.icebergTableHandler.getIcebergTable(tableName, tableMetadataLocation, includeInfoDetails);
        return connectorConverter.fromIcebergTableToTableInfo(tableName, icebergTable, tableMetadataLocation, info);
    }
}
