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
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableHandler;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableWrapper;
import com.netflix.metacat.connector.hive.util.HiveTableUtil;
import com.netflix.metacat.connector.polaris.mappers.PolarisTableMapper;
import com.netflix.metacat.connector.polaris.store.PolarisStoreService;
import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
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
    protected final String catalogName;

    /**
     * Constructor.
     *
     * @param polarisStoreService               polaris connector
     * @param catalogName                       catalog name
     * @param polarisConnectorDatabaseService   connector database service
     * @param connectorConverter                converter
     * @param icebergTableHandler               iceberg table handler
     * @param connectorContext                  the connector context
     */
    public PolarisConnectorTableService(
        final PolarisStoreService polarisStoreService,
        final String catalogName,
        final PolarisConnectorDatabaseService polarisConnectorDatabaseService,
        final HiveConnectorInfoConverter connectorConverter,
        final IcebergTableHandler icebergTableHandler,
        final ConnectorContext connectorContext
    ) {
        this.polarisStoreService = polarisStoreService;
        this.polarisConnectorDatabaseService = polarisConnectorDatabaseService;
        this.connectorConverter = connectorConverter;
        this.connectorContext = connectorContext;
        this.icebergTableHandler = icebergTableHandler;
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
            final PolarisTableMapper mapper = new PolarisTableMapper(name.getCatalogName());
            final PolarisTableEntity entity = mapper.toEntity(tableInfo);
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
            final PolarisTableMapper mapper = new PolarisTableMapper(name.getCatalogName());
            final TableInfo info = mapper.toInfo(polarisTableEntity);
            final String tableLoc = HiveTableUtil.getIcebergTableMetadataLocation(info);
            final IcebergTableWrapper icebergTable = icebergTableHandler.getIcebergTable(
                name, tableLoc, connectorContext.getConfig().isIcebergCacheEnabled());
            return connectorConverter.fromIcebergTableToTableInfo(name, icebergTable, tableLoc, info);
        } catch (TableNotFoundException exception) {
            log.error(String.format("Not found exception for polaris table %s", name), exception);
            throw exception;
        } catch (Exception exception) {
            final String msg = String.format("Failed getting polaris table %s", name);
            log.error(msg, exception);
            throw new ConnectorException(msg, exception);
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
        try {
            // TODO: implement once data model supports this operation
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
            final PolarisTableMapper mapper = new PolarisTableMapper(name.getCatalogName());
            final String tableFilter = (prefix != null && prefix.isTableDefinition()) ? prefix.getTableName() : "";
            final List<PolarisTableEntity> tbls =
                polarisStoreService.getTableEntities(name.getDatabaseName(), tableFilter);
            if (sort != null) {
                ConnectorUtils.sort(tbls, sort, Comparator.comparing(t -> t.getTblName()));
            }
            return ConnectorUtils.paginate(tbls, pageable).stream()
                .map(t -> mapper.toInfo(t)).collect(Collectors.toList());
        } catch (Exception exception) {
            final String msg = String.format("Failed polaris list tables %s using prefix %s", name, prefix);
            log.error(msg, exception);
            throw new ConnectorException(msg, exception);
        }
    }
}
