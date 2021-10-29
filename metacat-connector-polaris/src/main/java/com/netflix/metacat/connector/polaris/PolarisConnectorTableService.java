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
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.polaris.store.PolarisStoreConnector;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;

/**
 * table service for polaris connector.
 */
@Slf4j
public class PolarisConnectorTableService implements ConnectorTableService {
    protected final PolarisStoreConnector polarisConnector;
    protected final PolarisConnectorDatabaseService polarisConnectorDatabaseService;
    protected final HiveConnectorInfoConverter connectorConverter;
    protected final ConnectorContext connectorContext;
    protected final String catalogName;

    /**
     * Constructor.
     *
     * @param polarisConnector                  polaris connector
     * @param catalogName                       catalog name
     * @param polarisConnectorDatabaseService   connector database service
     * @param connectorConverter                converter
     * @param connectorContext                  the connector context
     */
    public PolarisConnectorTableService(
        final PolarisStoreConnector polarisConnector,
        final String catalogName,
        final PolarisConnectorDatabaseService polarisConnectorDatabaseService,
        final HiveConnectorInfoConverter connectorConverter,
        final ConnectorContext connectorContext
    ) {
        this.polarisConnector = polarisConnector;
        this.polarisConnectorDatabaseService = polarisConnectorDatabaseService;
        this.connectorConverter = connectorConverter;
        this.connectorContext = connectorContext;
        this.catalogName = catalogName;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void create(final ConnectorRequestContext requestContext, final TableInfo tableInfo) {
        final QualifiedName name = tableInfo.getName();
        try {
            polarisConnector.createTable(name.getDatabaseName(), name.getTableName());
        } catch (Exception exception) {
            // TODO: distinguish between different constraint exceptions once data model supports get APIs
            String msg = String.format("Failed creating polaris table %s", name);
            log.error(msg);
            log.error(exception.getMessage(), exception);
            throw new ConnectorException(msg, exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public TableInfo get(final ConnectorRequestContext requestContext, final QualifiedName name) {
        try {
            // TODO: implement once data model supports this operation
            return null;
        } catch (Exception exception) {
            String msg = String.format("Failed getting polaris table %s", name);
            log.error(msg);
            log.error(exception.getMessage(), exception);
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
            for (String tableName : polarisConnector.getTables(name.getDatabaseName(), tableFilter)) {
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
            String msg = String.format("Failed polaris list table names %s using prefix %s", name, prefix);
            log.error(msg);
            log.error(exception.getMessage(), exception);
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
            String msg = String.format("Failed updating polaris table %s", tableInfo.getName());
            log.error(msg);
            log.error(exception.getMessage(), exception);
            throw new ConnectorException(msg, exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void delete(final ConnectorRequestContext requestContext, final QualifiedName name) {
        try {
            polarisConnector.deleteTable(name.getDatabaseName(), name.getTableName());
        } catch (Exception exception) {
            // TODO: handle db not exist once data model supports get APIs
            String msg = String.format("Failed updating polaris table %s", name);
            log.error(msg);
            log.error(exception.getMessage(), exception);
            throw new ConnectorException(msg, exception);
        }
    }
}
