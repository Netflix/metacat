package com.netflix.metacat.connector.polaris;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.ConnectorUtils;
import com.netflix.metacat.common.server.connectors.exception.ConnectorException;
import com.netflix.metacat.common.server.connectors.exception.DatabaseAlreadyExistsException;
import com.netflix.metacat.common.server.connectors.exception.DatabaseNotFoundException;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException;
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo;
import com.netflix.metacat.connector.polaris.common.PolarisUtils;
import com.netflix.metacat.connector.polaris.mappers.PolarisDatabaseMapper;
import com.netflix.metacat.connector.polaris.store.PolarisStoreService;
import com.netflix.metacat.connector.polaris.store.entities.PolarisDatabaseEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * database service for polaris connector.
 */
@Slf4j
public class PolarisConnectorDatabaseService implements ConnectorDatabaseService {
    private static final String DEFAULT_LOCATION_SUFFIX = ".db";
    private static final String DB_DEFAULT_LOCATION = "polaris.db-default-location";
    private final String defaultLocationPrefix;
    private final PolarisStoreService polarisStoreService;

    private final ConnectorContext connectorContext;

    /**
     * Constructor.
     *
     * @param polarisStoreService polaris connector
     * @param connectorContext    connector context
     */
    public PolarisConnectorDatabaseService(
        final PolarisStoreService polarisStoreService,
        final ConnectorContext connectorContext
    ) {
        this.polarisStoreService = polarisStoreService;
        this.connectorContext = connectorContext;
        this.defaultLocationPrefix = connectorContext.getConfiguration().getOrDefault(DB_DEFAULT_LOCATION, "");
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void create(final ConnectorRequestContext context, final DatabaseInfo databaseInfo) {
        final QualifiedName name = databaseInfo.getName();
        final String createdBy = PolarisUtils.getUserOrDefault(context);

        // check exists then create in non-transactional optimistic manner
        if (exists(context, name)) {
            throw new DatabaseAlreadyExistsException(name);
        }
        try {
            final String location = databaseInfo.getUri() == null
                ? this.defaultLocationPrefix + name.getDatabaseName() + DEFAULT_LOCATION_SUFFIX : databaseInfo.getUri();
            final Map<String, String> params = databaseInfo.getMetadata();
            this.polarisStoreService.createDatabase(
                name.getCatalogName(), name.getDatabaseName(), location, createdBy, params);
        } catch (DataIntegrityViolationException exception) {
            throw new InvalidMetaException(name, exception);
        } catch (Exception exception) {
            throw new ConnectorException(
                String.format("Failed creating polaris database %s", name), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void delete(final ConnectorRequestContext context, final QualifiedName name) {
        // check exists then delete in non-transactional optimistic manner
        if (!exists(context, name)) {
            throw new DatabaseNotFoundException(name);
        }
        try {
            this.polarisStoreService.deleteDatabase(name.getCatalogName(), name.getDatabaseName());
        } catch (DataIntegrityViolationException exception) {
            throw new InvalidMetaException(name, exception);
        } catch (Exception exception) {
            throw new ConnectorException(
                String.format("Failed deleting polaris database %s", name), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void update(final ConnectorRequestContext context, final DatabaseInfo databaseInfo) {
        final QualifiedName name = databaseInfo.getName();
        try {
            final PolarisDatabaseEntity db =
                polarisStoreService.getDatabase(name.getCatalogName(), name.getDatabaseName())
                .orElseThrow(() -> new DatabaseNotFoundException(name));
            final Map<String, String> params = (db.getParams() != null)
                ? new HashMap<>(db.getParams()) : new HashMap<>();
            if (databaseInfo.getMetadata() != null) {
                params.putAll(databaseInfo.getMetadata());
            }
            db.setParams(params);
            // Temporarily setting location/audit from databaseInfo for backfill
            if (databaseInfo.getUri() != null) {
                db.setLocation(databaseInfo.getUri());
            }
            if (databaseInfo.getAudit() != null && databaseInfo.getAudit().getCreatedBy() != null) {
                db.getAudit().setCreatedBy(databaseInfo.getAudit().getCreatedBy());
            }
            db.getAudit().setLastModifiedBy(PolarisUtils.getUserOrDefault(context));
            polarisStoreService.saveDatabase(db.toBuilder().build());
        } catch (DatabaseNotFoundException exception) {
            log.error(String.format("Not found exception for polaris database %s", name), exception);
            throw exception;
        } catch (DataIntegrityViolationException exception) {
            throw new InvalidMetaException(name, exception);
        } catch (Exception exception) {
            throw new ConnectorException(
                String.format("Failed updating polaris database %s", databaseInfo.getName()), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public DatabaseInfo get(final ConnectorRequestContext context, final QualifiedName name) {
        try {
            final PolarisDatabaseMapper mapper = new PolarisDatabaseMapper();
            final PolarisDatabaseEntity db =
                polarisStoreService.getDatabase(name.getCatalogName(), name.getDatabaseName())
                .orElseThrow(() -> new DatabaseNotFoundException(name));
            return mapper.toInfo(db);
        } catch (DatabaseNotFoundException exception) {
            log.error(String.format("Not found exception for polaris database %s", name), exception);
            throw exception;
        } catch (Exception exception) {
            throw new ConnectorException(
                String.format("Failed get polaris database %s", name), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public boolean exists(final ConnectorRequestContext context, final QualifiedName name) {
        try {
            return polarisStoreService.getDatabase(name.getCatalogName(), name.getDatabaseName()).isPresent();
        } catch (Exception exception) {
            throw new ConnectorException(
                String.format("Failed exists polaris database %s", name), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<QualifiedName> listNames(
        final ConnectorRequestContext context,
        final QualifiedName name,
        @Nullable final QualifiedName prefix,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {
        try {
            final String dbPrefix = prefix == null ? "" : prefix.getDatabaseName();
            final List<QualifiedName> qualifiedNames = polarisStoreService.getDatabaseNames(
                name.getCatalogName(),
                dbPrefix, sort,
                    connectorContext.getConfig().getListDatabaseNamesPageSize(),
                    connectorContext.getConfig().isAuroraDataSourceEnabled())
                .stream()
                .map(dbName -> QualifiedName.ofDatabase(name.getCatalogName(), dbName))
                .collect(Collectors.toCollection(ArrayList::new));
            return ConnectorUtils.paginate(qualifiedNames, pageable);
        } catch (Exception exception) {
            throw new ConnectorException(
                String.format("Failed databases list names polaris prefix %s", prefix), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<DatabaseInfo> list(
        final ConnectorRequestContext context,
        final QualifiedName name,
        @Nullable final QualifiedName prefix,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {
        try {
            final PolarisDatabaseMapper mapper = new PolarisDatabaseMapper();
            final String dbPrefix = prefix == null ? "" : prefix.getDatabaseName();

            final List<PolarisDatabaseEntity> dbs = polarisStoreService.getDatabases(
                name.getCatalogName(),
                dbPrefix, sort,
                connectorContext.getConfig().getListDatabaseEntitiesPageSize(),
                connectorContext.getConfig().isAuroraDataSourceEnabled()
            );

            return ConnectorUtils.paginate(dbs, pageable).stream()
                .map(d -> mapper.toInfo(d)).collect(Collectors.toList());
        } catch (Exception exception) {
            throw new ConnectorException(
                String.format("Failed databases list polaris prefix %s", prefix), exception);
        }
    }
}
