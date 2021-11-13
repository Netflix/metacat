package com.netflix.metacat.connector.polaris;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.ConnectorUtils;
import com.netflix.metacat.common.server.connectors.exception.ConnectorException;
import com.netflix.metacat.common.server.connectors.exception.DatabaseAlreadyExistsException;
import com.netflix.metacat.common.server.connectors.exception.DatabaseNotFoundException;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException;
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo;
import com.netflix.metacat.connector.polaris.mappers.PolarisDatabaseMapper;
import com.netflix.metacat.connector.polaris.store.PolarisStoreConnector;
import com.netflix.metacat.connector.polaris.store.entities.PolarisDatabaseEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * database service for polaris connector.
 */
@Slf4j
public class PolarisConnectorDatabaseService implements ConnectorDatabaseService {
    private final PolarisStoreConnector polarisConnector;

    /**
     * Constructor.
     *
     * @param polarisConnector polaris connector
     */
    public PolarisConnectorDatabaseService(
        final PolarisStoreConnector polarisConnector
    ) {
        this.polarisConnector = polarisConnector;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void create(final ConnectorRequestContext context, final DatabaseInfo databaseInfo) {
        final QualifiedName name = databaseInfo.getName();
        // check exists then create in non-transactional optimistic manner
        if (exists(context, name)) {
            throw new DatabaseAlreadyExistsException(name);
        }
        try {
            this.polarisConnector.createDatabase(name.getDatabaseName());
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
            this.polarisConnector.deleteDatabase(name.getDatabaseName());
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
            final PolarisDatabaseEntity db = polarisConnector.getDatabase(name.getDatabaseName())
                .orElseThrow(() -> new DatabaseNotFoundException(name));
            // currently db objects have no mutable fields so this is noop
            polarisConnector.saveDatabase(db.toBuilder().build());
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
            final PolarisDatabaseMapper mapper = new PolarisDatabaseMapper(name.getCatalogName());
            final PolarisDatabaseEntity db = polarisConnector.getDatabase(name.getDatabaseName())
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
            return polarisConnector.getDatabase(name.getDatabaseName()).isPresent();
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
            List<QualifiedName> qualifiedNames = polarisConnector.getAllDatabases().stream()
                .map(d -> QualifiedName.ofDatabase(name.getCatalogName(), d.getDbName()))
                .collect(Collectors.toCollection(ArrayList::new));
            if (prefix != null) {
                qualifiedNames = qualifiedNames.stream()
                    .filter(n -> n.startsWith(prefix))
                    .collect(Collectors.toCollection(ArrayList::new));
            }
            if (sort != null) {
                ConnectorUtils.sort(qualifiedNames, sort, Comparator.comparing(QualifiedName::toString));
            }
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
            final PolarisDatabaseMapper mapper = new PolarisDatabaseMapper(name.getCatalogName());
            List<PolarisDatabaseEntity> dbs = polarisConnector.getAllDatabases();
            if (prefix != null) {
                dbs = dbs.stream()
                    .filter(n -> QualifiedName.ofDatabase(name.getCatalogName(), n.getDbName()).startsWith(prefix))
                    .collect(Collectors.toCollection(ArrayList::new));
            }
            if (sort != null) {
                ConnectorUtils.sort(dbs, sort, Comparator.comparing(p -> p.getDbName()));
            }
            return ConnectorUtils.paginate(dbs, pageable).stream()
                .map(d -> mapper.toInfo(d)).collect(Collectors.toList());
        } catch (Exception exception) {
            throw new ConnectorException(
                String.format("Failed databases list polaris prefix %s", prefix), exception);
        }
    }
}
