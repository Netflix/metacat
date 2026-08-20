package com.netflix.metacat.connector.polaris;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.usermetadata.AliasService;
import com.netflix.metacat.connector.polaris.store.repos.PolarisAliasRepository;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

/**
 * Resolves table aliases against the polaris store.
 */
@Slf4j
public class PolarisAliasService implements AliasService {
    private final PolarisAliasRepository aliasRepository;
    private final Config config;

    /**
     * Constructor.
     *
     * @param aliasRepository the alias repository
     * @param config the runtime config
     */
    public PolarisAliasService(final PolarisAliasRepository aliasRepository, final Config config) {
        this.aliasRepository = aliasRepository;
        this.config = config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public QualifiedName getTableName(@NonNull final QualifiedName tableAlias) {
        return resolve(tableAlias).orElse(tableAlias);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overridden so that the answer comes from the same cached lookup as
     * {@link #getTableName(QualifiedName)} rather than costing a second resolution.
     */
    @Override
    public boolean isAlias(@NonNull final QualifiedName name) {
        return config.isTableAliasEnabled() && resolve(name).isPresent();
    }

    private Optional<QualifiedName> resolve(final QualifiedName name) {
        if (name.getType() != QualifiedName.Type.TABLE) {
            return Optional.empty();
        }
        try {
            return aliasRepository
                .findSourceTable(name.getCatalogName(), name.getDatabaseName(), name.getTableName())
                .map(table -> QualifiedName.ofTable(
                    table.getCatalogName(), table.getDbName(), table.getTblName()));
        } catch (Exception e) {
            log.error("Failed to resolve alias for {}", name, e);
            throw new IllegalStateException("Failed to resolve alias for " + name, e);
        }
    }
}
