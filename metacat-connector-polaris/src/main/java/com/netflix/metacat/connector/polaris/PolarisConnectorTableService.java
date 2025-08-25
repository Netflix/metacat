package com.netflix.metacat.connector.polaris;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import com.netflix.metacat.common.server.connectors.exception.UnsupportedClientOperationException;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.util.MetacatUtils;
import com.netflix.metacat.connector.hive.commonview.CommonViewHandler;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableHandler;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableWrapper;
import com.netflix.metacat.connector.hive.sql.DirectSqlTable;
import com.netflix.metacat.connector.hive.util.HiveTableUtil;
import com.netflix.metacat.connector.polaris.common.PolarisUtils;
import com.netflix.metacat.connector.polaris.mappers.PolarisTableMapper;
import com.netflix.metacat.connector.polaris.store.PolarisStoreService;
import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.dao.DataIntegrityViolationException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * table service for polaris connector.
 */
@Slf4j
@CacheConfig(cacheNames = "metacat")
public class PolarisConnectorTableService implements ConnectorTableService {
    protected final PolarisStoreService polarisStoreService;
    protected final PolarisConnectorDatabaseService polarisConnectorDatabaseService;
    protected final HiveConnectorInfoConverter connectorConverter;
    protected final ConnectorContext connectorContext;
    protected final IcebergTableHandler icebergTableHandler;
    protected final CommonViewHandler commonViewHandler;
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
     * @param commonViewHandler                 common view handler
     * @param polarisTableMapper                polaris table polarisTableMapper
     * @param connectorContext                  the connector context
     */
    public PolarisConnectorTableService(
        final PolarisStoreService polarisStoreService,
        final String catalogName,
        final PolarisConnectorDatabaseService polarisConnectorDatabaseService,
        final HiveConnectorInfoConverter connectorConverter,
        final IcebergTableHandler icebergTableHandler,
        final CommonViewHandler commonViewHandler,
        final PolarisTableMapper polarisTableMapper,
        final ConnectorContext connectorContext
    ) {
        this.polarisStoreService = polarisStoreService;
        this.polarisConnectorDatabaseService = polarisConnectorDatabaseService;
        this.connectorConverter = connectorConverter;
        this.connectorContext = connectorContext;
        this.icebergTableHandler = icebergTableHandler;
        this.commonViewHandler = commonViewHandler;
        this.polarisTableMapper = polarisTableMapper;
        this.catalogName = catalogName;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void create(final ConnectorRequestContext requestContext, final TableInfo tableInfo) {
        final QualifiedName name = tableInfo.getName();
        final String createdBy = PolarisUtils.getUserOrDefault(requestContext);
        // check exists then create in non-transactional optimistic manner
        if (exists(requestContext, name)) {
            throw new TableAlreadyExistsException(name);
        }
        try {
            final PolarisTableEntity entity = polarisTableMapper.toEntity(tableInfo);
            if (HiveTableUtil.isCommonView(tableInfo)) {
                polarisStoreService.createTable(entity.getDbName(), entity.getTblName(),
                    entity.getMetadataLocation(), entity.getParams(), createdBy);
            } else {
                polarisStoreService.createTable(entity.getDbName(), entity.getTblName(),
                    entity.getMetadataLocation(), createdBy);
            }
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
            final String lastModifiedBy = PolarisUtils.getUserOrDefault(context);
            final PolarisTableEntity table = polarisStoreService
                    .getTable(oldName.getDatabaseName(), oldName.getTableName())
                    .orElseThrow(() -> new TableNotFoundException(oldName));
            table.getAudit().setLastModifiedBy(lastModifiedBy);
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
            final boolean isView = MetacatUtils.isCommonView(polarisTableEntity.getParams());
            final TableInfo info = polarisTableMapper.toInfo(polarisTableEntity, isView);
            final String tableLoc = HiveTableUtil.getIcebergTableMetadataLocation(info);

            // Return the iceberg table with just the metadata location included if requested.
            if (connectorContext.getConfig().shouldFetchOnlyMetadataLocationEnabled()
                    && requestContext.isIncludeMetadataLocationOnly()) {
                return TableInfo.builder()
                        .auditInfo(info.getAudit())
                        .metadata(Maps.newHashMap(info.getMetadata()))
                        .fields(Collections.emptyList())
                        .build();
            }
            if (isView) {
                return getCommonView(name, tableLoc, info, connectorContext.getConfig().isIcebergCacheEnabled());
            } else {
                return getIcebergTable(name, tableLoc, info,
                        requestContext.isIncludeMetadata(), connectorContext.getConfig().isIcebergCacheEnabled());
            }
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
            for (String tableName : polarisStoreService.getTables(name.getDatabaseName(),
                tableFilter,
                connectorContext.getConfig().getListTableNamesPageSize())
            ) {
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
        
        // Validate Iceberg branches/tags support for client compatibility
        validateIcebergBranchesTagsSupport(requestContext, name, tableInfo);
        
        final Config conf = connectorContext.getConfig();
        final String lastModifiedBy = PolarisUtils.getUserOrDefault(requestContext);
        final boolean isView = HiveTableUtil.isCommonView(tableInfo);
        if (isView) {
            commonViewHandler.update(tableInfo);
        } else {
            icebergTableHandler.update(tableInfo);
        }
        try {
            final Map<String, String> newTableMetadata = tableInfo.getMetadata();
            if (MapUtils.isEmpty(newTableMetadata)) {
                log.warn("No parameters defined for iceberg table %s, no data update needed", name);
                return;
            }

            final String prevLoc = newTableMetadata.get(DirectSqlTable.PARAM_PREVIOUS_METADATA_LOCATION);
            final String newLoc = newTableMetadata.get(DirectSqlTable.PARAM_METADATA_LOCATION);
            if (StringUtils.isBlank(prevLoc)) {
                log.info("Provided previous {} empty for {} with new {}, treating as no location update needed.",
                        prevLoc, name, newLoc);
                return;
            }
            if (StringUtils.isBlank(newLoc)) {
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

            boolean updated = false;
            if (isView) {
                final Map<String, String> newTableParams = polarisTableMapper.filterMetadata(newTableMetadata);
                final Map<String, String> existingTableParams = polarisStoreService
                    .getTable(name.getDatabaseName(), name.getTableName())
                    .orElseThrow(() -> new TableNotFoundException(name))
                    .getParams();
                // optimistically attempt to update metadata location and/or params
                updated = polarisStoreService.updateTableMetadataLocationAndParams(
                    name.getDatabaseName(), name.getTableName(), prevLoc, newLoc,
                    existingTableParams, newTableParams, lastModifiedBy);
            } else {
                // optimistically attempt to update metadata location
                updated = polarisStoreService.updateTableMetadataLocation(
                    name.getDatabaseName(), name.getTableName(), prevLoc, newLoc, lastModifiedBy
                );
            }

            // if succeeded then done, else try to figure out why and throw corresponding exception
            if (updated) {
                requestContext.setIgnoreErrorsAfterUpdate(true);
                log.warn("Success servicing Iceberg commit request for table: {}, "
                        + "previousLocation: {}, newLocation: {}",
                        tableInfo.getName(), prevLoc, newLoc);
                return;
            }
            final PolarisTableEntity table = polarisStoreService
                    .getTable(name.getDatabaseName(), name.getTableName())
                    .orElseThrow(() -> new TableNotFoundException(name));
            final String existingLoc = table.getMetadataLocation();
            log.warn("Error servicing Iceberg commit request for tableId: {}, "
                    + "previousLocation: {}, existingLocation: {}, newLocation: {}",
                    table.getTblId(), prevLoc, existingLoc, newLoc);
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
                polarisStoreService.getTableEntities(name.getDatabaseName(),
                    tableFilter,
                    connectorContext.getConfig().getListTableEntitiesPageSize());
            if (sort != null) {
                ConnectorUtils.sort(tbls, sort, Comparator.comparing(t -> t.getTblName()));
            }
            return ConnectorUtils.paginate(tbls, pageable).stream()
                .map(
                    t -> polarisTableMapper.toInfo(t, MetacatUtils.isCommonView(t.getParams()))
                )
                .collect(Collectors.toList());
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
     * @param info                  table info stored in Polaris
     * @param includeInfoDetails    if true, will include more details like the manifest file content
     * @param useCache              true, if table can be retrieved from cache
     * @return TableInfo
     */
    @Cacheable(
        cacheNames = "metacat",
        key = "'iceberg.table.' + #includeInfoDetails + '.' + #tableMetadataLocation",
        condition = "#useCache"
    )
    public TableInfo getIcebergTable(final QualifiedName tableName,
                                     final String tableMetadataLocation,
                                     final TableInfo info,
                                     final boolean includeInfoDetails,
                                     final boolean useCache) {
        final IcebergTableWrapper icebergTable =
            this.icebergTableHandler.getIcebergTable(tableName, tableMetadataLocation, includeInfoDetails);
        return connectorConverter.fromIcebergTableToTableInfo(tableName, icebergTable, tableMetadataLocation, info);
    }

    /**
     * Return the view metadata from cache if exists else make the iceberg call and refresh it.
     * @param tableName             table name
     * @param tableMetadataLocation table metadata location
     * @param info                  table info stored in Polaris
     * @param useCache              true, if table can be retrieved from cache
     * @return TableInfo
     */
    @Cacheable(key = "'iceberg.view.' + #tableMetadataLocation", condition = "#useCache")
    public TableInfo getCommonView(final QualifiedName tableName,
                                   final String tableMetadataLocation,
                                   final TableInfo info,
                                   final boolean useCache) {
        return commonViewHandler.getCommonViewTableInfo(
                tableName, tableMetadataLocation, info, new HiveTypeConverter()
        );
    }

    @Override
    public List<QualifiedName> getTableNames(
        final ConnectorRequestContext context,
        final QualifiedName name,
        final String filter,
        @Nullable final Integer limit) {
        try {
            if (!Strings.isNullOrEmpty(filter)) {
                // workaround for trino issue, hive param filters not supported on iceberg tables
                log.warn(String.format("Calling Polaris getTableNames with nonempty filter %s", filter));
            }
            final List<String> databaseNames = name.isDatabaseDefinition() ? ImmutableList.of(name.getDatabaseName())
                : polarisStoreService.getDatabaseNames(null, null,
                connectorContext.getConfig().getListDatabaseNamesPageSize()
            );
            int limitSize = limit == null || limit < 0 ? Integer.MAX_VALUE : limit;
            final List<QualifiedName> result = Lists.newArrayList();
            for (int i = 0; i < databaseNames.size() && limitSize > 0; i++) {
                final String databaseName = databaseNames.get(i);
                final List<String> tableNames = polarisStoreService.getTables(
                    name.getDatabaseName(),
                    "",
                    connectorContext.getConfig().getListTableNamesPageSize());
                result.addAll(tableNames.stream()
                    .map(n -> QualifiedName.ofTable(name.getCatalogName(), databaseName, n))
                    .limit(limitSize)
                    .collect(Collectors.toList()));
                limitSize = limitSize - tableNames.size();
            }
            return result;
        } catch (Exception exception) {
            final String msg = String.format("Failed polaris get table names using %s", name);
            log.error(msg, exception);
            throw new ConnectorException(msg, exception);
        }
    }

    /**
     * Validates that the client supports Iceberg branches and tags if the table has them.
     * This prevents older clients (e.g., Iceberg 0.9) from modifying tables that contain
     * branches or tags, which could result in data loss.
     *
     * @param requestContext the connector request context containing headers
     * @param name the qualified table name
     * @param tableInfo the table info
     * @throws UnsupportedClientOperationException if the client doesn't support branches/tags but the table has them
     */
    private void validateIcebergBranchesTagsSupport(final ConnectorRequestContext requestContext,
                                                   final QualifiedName name,
                                                   final TableInfo tableInfo) {
        // Only validate for Iceberg tables
        if (!HiveTableUtil.isIcebergTable(tableInfo)) {
            return;
        }

        // Check if the table has branches or tags using IcebergTableHandler
        boolean tableHasBranchesOrTags;
        try {
            final String tableMetadataLocation = HiveTableUtil.getIcebergTableMetadataLocation(tableInfo);
            if (StringUtils.isBlank(tableMetadataLocation)) {
                return; // Can't check without metadata location
            }
            
            // Get the Iceberg table metadata content through IcebergTableHandler
            final IcebergTableWrapper icebergWrapper = icebergTableHandler.getIcebergTable(name, tableMetadataLocation, true);
            final String metadataContent = icebergWrapper.getExtraProperties().get("metadata_content");
            
            // Check if table has branches or tags by parsing the metadata
            tableHasBranchesOrTags = hasIcebergBranchesOrTags(metadataContent);
        } catch (Exception e) {
            log.warn("Failed to check for branches/tags in table {}, allowing update to proceed: {}",
                name, e.getMessage());
            return;
        }

        // If the table doesn't have branches or tags, no validation needed
        if (!tableHasBranchesOrTags) {
            return;
        }

        // Check headers for client support
        final Map<String, String> headers = requestContext.getAdditionalContext();
        if (headers == null) {
            blockUnsupportedClient(name, "No headers provided");
            return;
        }

        // Check for direct support header
        final String branchesTagsSupportHeader = headers.get("X-Iceberg-Branches-Tags-Support");
        if (branchesTagsSupportHeader != null && "true".equalsIgnoreCase(branchesTagsSupportHeader)) {
            log.debug("Client supports Iceberg branches/tags for table {}", name);
            return;
        }

        // Check for Iceberg REST catalog spec version (0.14.1+ supports branches/tags)
        final String clientVersion = headers.get("X-Client-Version");
        if (clientVersion != null && isIcebergVersionSupported(clientVersion)) {
            log.debug("Iceberg REST catalog spec version {} supports branches/tags for table {}", clientVersion, name);
            return;
        }

        // Neither header indicates support
        blockUnsupportedClient(name, String.format("X-Iceberg-Branches-Tags-Support: %s, X-Client-Version: %s", 
            branchesTagsSupportHeader, clientVersion));
    }

    /**
     * Checks if the Iceberg REST catalog specification version supports branches and tags.
     * The X-Client-Version header contains the REST catalog spec version (e.g., "0.14.1").
     *
     * @param versionString the REST spec version string (e.g., "0.14.1", "0.15.0")
     * @return true if the REST spec version supports branches and tags
     */
    private boolean isIcebergVersionSupported(final String versionString) {
        if (StringUtils.isBlank(versionString)) {
            return false;
        }

        try {
            // Parse version string (e.g., "0.14.1" -> [0, 14, 1])
            final String[] versionParts = versionString.trim().split("\\.");
            if (versionParts.length < 2) {
                return false;
            }

            final int majorVersion = Integer.parseInt(versionParts[0]);
            final int minorVersion = Integer.parseInt(versionParts[1]);

            // Only handle REST catalog spec versions (0.x.y format)
            if (majorVersion == 0) {
                if (minorVersion > 14) {
                    return true; // 0.15+
                }
                if (minorVersion == 14) {
                    // For 0.14.x, check patch version if available
                    if (versionParts.length >= 3) {
                        final int patchVersion = Integer.parseInt(versionParts[2]);
                        return patchVersion >= 1;  // 0.14.1+
                    }
                    return false; // 0.14.0 doesn't support branches/tags
                }
                return false; // 0.13.x and below don't support branches/tags
            }

            // Unexpected major version (not 0.x.y format for REST spec)
            log.warn("Unexpected REST spec version format '{}', expected 0.x.y format", versionString);
            return false;
        } catch (NumberFormatException e) {
            log.warn("Failed to parse Iceberg REST spec version string '{}': {}", versionString, e.getMessage());
            return false;
        }
    }

    /**
     * Checks if the Iceberg table has branches or tags by parsing the metadata JSON.
     * This approach leverages the IcebergTableHandler to get the metadata content and 
     * parses it to detect branches/tags in the refs field.
     * 
     * @param metadataContent the JSON content of the Iceberg metadata file
     * @return true if branches or tags are found, false otherwise
     */
    private boolean hasIcebergBranchesOrTags(final String metadataContent) {
        if (StringUtils.isBlank(metadataContent)) {
            return false;
        }

        try {
            final ObjectMapper objectMapper = new ObjectMapper();
            final JsonNode rootNode = objectMapper.readTree(metadataContent);
            
            // Check for refs field (branches and tags are stored here)
            final JsonNode refsNode = rootNode.get("refs");
            if (refsNode != null && refsNode.isObject()) {
                // Check if there are refs other than the default 'main' branch
                // A table with only the main branch has refs.size() == 1 and contains "main"
                // A table with branches/tags has refs.size() > 1 OR doesn't have "main" as the only ref
                return refsNode.size() > 1 || (refsNode.size() == 1 && !refsNode.has("main"));
            }
            
            return false;
        } catch (Exception e) {
            log.warn("Failed to parse Iceberg metadata content for branches/tags detection: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Blocks the operation for unsupported clients.
     */
    private void blockUnsupportedClient(final QualifiedName name, final String headerInfo) {
        final String message = String.format(
            "Table '%s' contains Iceberg branches or tags, but the client does not support them. "
            + "Please use a client that supports Iceberg branches and tags (REST catalog spec 0.14.1+ "
            + "or client with X-Iceberg-Branches-Tags-Support header) to modify this table. Client headers: %s",
            name, headerInfo);

        log.warn("Blocking update to table {} due to unsupported client: {}", name, message);
        throw new UnsupportedClientOperationException(name, message);
    }
}
