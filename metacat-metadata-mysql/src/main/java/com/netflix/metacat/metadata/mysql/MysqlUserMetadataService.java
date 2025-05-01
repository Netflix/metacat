/*
 *       Copyright 2017 Netflix, Inc.
 *          Licensed under the Apache License, Version 2.0 (the "License");
 *          you may not use this file except in compliance with the License.
 *          You may obtain a copy of the License at
 *              http://www.apache.org/licenses/LICENSE-2.0
 *          Unless required by applicable law or agreed to in writing, software
 *          distributed under the License is distributed on an "AS IS" BASIS,
 *          WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *          See the License for the specific language governing permissions and
 *          limitations under the License.
 */

package com.netflix.metacat.metadata.mysql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.DefinitionMetadataDto;
import com.netflix.metacat.common.dto.HasDataMetadata;
import com.netflix.metacat.common.dto.HasDefinitionMetadata;
import com.netflix.metacat.common.dto.HasMetadata;
import com.netflix.metacat.common.exception.MetacatBadRequestException;
import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.json.MetacatJsonException;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetadataException;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.usermetadata.BaseUserMetadataService;
import com.netflix.metacat.common.server.usermetadata.GetMetadataInterceptorParameters;
import com.netflix.metacat.common.server.usermetadata.MetadataInterceptor;
import com.netflix.metacat.common.server.usermetadata.MetadataPreMergeInterceptor;
import com.netflix.metacat.common.server.usermetadata.UserMetadataServiceException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Types;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * User metadata service.
 * <p>
 * Definition metadata (business metadata about the logical schema definition) is stored in two tables. Definition
 * metadata about the partitions are stored in 'partition_definition_metadata' table. Definition metadata about the
 * catalogs, databases and tables are stored in 'definition_metadata' table.
 * <p>
 * Data metadata (metadata about the data stored in the location referred by the schema). This information is stored in
 * 'data_metadata' table.
 */
@Slf4j
@SuppressFBWarnings
@Transactional("metadataTxManager")
public class MysqlUserMetadataService extends BaseUserMetadataService {
    protected static final String NAME_OWNER = "owner";
    protected static final String NAME_USERID = "userId";
    protected static final List<String> DEFINITION_METADATA_SORT_BY_COLUMNS = Arrays.asList(
        "id", "date_created", "created_by", "last_updated_by", "name", "last_updated");
    protected static final List<String> VALID_SORT_ORDER = Arrays.asList("ASC", "DESC");
    protected final MetacatJson metacatJson;
    protected final Config config;
    protected JdbcTemplate jdbcTemplate;
    protected final MetadataInterceptor metadataInterceptor;
    protected final MetadataPreMergeInterceptor metadataPreMergeInterceptor;

    /**
     * Constructor.
     *
     * @param jdbcTemplate        jdbc template
     * @param metacatJson         json utility
     * @param config              config
     * @param metadataInterceptor metadata interceptor
     * @param metadataPreMergeInterceptor metadataPreMergeInterceptor
     */
    public MysqlUserMetadataService(
        final JdbcTemplate jdbcTemplate,
        final MetacatJson metacatJson,
        final Config config,
        final MetadataInterceptor metadataInterceptor,
        final MetadataPreMergeInterceptor metadataPreMergeInterceptor
    ) {
        this.metacatJson = metacatJson;
        this.config = config;
        this.jdbcTemplate = jdbcTemplate;
        this.metadataInterceptor = metadataInterceptor;
        this.metadataPreMergeInterceptor = metadataPreMergeInterceptor;
    }

    @Override
    public void saveMetadata(final String userId, final HasMetadata holder, final boolean merge) {
        super.saveMetadata(userId, holder, merge);
    }

    @Override
    public void populateMetadata(final HasMetadata holder, final ObjectNode definitionMetadata,
                                 final ObjectNode dataMetadata) {
        super.populateMetadata(holder, definitionMetadata, dataMetadata);
    }

    @Nonnull
    @Override
    @Transactional(readOnly = true)
    public Optional<ObjectNode> getDefinitionMetadataWithInterceptor(
        @Nonnull final QualifiedName name,
        final GetMetadataInterceptorParameters getMetadataInterceptorParameters) {
        //not applying interceptor
        final Optional<ObjectNode> retData = getDefinitionMetadata(name);
        retData.ifPresent(objectNode ->
            this.metadataInterceptor.onRead(this, name, objectNode, getMetadataInterceptorParameters));
        return retData;
    }


    @Override
    public void softDeleteDataMetadata(
        final String user,
        @Nonnull final List<String> uris
    ) {
        try {
            final List<List<String>> subLists = Lists.partition(uris, config.getUserMetadataMaxInClauseItems());
            for (List<String> subUris : subLists) {
                _softDeleteDataMetadata(user, subUris);
            }
        } catch (Exception e) {
            final String message = String.format("Failed deleting the data metadata for %s", uris);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
    }

    @Override
    public void deleteDataMetadata(
        @Nonnull final List<String> uris
    ) {
        deleteDataMetadatasWithBatch(uris, true);
    }

    @Override
    public void deleteDataMetadataDeletes(
        @Nonnull final List<String> uris
    ) {
        deleteDataMetadatasWithBatch(uris, false);
    }

    private void deleteDataMetadatasWithBatch(final List<String> uris, final boolean removeDataMetadata) {
        try {
            final List<List<String>> subLists = Lists.partition(uris, config.getUserMetadataMaxInClauseItems());
            for (List<String> subUris : subLists) {
                _deleteDataMetadata(subUris, removeDataMetadata);
            }
        } catch (Exception e) {
            final String message = String.format("Failed deleting the data metadata for %s", uris);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
    }

    @Override
    public void deleteDefinitionMetadata(
        @Nonnull final List<QualifiedName> names
    ) {
        try {
            final List<List<QualifiedName>> subLists =
                Lists.partition(names, config.getUserMetadataMaxInClauseItems());
            for (List<QualifiedName> subNames : subLists) {
                _deleteDefinitionMetadata(subNames);
            }
        } catch (Exception e) {
            final String message = String.format("Failed deleting the definition metadata for %s", names);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
    }

    @Override
    public void deleteStaleDefinitionMetadata(
        @NonNull final String qualifiedNamePattern,
        @NonNull final Date lastUpdated) {
        if (qualifiedNamePattern == null || lastUpdated == null) {
            return;
        }

        try {
            jdbcTemplate.update(SQL.DELETE_DEFINITION_METADATA_STALE, new Object[]{qualifiedNamePattern, lastUpdated},
                new int[]{Types.VARCHAR, Types.TIMESTAMP});
        } catch (Exception e) {
            final String message = String.format("Failed to delete stale definition metadata for pattern %s",
                qualifiedNamePattern);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
    }

    @Override
    public void deleteMetadata(final String userId, final List<HasMetadata> holders) {
        try {
            final List<List<HasMetadata>> subLists =
                Lists.partition(holders, config.getUserMetadataMaxInClauseItems());
            for (List<HasMetadata> hasMetadatas : subLists) {
                final List<QualifiedName> names = hasMetadatas.stream()
                    .filter(m -> m instanceof HasDefinitionMetadata)
                    .map(m -> ((HasDefinitionMetadata) m).getDefinitionName())
                    .collect(Collectors.toList());
                if (!names.isEmpty()) {
                    _deleteDefinitionMetadata(names);
                }
                if (config.canSoftDeleteDataMetadata()) {
                    final List<String> uris = hasMetadatas.stream()
                        .filter(m -> m instanceof HasDataMetadata && ((HasDataMetadata) m).isDataExternal())
                        .map(m -> ((HasDataMetadata) m).getDataUri()).collect(Collectors.toList());
                    if (!uris.isEmpty()) {
                        _softDeleteDataMetadata(userId, uris);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed deleting metadatas", e);
            throw new UserMetadataServiceException("Failed deleting metadatas", e);
        }
    }

    /**
     * delete Definition Metadatas.
     *
     * @param names names to delete
     */
    @SuppressWarnings("checkstyle:methodname")
    private void _deleteDefinitionMetadata(
        @Nullable final List<QualifiedName> names
    ) {
        if (names != null && !names.isEmpty()) {
            for (QualifiedName name : names) {
                this.metadataInterceptor.onDelete(this, name);
            }
            final SqlParameterValue[] aNames = names.stream().filter(name -> !name.isPartitionDefinition())
                .map(n -> new SqlParameterValue(Types.VARCHAR, n))
                .toArray(SqlParameterValue[]::new);
            final SqlParameterValue[] aPartitionNames = names.stream().filter(QualifiedName::isPartitionDefinition)
                .map(n -> new SqlParameterValue(Types.VARCHAR, n))
                .toArray(SqlParameterValue[]::new);
            if (aNames.length > 0) {
                final List<String> paramVariables = Arrays.stream(aNames).map(s -> "?").collect(Collectors.toList());
                jdbcTemplate.update(
                    String.format(SQL.DELETE_DEFINITION_METADATA, Joiner.on(",").skipNulls().join(paramVariables)),
                    (Object[]) aNames);
            }
            if (aPartitionNames.length > 0) {
                final List<String> paramVariables =
                    Arrays.stream(aPartitionNames).map(s -> "?").collect(Collectors.toList());
                jdbcTemplate.update(
                    String.format(SQL.DELETE_PARTITION_DEFINITION_METADATA,
                        Joiner.on(",").skipNulls().join(paramVariables)), (Object[]) aPartitionNames);
            }
        }
    }

    /**
     * soft Delete Data Metadatas.
     *
     * @param userId user id
     * @param uris   uri list
     */

    @SuppressWarnings("checkstyle:methodname")
    private void _softDeleteDataMetadata(final String userId,
                                         @Nullable final List<String> uris) {
        if (uris != null && !uris.isEmpty()) {
            final List<String> paramVariables = uris.stream().map(s -> "?").collect(Collectors.toList());
            final String[] aUris = uris.toArray(new String[0]);
            final String paramString = Joiner.on(",").skipNulls().join(paramVariables);
            final List<Long> ids = jdbcTemplate
                .query(String.format(SQL.GET_DATA_METADATA_IDS, paramString), aUris, (rs, rowNum) -> rs.getLong("id"));
            if (!ids.isEmpty()) {
                final List<String> idParamVariables = ids.stream().map(s -> "?").collect(Collectors.toList());
                final Long[] aIds = ids.toArray(new Long[0]);
                final String idParamString = Joiner.on(",").skipNulls().join(idParamVariables);
                final List<Long> dupIds = jdbcTemplate
                    .query(String.format(SQL.GET_DATA_METADATA_DELETE_BY_IDS, idParamString), aIds,
                        (rs, rowNum) -> rs.getLong("id"));
                if (!dupIds.isEmpty()) {
                    ids.removeAll(dupIds);
                }
                final List<Object[]> deleteDataMetadatas = Lists.newArrayList();
                ids.forEach(id -> deleteDataMetadatas.add(new Object[]{id, userId}));
                final int[] colTypes = {Types.BIGINT, Types.VARCHAR};
                jdbcTemplate.batchUpdate(SQL.SOFT_DELETE_DATA_METADATA, deleteDataMetadatas, colTypes);
            }
        }
    }

    /**
     * delete Data Metadatas.
     *
     * @param uris               uri list
     * @param removeDataMetadata flag to remove data meta data
     */
    @SuppressWarnings("checkstyle:methodname")
    private void _deleteDataMetadata(
        @Nullable final List<String> uris,
        final boolean removeDataMetadata
    ) {
        if (uris != null && !uris.isEmpty()) {
            final List<String> paramVariables = uris.stream().map(s -> "?").collect(Collectors.toList());
            final String[] aUris = uris.toArray(new String[0]);
            final String paramString = Joiner.on(",").skipNulls().join(paramVariables);
            final List<Long> ids = jdbcTemplate
                .query(String.format(SQL.GET_DATA_METADATA_IDS, paramString), aUris, (rs, rowNum) -> rs.getLong("id"));
            if (!ids.isEmpty()) {
                final List<String> idParamVariables = ids.stream().map(s -> "?").collect(Collectors.toList());
                final SqlParameterValue[] aIds = ids.stream().map(id -> new SqlParameterValue(Types.BIGINT, id))
                    .toArray(SqlParameterValue[]::new);
                final String idParamString = Joiner.on(",").skipNulls().join(idParamVariables);
                jdbcTemplate.update(String.format(SQL.DELETE_DATA_METADATA_DELETE, idParamString), (Object[]) aIds);
                if (removeDataMetadata) {
                    jdbcTemplate.update(String.format(SQL.DELETE_DATA_METADATA, idParamString), (Object[]) aIds);
                }
            }
        }
    }

    @Nonnull
    @Override
    @Transactional(readOnly = true)
    public Optional<ObjectNode> getDataMetadata(
        @Nonnull final String uri) {
        return getJsonForKey(SQL.GET_DATA_METADATA, uri);
    }

    @Nonnull
    @Override
    @Transactional(readOnly = true)
    public Map<String, ObjectNode> getDataMetadataMap(
        @Nonnull final List<String> uris) {
        final Map<String, ObjectNode> result = Maps.newHashMap();
        if (!uris.isEmpty()) {
            final List<List<String>> parts = Lists.partition(uris, config.getUserMetadataMaxInClauseItems());
            parts.forEach(keys -> result.putAll(_getMetadataMap(keys, SQL.GET_DATA_METADATAS)));
        }
        return result;
    }

    @Nonnull
    @Override
    @Transactional(readOnly = true)
    public Optional<ObjectNode> getDefinitionMetadata(
        @Nonnull final QualifiedName name) {
        final Optional<ObjectNode> retData = getJsonForKey(
            name.isPartitionDefinition() ? SQL.GET_PARTITION_DEFINITION_METADATA : SQL.GET_DEFINITION_METADATA,
            name.toString());
        return retData;
    }

    @Nonnull
    protected Optional<ObjectNode> getDefinitionMetadataForUpdate(
        @Nonnull final QualifiedName name) {
        final Optional<ObjectNode> retData = getJsonForKey(
            name.isPartitionDefinition()
                ? SQL.GET_PARTITION_DEFINITION_METADATA : SQL.GET_DEFINITION_METADATA_FOR_UPDATE,
            name.toString());
        return retData;
    }

    @Override
    @Transactional(readOnly = true)
    public List<QualifiedName> getDescendantDefinitionNames(@Nonnull final QualifiedName name) {
        final List<String> result;
        try {
            result = jdbcTemplate
                .query(SQL.GET_DESCENDANT_DEFINITION_NAMES, new Object[]{name.toString() + "/%"},
                    new int[]{Types.VARCHAR},
                    (rs, rowNum) -> rs.getString("name"));
        } catch (Exception e) {
            final String message = String.format("Failed to get descendant names for %s", name);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
        return result.stream().map(QualifiedName::fromString).collect(Collectors.toList());
    }

    @Override
    @Transactional(readOnly = true)
    public List<String> getDescendantDataUris(@Nonnull final String uri) {
        final List<String> result;
        try {
            result = jdbcTemplate.query(SQL.GET_DESCENDANT_DATA_URIS, new Object[]{uri + "/%"},
                new int[]{Types.VARCHAR},
                (rs, rowNum) -> rs.getString("uri"));
        } catch (Exception e) {
            final String message = String.format("Failed to get descendant uris for %s", uri);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
        return result;
    }

    //TODO: For partition metadata, add interceptor if needed
    @Nonnull
    @Override
    @Transactional(readOnly = true)
    public Map<String, ObjectNode> getDefinitionMetadataMap(
        @Nonnull final List<QualifiedName> names) {
        //
        // names can contain partition names and non-partition names. Since definition metadata is stored in two tables,
        // metadata needs to be retrieved from both the tables.
        //
        final List<QualifiedName> oNames = names.stream().filter(name -> !name.isPartitionDefinition()).collect(
            Collectors.toList());
        final List<QualifiedName> partitionNames = names.stream().filter(QualifiedName::isPartitionDefinition).collect(
            Collectors.toList());
        final Map<String, ObjectNode> result = Maps.newHashMap();
        if (!oNames.isEmpty()) {
            result.putAll(_getNonPartitionDefinitionMetadataMap(oNames));
        }
        if (!partitionNames.isEmpty()) {
            result.putAll(_getPartitionDefinitionMetadata(partitionNames));
        }
        return result;
    }

    @SuppressWarnings("checkstyle:methodname")
    private Map<String, ObjectNode> _getNonPartitionDefinitionMetadataMap(final List<QualifiedName> names) {
        final List<List<QualifiedName>> parts = Lists.partition(names, config.getUserMetadataMaxInClauseItems());
        return parts.parallelStream()
            .map(keys -> _getMetadataMap(keys, SQL.GET_DEFINITION_METADATAS))
            .flatMap(it -> it.entrySet().stream())
            .collect(Collectors.toConcurrentMap(it -> QualifiedName.fromString(it.getKey()).toString(),
                Map.Entry::getValue));
    }

    @SuppressWarnings("checkstyle:methodname")
    private Map<String, ObjectNode> _getPartitionDefinitionMetadata(final List<QualifiedName> names) {
        final List<List<QualifiedName>> parts = Lists.partition(names, config.getUserMetadataMaxInClauseItems());
        return parts.parallelStream()
            .map(keys -> _getMetadataMap(keys, SQL.GET_PARTITION_DEFINITION_METADATAS))
            .flatMap(it -> it.entrySet().stream())
            .collect(Collectors.toConcurrentMap(it -> QualifiedName.fromString(it.getKey()).toString(),
                Map.Entry::getValue));
    }

    /**
     * get Metadata Map.
     *
     * @param keys list of keys
     * @param sql  query string
     * @return map of the metadata
     */
    @SuppressWarnings("checkstyle:methodname")
    private Map<String, ObjectNode> _getMetadataMap(@Nullable final List<?> keys, final String sql) {
        final Map<String, ObjectNode> result = Maps.newHashMap();
        if (keys == null || keys.isEmpty()) {
            return result;
        }
        final List<String> paramVariables = keys.stream().map(s -> "?").collect(Collectors.toList());
        final SqlParameterValue[] aKeys = keys.stream().map(o -> new SqlParameterValue(Types.VARCHAR, o.toString()))
            .toArray(SqlParameterValue[]::new);
        final String query = String.format(sql, Joiner.on(","
            + "").join(paramVariables));
        try {
            final ResultSetExtractor<Void> handler = resultSet -> {
                while (resultSet.next()) {
                    final String json = resultSet.getString("data");
                    final String name = resultSet.getString("name");
                    if (json != null) {
                        try {
                            result.put(name, metacatJson.parseJsonObject(json));
                        } catch (MetacatJsonException e) {
                            log.error("Invalid json '{}' for name '{}'", json, name);
                            throw new UserMetadataServiceException(
                                String.format("Invalid json %s for name %s", json, name), e);
                        }
                    }
                }
                return null;
            };
            jdbcTemplate.query(query, aKeys, handler);
        } catch (Exception e) {
            final String message = String.format("Failed to get data for %s", keys);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
        return result;
    }

    /**
     * get Json for key.
     *
     * @param query    query string
     * @param keyValue parameters
     * @return result object node
     */
    private Optional<ObjectNode> getJsonForKey(final String query, final String keyValue) {
        try {
            ResultSetExtractor<Optional<ObjectNode>> handler = rs -> {
                final String json;
                Optional<ObjectNode> result = Optional.empty();
                while (rs.next()) {
                    final String key = rs.getString(1);
                    if (keyValue.equalsIgnoreCase(key)) {
                        json = rs.getString(2);
                        if (Strings.isNullOrEmpty(json)) {
                            return Optional.empty();
                        }
                        result = Optional.ofNullable(metacatJson.parseJsonObject(json));
                        break;
                    }
                }
                return result;
            };
            return jdbcTemplate.query(query, new String[]{keyValue}, new int[]{Types.VARCHAR}, handler);
        } catch (MetacatJsonException e) {
            final String message = String.format("Invalid json %s for name %s", e.getInputJson(), keyValue);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        } catch (Exception e) {
            final String message = String.format("Failed to get data for %s", keyValue);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
    }

    /**
     * executeUpdateForKey.
     *
     * @param query     sql query string
     * @param keyValues parameters
     * @return number of updated rows
     */
    protected int executeUpdateForKey(final String query, final String... keyValues) {
        try {
            final SqlParameterValue[] values =
                Arrays.stream(keyValues).map(keyValue -> new SqlParameterValue(Types.VARCHAR, keyValue))
                    .toArray(SqlParameterValue[]::new);
            return jdbcTemplate.update(query, (Object[]) values);
        } catch (Exception e) {
            final String message = String.format("Failed to save data for %s", Arrays.toString(keyValues));
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
    }

    protected void throwIfPartitionDefinitionMetadataDisabled() {
        if (config.disablePartitionDefinitionMetadata()) {
            throw new MetacatBadRequestException("Partition Definition metadata updates are disabled");
        }
    }

    @Override
    public void saveDataMetadata(
        @Nonnull final String uri,
        @Nonnull final String userId,
        @Nonnull final Optional<ObjectNode> metadata, final boolean merge) {
        final Optional<ObjectNode> existingData = getDataMetadata(uri);
        final int count;
        if (existingData.isPresent() && metadata.isPresent()) {
            final ObjectNode merged = existingData.get();
            if (merge) {
                metacatJson.mergeIntoPrimary(merged, metadata.get());
            }
            count = executeUpdateForKey(SQL.UPDATE_DATA_METADATA, merged.toString(), userId, uri);
        } else {
            count = metadata.map(
                jsonNodes -> executeUpdateForKey(SQL.INSERT_DATA_METADATA, jsonNodes.toString(), userId, userId, uri))
                .orElse(1);
        }

        if (count != 1) {
            throw new IllegalStateException("Expected one row to be insert or update for " + uri);
        }
    }

    @Override
    public void saveDefinitionMetadata(
        @Nonnull final QualifiedName name,
        @Nonnull final String userId,
        @Nonnull final Optional<ObjectNode> metadata, final boolean merge)
        throws InvalidMetadataException {
        final Optional<ObjectNode> existingData =
            config.isDefinitionMetadataSelectForUpdateEnabled()
                ? getDefinitionMetadataForUpdate(name) : getDefinitionMetadata(name);
        metadataPreMergeInterceptor.onWrite(
            this,
            name,
            existingData,
            metadata
        );
        final int count;
        if (existingData.isPresent() && metadata.isPresent()) {
            ObjectNode merged = existingData.get();
            if (merge) {
                metacatJson.mergeIntoPrimary(merged, metadata.get());
            } else {
                merged = metadata.get();
            }
            //apply interceptor to change the object node
            this.metadataInterceptor.onWrite(this, name, merged);
            String query;
            if (name.isPartitionDefinition()) {
                throwIfPartitionDefinitionMetadataDisabled();
                query = SQL.UPDATE_PARTITION_DEFINITION_METADATA;
            } else {
                query = SQL.UPDATE_DEFINITION_METADATA;
            }
            count = executeUpdateForKey(
                query,
                merged.toString(),
                userId,
                name.toString());

        } else {
            // apply interceptor to change the object node
            if (metadata.isPresent()) {
                this.metadataInterceptor.onWrite(this, name, metadata.get());
            }
            String queryToExecute;
            if (name.isPartitionDefinition()) {
                throwIfPartitionDefinitionMetadataDisabled();
                queryToExecute = SQL.INSERT_PARTITION_DEFINITION_METADATA;
            } else {
                queryToExecute = SQL.INSERT_DEFINITION_METADATA;
            }
            count = metadata.map(jsonNodes -> executeUpdateForKey(
                queryToExecute,
                jsonNodes.toString(),
                userId,
                userId,
                name.toString()
            )).orElse(1);
        }

        if (count != 1) {
            throw new IllegalStateException("Expected one row to be insert or update for " + name);
        }
    }

    @Override
    public int renameDataMetadataKey(
        @Nonnull final String oldUri,
        @Nonnull final String newUri) {
        return executeUpdateForKey(SQL.RENAME_DATA_METADATA, newUri, oldUri);
    }

    @Override
    public int renameDefinitionMetadataKey(
        @Nonnull final QualifiedName oldName,
        @Nonnull final QualifiedName newName) {
        _deleteDefinitionMetadata(Lists.newArrayList(newName));
        return executeUpdateForKey(SQL.RENAME_DEFINITION_METADATA, newName.toString(), oldName.toString());
    }

    @Override
    public void saveMetadata(final String user, final List<? extends HasMetadata> metadatas, final boolean merge) {
        try {
            @SuppressWarnings("unchecked") final List<List<HasMetadata>> subLists = Lists.partition(
                (List<HasMetadata>) metadatas,
                config.getUserMetadataMaxInClauseItems()
            );
            for (List<HasMetadata> hasMetadatas : subLists) {
                final List<String> uris = Lists.newArrayList();
                final List<QualifiedName> names = Lists.newArrayList();
                // Get the names and uris
                final List<HasDefinitionMetadata> definitionMetadatas = Lists.newArrayList();
                final List<HasDataMetadata> dataMetadatas = Lists.newArrayList();
                hasMetadatas.forEach(hasMetadata -> {
                    if (hasMetadata instanceof HasDefinitionMetadata) {
                        final HasDefinitionMetadata oDef = (HasDefinitionMetadata) hasMetadata;
                        names.add(oDef.getDefinitionName());
                        if (oDef.getDefinitionMetadata() != null) {
                            definitionMetadatas.add(oDef);
                        }
                    }
                    if (hasMetadata instanceof HasDataMetadata) {
                        final HasDataMetadata oData = (HasDataMetadata) hasMetadata;
                        if (oData.isDataExternal() && oData.getDataMetadata() != null
                            && oData.getDataMetadata().size() > 0) {
                            uris.add(oData.getDataUri());
                            dataMetadatas.add(oData);
                        }
                    }
                });
                if (!definitionMetadatas.isEmpty() || !dataMetadatas.isEmpty()) {
                    // Get the existing metadata based on the names and uris
                    final Map<String, ObjectNode> definitionMap = getDefinitionMetadataMap(names);
                    final Map<String, ObjectNode> dataMap = getDataMetadataMap(uris);
                    // Curate the list of existing and new metadatas
                    final List<Object[]> insertDefinitionMetadatas = Lists.newArrayList();
                    final List<Object[]> updateDefinitionMetadatas = Lists.newArrayList();
                    final List<Object[]> insertPartitionDefinitionMetadatas = Lists.newArrayList();
                    final List<Object[]> updatePartitionDefinitionMetadatas = Lists.newArrayList();
                    final List<Object[]> insertDataMetadatas = Lists.newArrayList();
                    final List<Object[]> updateDataMetadatas = Lists.newArrayList();
                    definitionMetadatas.forEach(oDef -> {
                        final QualifiedName qualifiedName = oDef.getDefinitionName();
                        if (qualifiedName != null && oDef.getDefinitionMetadata() != null
                            && oDef.getDefinitionMetadata().size() != 0) {
                            final String name = qualifiedName.toString();
                            final ObjectNode oNode = definitionMap.get(name);
                            if (oNode == null) {
                                final Object[] o = new Object[]{
                                    metacatJson.toJsonString(oDef.getDefinitionMetadata()), user, user, name, };
                                if (qualifiedName.isPartitionDefinition()) {
                                    insertPartitionDefinitionMetadatas.add(o);
                                } else {
                                    insertDefinitionMetadatas.add(o);
                                }
                            } else {
                                metacatJson.mergeIntoPrimary(oNode, oDef.getDefinitionMetadata());
                                final Object[] o = new Object[]{metacatJson.toJsonString(oNode), user, name};
                                if (qualifiedName.isPartitionDefinition()) {
                                    updatePartitionDefinitionMetadatas.add(o);
                                } else {
                                    updateDefinitionMetadatas.add(o);
                                }
                            }
                        }
                    });
                    dataMetadatas.forEach(oData -> {
                        final String uri = oData.getDataUri();
                        final ObjectNode oNode = dataMap.get(uri);
                        if (oData.getDataMetadata() != null && oData.getDataMetadata().size() != 0) {
                            if (oNode == null) {
                                insertDataMetadatas.add(
                                    new Object[]{
                                        metacatJson.toJsonString(oData.getDataMetadata()),
                                        user,
                                        user,
                                        uri,
                                    }
                                );
                            } else {
                                metacatJson.mergeIntoPrimary(oNode, oData.getDataMetadata());
                                updateDataMetadatas
                                    .add(new Object[]{metacatJson.toJsonString(oNode), user, uri});
                            }
                        }
                    });
                    if (!insertDefinitionMetadatas.isEmpty()) {
                        jdbcTemplate.batchUpdate(SQL.INSERT_DEFINITION_METADATA, insertDefinitionMetadatas,
                            new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR});
                    }
                    if (!updateDefinitionMetadatas.isEmpty()) {
                        jdbcTemplate.batchUpdate(SQL.UPDATE_DEFINITION_METADATA, updateDefinitionMetadatas,
                            new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR});
                    }
                    if (!insertPartitionDefinitionMetadatas.isEmpty()) {
                        throwIfPartitionDefinitionMetadataDisabled();
                        jdbcTemplate.batchUpdate(SQL.INSERT_PARTITION_DEFINITION_METADATA,
                            insertPartitionDefinitionMetadatas,
                            new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR});
                    }
                    if (!updatePartitionDefinitionMetadatas.isEmpty()) {
                        throwIfPartitionDefinitionMetadataDisabled();
                        jdbcTemplate.batchUpdate(SQL.UPDATE_PARTITION_DEFINITION_METADATA,
                            updatePartitionDefinitionMetadatas,
                            new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR});
                    }
                    if (!insertDataMetadatas.isEmpty()) {
                        jdbcTemplate.batchUpdate(SQL.INSERT_DATA_METADATA, insertDataMetadatas,
                            new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR});
                    }
                    if (!updateDataMetadatas.isEmpty()) {
                        jdbcTemplate.batchUpdate(SQL.UPDATE_DATA_METADATA, updateDataMetadatas,
                            new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR});
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to save metadata", e);
            throw new UserMetadataServiceException("Failed to save metadata", e);
        }
    }

    @Override
    @Transactional(readOnly = true)
    public List<DefinitionMetadataDto> searchDefinitionMetadata(
        @Nullable final Set<String> propertyNames,
        @Nullable final String type,
        @Nullable final String name,
        @Nullable final HasMetadata holder,
        @Nullable final String sortBy,
        @Nullable final String sortOrder,
        @Nullable final Integer offset,
        @Nullable final Integer limit
    ) {
        final List<DefinitionMetadataDto> result = Lists.newArrayList();
        final SearchMetadataQuery queryObj = new SearchMetadataQuery(SQL.SEARCH_DEFINITION_METADATAS)
            .buildSearchMetadataQuery(
                propertyNames,
                type,
                name,
                sortBy,
                sortOrder,
                offset,
                limit
            );

        try {
            // Handler for reading the result set
            final ResultSetExtractor<Void> handler = rs -> {
                while (rs.next()) {
                    final String definitionName = rs.getString("name");
                    final String data = rs.getString("data");
                    final DefinitionMetadataDto definitionMetadataDto = new DefinitionMetadataDto();
                    definitionMetadataDto.setName(QualifiedName.fromString(definitionName));
                    definitionMetadataDto.setDefinitionMetadata(metacatJson.parseJsonObject(data));
                    result.add(definitionMetadataDto);
                }
                return null;
            };
            jdbcTemplate.query(queryObj.getSearchQuery().toString(), queryObj.getSearchParamList().toArray(), handler);
        } catch (Exception e) {
            log.error("Failed to search definition data", e);
            throw new UserMetadataServiceException("Failed to search definition data", e);
        }
        return result;
    }


    @Override
    @Transactional(readOnly = true)
    public List<QualifiedName> searchByOwners(final Set<String> owners) {
        final List<QualifiedName> result = Lists.newArrayList();
        final StringBuilder query = new StringBuilder(SQL.SEARCH_DEFINITION_METADATA_NAMES);
        final List<SqlParameterValue> paramList = Lists.newArrayList();
        query.append(" where 1=0");
        owners.forEach(s -> {
            query.append(" or data like ?");
            paramList.add(new SqlParameterValue(Types.VARCHAR, "%\"userId\":\"" + s.trim() + "\"%"));
        });
        final SqlParameterValue[] params = new SqlParameterValue[paramList.size()];
        try {
            // Handler for reading the result set
            final ResultSetExtractor<Void> handler = rs -> {
                while (rs.next()) {
                    final String definitionName = rs.getString("name");
                    result.add(QualifiedName.fromString(definitionName, false));
                }
                return null;
            };
            jdbcTemplate.query(query.toString(), paramList.toArray(params), handler);
        } catch (Exception e) {
            log.error("Failed to search by owners", e);
            throw new UserMetadataServiceException("Failed to search by owners", e);
        }
        return result;

    }

    @Override
    @Transactional(readOnly = true)
    public List<String> getDeletedDataMetadataUris(final Date deletedPriorTo, final Integer offset,
                                                   final Integer limit) {
        try {
            return jdbcTemplate.query(String.format(SQL.GET_DELETED_DATA_METADATA_URI, offset, limit),
                new Object[]{deletedPriorTo}, new int[]{Types.TIMESTAMP}, (rs, rowNum) -> rs.getString("uri"));
        } catch (Exception e) {
            final String message =
                String.format("Failed to get deleted data metadata uris deleted prior to %s", deletedPriorTo);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
    }

    @Override
    public void populateOwnerIfMissing(final HasDefinitionMetadata holder, final String owner) {
        ObjectNode definitionMetadata = holder.getDefinitionMetadata();
        if (definitionMetadata == null) {
            definitionMetadata = metacatJson.emptyObjectNode();
            holder.setDefinitionMetadata(definitionMetadata);
        }
        final ObjectNode ownerNode = definitionMetadata.with(NAME_OWNER);
        final JsonNode userId = ownerNode.get(NAME_USERID);
        if (userId == null || Strings.isNullOrEmpty(userId.textValue())) {
            ownerNode.put(NAME_USERID, owner);
        }
    }

    /**
     * Inner help class for generating the search definition/business metadata.
     */
    @Data
    class SearchMetadataQuery {
        private StringBuilder searchQuery;
        private List<SqlParameterValue> searchParamList = Lists.newArrayList();

        SearchMetadataQuery(final String querySQL) {
            this.searchQuery = new StringBuilder(querySQL);
        }

        SearchMetadataQuery buildSearchMetadataQuery(@Nullable final Set<String> propertyNames,
                                                     @Nullable final String type,
                                                     @Nullable final String name,
                                                     @Nullable final String sortByStr,
                                                     @Nullable final String sortOrderStr,
                                                     @Nullable final Integer offset,
                                                     @Nullable final Integer limit) {
            String sortBy = null;
            if (StringUtils.isNotBlank(sortByStr)) {
                sortBy = sortByStr.trim().toLowerCase();
                if (!DEFINITION_METADATA_SORT_BY_COLUMNS.contains(sortBy)) {
                    throw new IllegalArgumentException(String.format("Invalid sortBy column %s", sortBy));
                }
            }

            String sortOrder = null;
            if (StringUtils.isNotBlank(sortOrderStr)) {
                sortOrder = sortOrderStr.trim().toUpperCase();
                if (!VALID_SORT_ORDER.contains(sortOrder)) {
                    throw new IllegalArgumentException("Invalid sort order. Expected ASC or DESC");
                }
            }

            if (type != null) {
                String typeRegex = null;
                switch (type) {
                    case "catalog":
                        typeRegex = "^[^/]*$";
                        break;
                    case "database":
                        typeRegex = "^[^/]*/[^/]*$";
                        break;
                    case "table":
                        typeRegex = "^[^/]*/[^/]*/[^/]*$";
                        break;
                    case "partition":
                        typeRegex = "^[^/]*/[^/]*/[^/]*/.*$";
                        break;
                    default:
                }
                if (typeRegex != null) {
                    this.searchQuery.append(" and name rlike ?");
                    this.searchParamList.add(new SqlParameterValue(Types.VARCHAR, typeRegex));
                }
            }
            if (propertyNames != null && !propertyNames.isEmpty()) {
                propertyNames.forEach(propertyName -> {
                    this.searchQuery.append(" and data like ?");
                    searchParamList.add(new SqlParameterValue(Types.VARCHAR, "%\"" + propertyName + "\":%"));
                });
            }
            if (!Strings.isNullOrEmpty(name)) {
                this.searchQuery.append(" and name like ?");
                this.searchParamList.add(new SqlParameterValue(Types.VARCHAR, name));
            }
            if (!Strings.isNullOrEmpty(sortBy)) {
                this.searchQuery.append(" order by ").append(sortBy);
                if (!Strings.isNullOrEmpty(sortOrder)) {
                    this.searchQuery.append(" ").append(sortOrder);
                }
            }
            if (limit != null) {
                this.searchQuery.append(" limit ");
                if (offset != null) {
                    this.searchQuery.append(offset).append(",");
                }
                this.searchQuery.append(limit);
            }
            return this;
        }
    }

    protected static class SQL {
        static final String SOFT_DELETE_DATA_METADATA =
            "insert into data_metadata_delete(id, created_by,date_created) values (?,?, now())";
        static final String GET_DATA_METADATA_IDS =
            "select id from data_metadata where uri in (%s)";
        static final String GET_DATA_METADATA_DELETE_BY_IDS =
            "select id from data_metadata_delete where id in (%s)";
        static final String DELETE_DATA_METADATA_DELETE =
            "delete from data_metadata_delete where id in (%s)";
        static final String DELETE_DATA_METADATA =
            "delete from data_metadata where id in (%s)";
        static final String DELETE_DEFINITION_METADATA =
            "delete from definition_metadata where name in (%s)";
        static final String DELETE_DEFINITION_METADATA_STALE =
            "delete from definition_metadata where name like ? and last_updated < ?";
        static final String DELETE_PARTITION_DEFINITION_METADATA =
            "delete from partition_definition_metadata where name in (%s)";
        static final String GET_DATA_METADATA =
            "select uri name, data from data_metadata where uri=?";
        static final String GET_DELETED_DATA_METADATA_URI =
            "select uri from data_metadata_delete dmd join data_metadata dm on dmd.id=dm.id"
                + " where dmd.date_created < ? limit %d,%d";
        static final String GET_DESCENDANT_DATA_URIS =
            "select uri from data_metadata where uri like ?";
        static final String GET_DESCENDANT_DEFINITION_NAMES =
            "select name from partition_definition_metadata where name like ?";
        static final String GET_DATA_METADATAS =
            "select uri name,data from data_metadata where uri in (%s)";
        static final String GET_DEFINITION_METADATA =
            "select name, data from definition_metadata where name=?";
        static final String GET_DEFINITION_METADATA_FOR_UPDATE =
            "select name, data from definition_metadata where name=? for update";
        static final String GET_PARTITION_DEFINITION_METADATA =
            "select name, data from partition_definition_metadata where name=?";
        static final String GET_DEFINITION_METADATAS =
            "select name,data from definition_metadata where name in (%s)";
        static final String GET_PARTITION_DEFINITION_METADATAS =
            "select name,data from partition_definition_metadata where name in (%s)";
        static final String SEARCH_DEFINITION_METADATAS =
            "select name,data from definition_metadata where 1=1";
        static final String SEARCH_DEFINITION_METADATA_NAMES =
            "select name from definition_metadata";
        static final String INSERT_DATA_METADATA = "insert into data_metadata "
            + "(data, created_by, last_updated_by, date_created, last_updated, version, uri) values "
            + "(?, ?, ?, now(), now(), 0, ?)";
        static final String INSERT_DEFINITION_METADATA = "insert into definition_metadata "
            + "(data, created_by, last_updated_by, date_created, last_updated, version, name) values "
            + "(?, ?, ?, now(), now(), 0, ?)";
        static final String INSERT_PARTITION_DEFINITION_METADATA = "insert into partition_definition_metadata "
            + "(data, created_by, last_updated_by, date_created, last_updated, version, name) values "
            + "(?, ?, ?, now(), now(), 0, ?)";
        static final String RENAME_DATA_METADATA = "update data_metadata set uri=? where uri=?";
        static final String RENAME_DEFINITION_METADATA = "update definition_metadata set name=? where name=?";
        static final String UPDATE_DATA_METADATA =
            "update data_metadata set data=?, last_updated=now(), last_updated_by=? where uri=?";
        static final String UPDATE_DEFINITION_METADATA =
            "update definition_metadata set data=?, last_updated=now(), last_updated_by=? where name=?";
        static final String UPDATE_PARTITION_DEFINITION_METADATA =
            "update partition_definition_metadata set data=?, last_updated=now(), last_updated_by=? where name=?";
    }
}
