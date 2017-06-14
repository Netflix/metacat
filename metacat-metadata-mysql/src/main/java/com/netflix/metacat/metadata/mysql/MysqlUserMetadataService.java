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
import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.json.MetacatJsonException;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.usermetadata.BaseUserMetadataService;
import com.netflix.metacat.common.server.usermetadata.UserMetadataServiceException;
import com.netflix.metacat.common.server.util.JdbcUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * User metadata service.
 */
@Slf4j
@SuppressFBWarnings
public class MysqlUserMetadataService extends BaseUserMetadataService {
    /**
     * Data source name used by user metadata service.
     */
    private final MetacatJson metacatJson;
    private final Config config;
    private JdbcUtil jdbcUtil;

    /**
     * Constructor.
     *
     * @param dataSource  data source
     * @param metacatJson json utility
     * @param config      config
     */
    public MysqlUserMetadataService(
        final DataSource dataSource,
        final MetacatJson metacatJson,
        final Config config
    ) {
        this.metacatJson = metacatJson;
        this.config = config;
        this.jdbcUtil = new JdbcUtil(dataSource);
    }

    @Override
    public void softDeleteDataMetadatas(
        final String user,
        @Nonnull final List<String> uris
    ) {
        try {
            final List<List<String>> subLists = Lists.partition(uris, config.getUserMetadataMaxInClauseItems());
            for (List<String> subUris : subLists) {
                _softDeleteDataMetadatas(user, subUris);
            }
        } catch (DataAccessException e) {
            log.error("DataAccessException exception", e);
            throw new UserMetadataServiceException(String.format("Failed deleting the data metadata for %s", uris), e);
        }
    }

    @Override
    public void deleteDataMetadatas(
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
                _deleteDataMetadatas(subUris, removeDataMetadata);
            }
        } catch (DataAccessException e) {
            log.error("DataAccessException exception", e);
            throw new UserMetadataServiceException(String.format("Failed deleting the data metadata for %s", uris), e);
        }
    }

    @Override
    public void deleteDefinitionMetadatas(
        @Nonnull final List<QualifiedName> names
    ) {
        try {
            final List<List<QualifiedName>> subLists =
                Lists.partition(names, config.getUserMetadataMaxInClauseItems());
            for (List<QualifiedName> subNames : subLists) {
                _deleteDefinitionMetadatas(subNames);
            }
        } catch (DataAccessException e) {
            log.error("DataAccessException exception", e);
            throw new UserMetadataServiceException(
                String.format("Failed deleting the definition metadata for %s", names), e);
        }
    }

    @Override
    public void deleteMetadatas(final String userId, final List<HasMetadata> holders) {
        try {
            jdbcUtil.getTransactionTemplate().execute(new TransactionCallbackWithoutResult() {
                @Override
                protected void doInTransactionWithoutResult(final TransactionStatus status) {
                    final List<List<HasMetadata>> subLists =
                        Lists.partition(holders, config.getUserMetadataMaxInClauseItems());
                    for (List<HasMetadata> hasMetadatas : subLists) {
                        final List<QualifiedName> names = hasMetadatas.stream()
                            .filter(m -> m instanceof HasDefinitionMetadata)
                            .map(m -> ((HasDefinitionMetadata) m).getDefinitionName())
                            .collect(Collectors.toList());
                        if (!names.isEmpty()) {
                            _deleteDefinitionMetadatas(names);
                        }
                        if (config.canSoftDeleteDataMetadata()) {
                            final List<String> uris = hasMetadatas.stream()
                                .filter(m -> m instanceof HasDataMetadata && ((HasDataMetadata) m).isDataExternal())
                                .map(m -> ((HasDataMetadata) m).getDataUri()).collect(Collectors.toList());
                            if (!uris.isEmpty()) {
                                _softDeleteDataMetadatas(userId, uris);
                            }
                        }
                    }
                }
            });
        } catch (DataAccessException e) {
            log.error("DataAccessException exception", e);
            throw new UserMetadataServiceException("Failed deleting data metadata", e);
        }
    }

    /**
     * delete Definition Metadatas.
     *
     * @param names names to delete
     * @return null or void
     */
    @SuppressWarnings("checkstyle:methodname")
    @Transactional
    public Void _deleteDefinitionMetadatas(
        @Nullable final List<QualifiedName> names
    ) {
        if (names != null && !names.isEmpty()) {
            final List<String> paramVariables = names.stream().map(s -> "?").collect(Collectors.toList());
            final String[] aNames = names.stream().map(QualifiedName::toString).toArray(String[]::new);
            jdbcUtil.getTransactionTemplate().execute(new TransactionCallbackWithoutResult() {
                @Override
                protected void doInTransactionWithoutResult(final TransactionStatus status) {
                    jdbcUtil.getJdbcTemplate().update(
                        String.format(SQL.DELETE_DEFINITION_METADATA, Joiner.on(",").skipNulls().join(paramVariables)),
                        (Object[]) aNames);
                }
            });
        }
        return null;
    }

    /**
     * soft Delete Data Metadatas.
     *
     * @param userId user id
     * @param uris   uri list
     * @return null or void
     * @throws DataAccessException data access exception
     */

    @SuppressWarnings("checkstyle:methodname")
    @Transactional
    public Void _softDeleteDataMetadatas(final String userId,
                                         @Nullable final List<String> uris
    ) throws DataAccessException {
        if (uris != null && !uris.isEmpty()) {
            final List<String> paramVariables = uris.stream().map(s -> "?").collect(Collectors.toList());
            final String[] aUris = uris.stream().toArray(String[]::new);
            final String paramString = Joiner.on(",").skipNulls().join(paramVariables);
            final List<Long> ids = jdbcUtil.getJdbcTemplate()
                .query(String.format(SQL.GET_DATA_METADATA_IDS, paramString), aUris, (rs, rowNum) -> rs.getLong("id"));
            if (!ids.isEmpty()) {
                final List<String> idParamVariables = ids.stream().map(s -> "?").collect(Collectors.toList());
                final Long[] aIds = ids.stream().toArray(Long[]::new);
                final String idParamString = Joiner.on(",").skipNulls().join(idParamVariables);
                final List<Long> dupIds = jdbcUtil.getJdbcTemplate()
                    .query(String.format(SQL.GET_DATA_METADATA_DELETE_BY_IDS, idParamString), aIds,
                        (rs, rowNum) -> rs.getLong("id"));
                if (!dupIds.isEmpty()) {
                    ids.removeAll(dupIds);
                }
                final List<Object[]> deleteDataMetadatas = Lists.newArrayList();
                ids.forEach(id -> deleteDataMetadatas.add(new Object[]{id, userId}));
                jdbcUtil.getTransactionTemplate().execute(new TransactionCallbackWithoutResult() {
                    @Override
                    protected void doInTransactionWithoutResult(final TransactionStatus status) {
                        jdbcUtil.getJdbcTemplate().batchUpdate(SQL.SOFT_DELETE_DATA_METADATA,
                            deleteDataMetadatas);
                    }
                });
            }
        }
        return null;
    }

    /**
     * delete Data Metadatas.
     *
     * @param uris               uri list
     * @param removeDataMetadata flag to remove data meta data
     * @return void or null
     */
    @SuppressWarnings("checkstyle:methodname")
    @Transactional
    public Void _deleteDataMetadatas(
        @Nullable final List<String> uris,
        final boolean removeDataMetadata
    ) {
        if (uris != null && !uris.isEmpty()) {
            final List<String> paramVariables = uris.stream().map(s -> "?").collect(Collectors.toList());
            final String[] aUris = uris.stream().toArray(String[]::new);
            final String paramString = Joiner.on(",").skipNulls().join(paramVariables);
            final List<Long> ids = jdbcUtil.getJdbcTemplate()
                .query(String.format(SQL.GET_DATA_METADATA_IDS, paramString), aUris, (rs, rowNum) -> rs.getLong("id"));
            if (!ids.isEmpty()) {
                jdbcUtil.getTransactionTemplate().execute(new TransactionCallbackWithoutResult() {
                    @Override
                    protected void doInTransactionWithoutResult(final TransactionStatus status) {
                        final List<String> idParamVariables = ids.stream().map(s -> "?").collect(Collectors.toList());
                        final Long[] aIds = ids.stream().toArray(Long[]::new);
                        final String idParamString = Joiner.on(",").skipNulls().join(idParamVariables);
                        jdbcUtil.getJdbcTemplate()
                            .update(String.format(SQL.DELETE_DATA_METADATA_DELETE, idParamString), (Object[]) aIds);
                        if (removeDataMetadata) {
                            jdbcUtil.getJdbcTemplate()
                                .update(String.format(SQL.DELETE_DATA_METADATA, idParamString), (Object[]) aIds);
                        }
                    }
                });
            }
        }
        return null;
    }

    @Nonnull
    @Override
    public Optional<ObjectNode> getDataMetadata(
        @Nonnull final String uri) {
        return getJsonForKey(SQL.GET_DATA_METADATA, uri);
    }

    @Nonnull
    @Override
    public Map<String, ObjectNode> getDataMetadataMap(
        @Nonnull final List<String> uris) {
        final Map<String, ObjectNode> result = Maps.newHashMap();
        if (!uris.isEmpty()) {
            final List<List<String>> parts = Lists.partition(uris, config.getUserMetadataMaxInClauseItems());
            parts.stream().forEach(keys -> result.putAll(_getMetadataMap(keys, SQL.GET_DATA_METADATAS)));
        }
        return result;
    }

    @Nonnull
    @Override
    public Optional<ObjectNode> getDefinitionMetadata(
        @Nonnull final QualifiedName name) {
        return getJsonForKey(SQL.GET_DEFINITION_METADATA, name.toString());
    }

    @Override
    @Transactional(readOnly = true)
    public List<QualifiedName> getDescendantDefinitionNames(@Nonnull final QualifiedName name) {
        final List<String> result;
        try {
            result = jdbcUtil.getJdbcTemplate()
                .query(SQL.GET_DESCENDANT_DEFINITION_NAMES, new Object[]{name.toString() + "/%"},
                    (rs, rowNum) -> rs.getString("name"));
        } catch (DataAccessException e) {
            log.error("DataAccessException exception", e);
            throw new UserMetadataServiceException(String.format("Failed to get descendant names for %s", name), e);
        }
        return result.stream().map(QualifiedName::fromString).collect(Collectors.toList());
    }

    @Override
    @Transactional(readOnly = true)
    public List<String> getDescendantDataUris(@Nonnull final String uri) {
        final List<String> result;
        try {
            result = jdbcUtil.getJdbcTemplate().query(SQL.GET_DESCENDANT_DATA_URIS, new Object[]{uri + "/%"},
                (rs, rowNum) -> rs.getString("uri"));
        } catch (DataAccessException e) {
            log.error("DataAccessException exception", e);
            throw new UserMetadataServiceException(String.format("Failed to get descendant uris for %s", uri), e);
        }
        return result;
    }

    @Nonnull
    @Override
    public Map<String, ObjectNode> getDefinitionMetadataMap(
        @Nonnull final List<QualifiedName> names) {
        if (!names.isEmpty()) {
            final List<List<QualifiedName>> parts = Lists.partition(names, config.getUserMetadataMaxInClauseItems());
            return parts.stream()
                .map(keys -> _getMetadataMap(keys, SQL.GET_DEFINITION_METADATAS))
                .flatMap(it -> it.entrySet().stream())
                .collect(Collectors.toMap(it -> QualifiedName.fromString(it.getKey()).toString(),
                    Map.Entry::getValue));
        } else {
            return Collections.emptyMap();
        }
    }

    /**
     * get Metadata Map.
     *
     * @param keys list of keys
     * @param sql  query string
     * @return map of the metadata
     */
    @SuppressWarnings("checkstyle:methodname")
    @Transactional(readOnly = true)
    public Map<String, ObjectNode> _getMetadataMap(@Nullable final List<?> keys, final String sql) {
        final Map<String, ObjectNode> result = Maps.newHashMap();
        if (keys == null || keys.isEmpty()) {
            return result;
        }
        final List<String> paramVariables = keys.stream().map(s -> "?").collect(Collectors.toList());
        final String[] aKeys = keys.stream().map(Object::toString).toArray(String[]::new);
        final String query = String.format(sql, Joiner.on(",").join(paramVariables));
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
            jdbcUtil.getJdbcTemplate().query(query, aKeys, handler);
        } catch (DataAccessException e) {
            log.error("DataAccessException exception", e);
            throw new UserMetadataServiceException(String.format("Failed to get data for %s", keys), e);
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
    @Transactional(readOnly = true)
    public Optional<ObjectNode> getJsonForKey(final String query, final String keyValue) {
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
            return jdbcUtil.getJdbcTemplate().query(new PreparedStatementCreator() {
                @Override
                public PreparedStatement createPreparedStatement(final Connection connection)
                    throws SQLException {
                    final PreparedStatement ps = connection.prepareStatement(query);
                    ps.setString(1, keyValue);
                    return ps;
                }
            }, handler);
        } catch (DataAccessException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException(String.format("Failed to get data for %s", keyValue), e);
        } catch (MetacatJsonException e) {
            log.error("Invalid json '{}' for keyValue '{}'", e.getInputJson(), keyValue, e);
            throw new UserMetadataServiceException(
                String.format("Invalid json %s for name %s", e.getInputJson(), keyValue), e);
        }
    }

    /**
     * executeUpdateForKey.
     *
     * @param query     sql query string
     * @param keyValues parameters
     * @return number of updated rows
     */
    @Transactional
    public int executeUpdateForKey(final String query, final String... keyValues) {
        try {
            return jdbcUtil.getTransactionTemplate().execute(new TransactionCallback<Integer>() {
                @Override
                public Integer doInTransaction(final TransactionStatus status) {
                    return jdbcUtil.getJdbcTemplate().update(query, (Object[]) keyValues);
                }
            });
        } catch (DataAccessException e) {
            log.error("DataAccessException exception", e);
            throw new UserMetadataServiceException(
                String.format("Failed to save data for %s", Arrays.toString(keyValues)), e);
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
        } else if (metadata.isPresent()) {
            count = executeUpdateForKey(SQL.INSERT_DATA_METADATA, metadata.get().toString(), userId, userId, uri);
        } else {
            // Nothing to insert in this case
            count = 1;
        }

        if (count != 1) {
            throw new IllegalStateException("Expected one row to be insert or update for " + uri);
        }
    }

    @Override
    public void saveDefinitionMetadata(
        @Nonnull final QualifiedName name,
        @Nonnull final String userId,
        @Nonnull final Optional<ObjectNode> metadata, final boolean merge) {
        final Optional<ObjectNode> existingData = getDefinitionMetadata(name);
        final int count;
        if (existingData.isPresent() && metadata.isPresent()) {
            final ObjectNode merged = existingData.get();
            if (merge) {
                metacatJson.mergeIntoPrimary(merged, metadata.get());
            }
            count = executeUpdateForKey(SQL.UPDATE_DEFINITION_METADATA, merged.toString(), userId, name.toString());
        } else if (metadata.isPresent()) {
            count = executeUpdateForKey(
                SQL.INSERT_DEFINITION_METADATA,
                metadata.get().toString(),
                userId,
                userId,
                name.toString()
            );
        } else {
            // Nothing to insert in this case
            count = 1;
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
        return executeUpdateForKey(SQL.RENAME_DEFINITION_METADATA, newName.toString(), oldName.toString());
    }

    @Override
    @Transactional
    public void saveMetadatas(final String user, final List<? extends HasMetadata> metadatas, final boolean merge) {
        jdbcUtil.getTransactionTemplate().execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(final TransactionStatus status) {
                executeSaveMetadatas(user, metadatas, merge);
            }
        });
    }

    private void executeSaveMetadatas(final String user,
                                      final List<? extends HasMetadata> metadatas,
                                      final boolean merge) {
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
                hasMetadatas.stream().forEach(hasMetadata -> {
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
                    final List<Object[]> insertDataMetadatas = Lists.newArrayList();
                    final List<Object[]> updateDataMetadatas = Lists.newArrayList();
                    definitionMetadatas.stream().forEach(oDef -> {
                        final QualifiedName qualifiedName = oDef.getDefinitionName();
                        if (qualifiedName != null && oDef.getDefinitionMetadata() != null
                            && oDef.getDefinitionMetadata().size() != 0) {
                            final String name = qualifiedName.toString();
                            final ObjectNode oNode = definitionMap.get(name);
                            if (oNode == null) {
                                insertDefinitionMetadatas
                                    .add(
                                        new Object[]{
                                            metacatJson.toJsonString(oDef.getDefinitionMetadata()),
                                            user,
                                            user,
                                            name,
                                        }
                                    );
                            } else {
                                metacatJson.mergeIntoPrimary(oNode, oDef.getDefinitionMetadata());
                                updateDefinitionMetadatas
                                    .add(new Object[]{
                                            metacatJson.toJsonString(oNode),
                                            user,
                                            name,
                                        }
                                    );
                            }
                        }
                    });
                    dataMetadatas.stream().forEach(oData -> {
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
                        MySqlServiceUtil.batchUpdateValues(jdbcUtil,
                            SQL.INSERT_DEFINITION_METADATA, insertDefinitionMetadatas);
                    }
                    if (!updateDefinitionMetadatas.isEmpty()) {
                        MySqlServiceUtil.batchUpdateValues(jdbcUtil,
                            SQL.UPDATE_DEFINITION_METADATA, updateDefinitionMetadatas);
                    }
                    if (!insertDataMetadatas.isEmpty()) {
                        MySqlServiceUtil.batchUpdateValues(jdbcUtil,
                            SQL.INSERT_DATA_METADATA, insertDataMetadatas);
                    }
                    if (!updateDataMetadatas.isEmpty()) {
                        MySqlServiceUtil.batchUpdateValues(jdbcUtil,
                            SQL.UPDATE_DATA_METADATA, updateDataMetadatas);
                    }
                }
            }
        } catch (DataAccessException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException("Failed to save metadata", e);
        }
    }

    @Override
    public List<DefinitionMetadataDto> searchDefinitionMetadatas(
        @Nullable final Set<String> propertyNames,
        @Nullable final String type,
        @Nullable final String name,
        @Nullable final String sortBy,
        @Nullable final String sortOrder,
        @Nullable final Integer offset,
        @Nullable final Integer limit
    ) {
        final List<DefinitionMetadataDto> result = Lists.newArrayList();
        final StringBuilder query = new StringBuilder(SQL.SEARCH_DEFINITION_METADATAS);
        final List<Object> paramList = Lists.newArrayList();
        if (type != null) {
            String typeRegex = null;
            switch (type) {
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
                query.append(" and name rlike ?");
                paramList.add(typeRegex);
            }
        }
        if (propertyNames != null && !propertyNames.isEmpty()) {
            propertyNames.forEach(propertyName -> {
                query.append(" and data like ?");
                paramList.add("%\"" + propertyName + "\":%");
            });
        }
        if (!Strings.isNullOrEmpty(name)) {
            query.append(" and name like ?");
            paramList.add(name);
        }
        if (!Strings.isNullOrEmpty(sortBy)) {
            query.append(" order by ").append(sortBy);
            if (!Strings.isNullOrEmpty(sortOrder)) {
                query.append(" ").append(sortOrder);
            }
        }
        if (limit != null) {
            query.append(" limit ");
            if (offset != null) {
                query.append(offset).append(",");
            }
            query.append(limit);
        }
        final Object[] params = new Object[paramList.size()];
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
            jdbcUtil.getJdbcTemplate().query(query.toString(), paramList.toArray(params), handler);
        } catch (DataAccessException e) {
            log.error("DataAccessException exception", e);
            throw new UserMetadataServiceException("Failed to get definition data", e);
        }
        return result;
    }

    @Override
    @Transactional(readOnly = true)
    public List<QualifiedName> searchByOwners(final Set<String> owners) {
        final List<QualifiedName> result = Lists.newArrayList();
        final StringBuilder query = new StringBuilder(SQL.SEARCH_DEFINITION_METADATA_NAMES);
        final List<Object> paramList = Lists.newArrayList();
        query.append(" where 1=0");
        owners.forEach(s -> {
            query.append(" or data like ?");
            paramList.add("%\"userId\":\"" + s.trim() + "\"%");
        });
        final Object[] params = new Object[paramList.size()];
        try {
            // Handler for reading the result set
            final ResultSetExtractor<Void> handler = rs -> {
                while (rs.next()) {
                    final String definitionName = rs.getString("name");
                    result.add(QualifiedName.fromString(definitionName, false));
                }
                return null;
            };
            jdbcUtil.getJdbcTemplate().query(query.toString(), paramList.toArray(params), handler);
        } catch (DataAccessException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException("Failed to get definition data", e);
        }
        return result;

    }

    @Override
    @Transactional(readOnly = true)
    public List<String> getDeletedDataMetadataUris(final Date deletedPriorTo, final Integer offset,
                                                   final Integer limit) {
        try {
            return jdbcUtil.getJdbcTemplate().query(String.format(SQL.GET_DELETED_DATA_METADATA_URI, offset, limit),
                new Object[]{deletedPriorTo}, (rs, rowNum) -> rs.getString("uri"));
        } catch (DataAccessException e) {
            log.error("DataAccessException exception", e);
            throw new UserMetadataServiceException(
                String.format("Failed to get deleted data metadata uris deleted prior to %s", deletedPriorTo), e);
        }
    }

    private static class SQL {
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
        static final String GET_DATA_METADATA =
            "select uri name, data from data_metadata where uri=?";
        static final String GET_DELETED_DATA_METADATA_URI =
            "select uri from data_metadata_delete dmd join data_metadata dm on dmd.id=dm.id"
                + " where dmd.date_created < ? limit %d,%d";
        static final String GET_DESCENDANT_DATA_URIS =
            "select uri from data_metadata where uri like ?";
        static final String GET_DESCENDANT_DEFINITION_NAMES =
            "select name from definition_metadata where name like ?";
        static final String GET_DATA_METADATAS =
            "select uri name,data from data_metadata where uri in (%s)";
        static final String GET_DEFINITION_METADATA =
            "select name, data from definition_metadata where name=?";
        static final String GET_DEFINITION_METADATAS =
            "select name,data from definition_metadata where name in (%s)";
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
        static final String RENAME_DATA_METADATA = "update data_metadata set uri=? where uri=?";
        static final String RENAME_DEFINITION_METADATA = "update definition_metadata set name=? where name=?";
        static final String UPDATE_DATA_METADATA =
            "update data_metadata set data=?, last_updated=now(), last_updated_by=? where uri=?";
        static final String UPDATE_DEFINITION_METADATA =
            "update definition_metadata set data=?, last_updated=now(), last_updated_by=? where name=?";
    }
}
