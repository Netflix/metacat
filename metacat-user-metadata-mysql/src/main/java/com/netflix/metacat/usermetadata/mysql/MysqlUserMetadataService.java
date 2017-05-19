/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.usermetadata.mysql;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
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
import com.netflix.metacat.common.server.util.DBUtil;
import com.netflix.metacat.common.server.util.DataSourceManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.ColumnListHandler;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.io.Reader;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * User metadata service.
 */
@Slf4j
public class MysqlUserMetadataService extends BaseUserMetadataService {
    /**
     * Data source name used by user metadata service.
     */
    public static final String NAME_DATASOURCE = "metacat-usermetadata";
    private final DataSourceManager dataSourceManager;
    private final MetacatJson metacatJson;
    private final Config config;
    private Properties connectionProperties;
    private DataSource poolingDataSource;

    /**
     * Constructor.
     *
     * @param dataSourceManager data source manager
     * @param metacatJson       json utility
     * @param config            config
     */
    public MysqlUserMetadataService(
        final DataSourceManager dataSourceManager,
        final MetacatJson metacatJson,
        final Config config
    ) {
        this.dataSourceManager = dataSourceManager;
        this.metacatJson = metacatJson;
        this.config = config;
    }

    @Override
    public void softDeleteDataMetadatas(
        final String user,
        @Nonnull final List<String> uris
    ) {
        try {
            final Connection conn = poolingDataSource.getConnection();
            try {
                final List<List<String>> subLists = Lists.partition(uris, config.getUserMetadataMaxInClauseItems());
                for (List<String> subUris : subLists) {
                    _softDeleteDataMetadatas(conn, user, subUris);
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            log.error("Sql exception", e);
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
            final Connection conn = poolingDataSource.getConnection();
            try {
                final List<List<String>> subLists = Lists.partition(uris, config.getUserMetadataMaxInClauseItems());
                for (List<String> subUris : subLists) {
                    _deleteDataMetadatas(conn, subUris, removeDataMetadata);
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException(String.format("Failed deleting the data metadata for %s", uris), e);
        }
    }

    @Override
    public void deleteDefinitionMetadatas(
        @Nonnull final List<QualifiedName> names
    ) {
        try {
            final Connection conn = poolingDataSource.getConnection();
            try {
                final List<List<QualifiedName>> subLists =
                    Lists.partition(names, config.getUserMetadataMaxInClauseItems());
                for (List<QualifiedName> subNames : subLists) {
                    _deleteDefinitionMetadatas(conn, subNames);
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException(
                String.format("Failed deleting the definition metadata for %s", names), e);
        }
    }

    @Override
    public void deleteMetadatas(final String userId, final List<HasMetadata> holders) {
        try {
            final Connection conn = poolingDataSource.getConnection();
            try {
                final List<List<HasMetadata>> subLists =
                    Lists.partition(holders, config.getUserMetadataMaxInClauseItems());
                for (List<HasMetadata> hasMetadatas : subLists) {
                    final List<QualifiedName> names = hasMetadatas.stream()
                        .filter(m -> m instanceof HasDefinitionMetadata)
                        .map(m -> ((HasDefinitionMetadata) m).getDefinitionName())
                        .collect(Collectors.toList());
                    if (!names.isEmpty()) {
                        _deleteDefinitionMetadatas(conn, names);
                    }
                    if (config.canSoftDeleteDataMetadata()) {
                        final List<String> uris = hasMetadatas.stream()
                            .filter(m -> m instanceof HasDataMetadata && ((HasDataMetadata) m).isDataExternal())
                            .map(m -> ((HasDataMetadata) m).getDataUri()).collect(Collectors.toList());
                        if (!uris.isEmpty()) {
                            _softDeleteDataMetadatas(conn, userId, uris);
                        }
                    }
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException("Failed deleting data metadata", e);
        }
    }

    @SuppressWarnings("checkstyle:methodname")
    private Void _deleteDefinitionMetadatas(
        final Connection conn,
        @Nullable final List<QualifiedName> names
    ) throws SQLException {
        if (names != null && !names.isEmpty()) {
            final List<String> paramVariables = names.stream().map(s -> "?").collect(Collectors.toList());
            final String[] aNames = names.stream().map(QualifiedName::toString).toArray(String[]::new);
            new QueryRunner().update(conn,
                String.format(SQL.DELETE_DEFINITION_METADATA, Joiner.on(",").skipNulls().join(paramVariables)),
                (Object[]) aNames);
        }
        return null;
    }

    @SuppressWarnings("checkstyle:methodname")
    private Void _softDeleteDataMetadatas(
        final Connection conn, final String userId,
        @Nullable final List<String> uris
    ) throws SQLException {
        if (uris != null && !uris.isEmpty()) {
            final List<String> paramVariables = uris.stream().map(s -> "?").collect(Collectors.toList());
            final String[] aUris = uris.stream().toArray(String[]::new);
            final String paramString = Joiner.on(",").skipNulls().join(paramVariables);
            final ColumnListHandler<Long> handler = new ColumnListHandler<>("id");
            final List<Long> ids = new QueryRunner().query(conn,
                String.format(SQL.GET_DATA_METADATA_IDS, paramString), handler, (Object[]) aUris);
            if (!ids.isEmpty()) {
                final List<String> idParamVariables = ids.stream().map(s -> "?").collect(Collectors.toList());
                final Long[] aIds = ids.stream().toArray(Long[]::new);
                final String idParamString = Joiner.on(",").skipNulls().join(idParamVariables);
                final List<Long> dupIds = new QueryRunner().query(conn,
                    String.format(SQL.GET_DATA_METADATA_DELETE_BY_IDS, idParamString), handler, (Object[]) aIds);
                if (!dupIds.isEmpty()) {
                    ids.removeAll(dupIds);
                }
                final List<Object[]> deleteDataMetadatas = Lists.newArrayList();
                ids.forEach(id -> deleteDataMetadatas.add(new Object[]{id, userId}));
                new QueryRunner().batch(conn, SQL.SOFT_DELETE_DATA_METADATA,
                    deleteDataMetadatas.toArray(new Object[deleteDataMetadatas.size()][2]));
            }
        }
        return null;
    }

    @SuppressWarnings("checkstyle:methodname")
    private Void _deleteDataMetadatas(
        final Connection conn,
        @Nullable final List<String> uris,
        final boolean removeDataMetadata
    )
        throws SQLException {
        if (uris != null && !uris.isEmpty()) {
            final List<String> paramVariables = uris.stream().map(s -> "?").collect(Collectors.toList());
            final String[] aUris = uris.stream().toArray(String[]::new);
            final String paramString = Joiner.on(",").skipNulls().join(paramVariables);
            final ColumnListHandler<Long> handler = new ColumnListHandler<>("id");
            final List<Long> ids = new QueryRunner().query(conn,
                String.format(SQL.GET_DATA_METADATA_IDS, paramString), handler, (Object[]) aUris);
            if (!ids.isEmpty()) {
                final List<String> idParamVariables = ids.stream().map(s -> "?").collect(Collectors.toList());
                final Long[] aIds = ids.stream().toArray(Long[]::new);
                final String idParamString = Joiner.on(",").skipNulls().join(idParamVariables);
                new QueryRunner().update(conn,
                    String.format(SQL.DELETE_DATA_METADATA_DELETE, idParamString), (Object[]) aIds);
                if (removeDataMetadata) {
                    new QueryRunner().update(conn,
                        String.format(SQL.DELETE_DATA_METADATA, idParamString), (Object[]) aIds);
                }
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
    public List<QualifiedName> getDescendantDefinitionNames(@Nonnull final QualifiedName name) {
        List<String> result;
        final Connection connection = DBUtil.getReadConnection(poolingDataSource);
        try {
            final ColumnListHandler<String> handler = new ColumnListHandler<>("name");
            result = new QueryRunner()
                .query(connection, SQL.GET_DESCENDANT_DEFINITION_NAMES, handler, name.toString() + "/%");
        } catch (SQLException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException(String.format("Failed to get descendant names for %s", name), e);
        } finally {
            DBUtil.closeReadConnection(connection);
        }
        return result.stream().map(QualifiedName::fromString).collect(Collectors.toList());
    }

    @Override
    public List<String> getDescendantDataUris(@Nonnull final String uri) {
        List<String> result;
        final Connection connection = DBUtil.getReadConnection(poolingDataSource);
        try {
            final ColumnListHandler<String> handler = new ColumnListHandler<>("uri");
            result = new QueryRunner().query(connection, SQL.GET_DESCENDANT_DATA_URIS, handler, uri + "/%");
        } catch (SQLException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException(String.format("Failed to get descendant uris for %s", uri), e);
        } finally {
            DBUtil.closeReadConnection(connection);
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

    @SuppressWarnings("checkstyle:methodname")
    private Map<String, ObjectNode> _getMetadataMap(@Nullable final List<?> keys, final String sql) {
        final Map<String, ObjectNode> result = Maps.newHashMap();
        if (keys == null || keys.isEmpty()) {
            return result;
        }
        final List<String> paramVariables = keys.stream().map(s -> "?").collect(Collectors.toList());
        final String[] aKeys = keys.stream().map(Object::toString).toArray(String[]::new);
        final String query = String.format(sql, Joiner.on(",").join(paramVariables));
        final Connection connection = DBUtil.getReadConnection(poolingDataSource);
        try {
            final ResultSetHandler<Void> handler = resultSet -> {
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
            new QueryRunner().query(connection, query, handler, (Object[]) aKeys);
        } catch (SQLException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException(String.format("Failed to get data for %s", keys), e);
        } finally {
            DBUtil.closeReadConnection(connection);
        }
        return result;
    }

    private Optional<ObjectNode> getJsonForKey(final String query, final String keyValue) {
        Optional<ObjectNode> result = Optional.empty();
        String json = null;
        final Connection connection = DBUtil.getReadConnection(poolingDataSource);
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setString(1, keyValue);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    final String key = resultSet.getString(1);
                    if (keyValue.equalsIgnoreCase(key)) {
                        json = resultSet.getString(2);
                        if (Strings.isNullOrEmpty(json)) {
                            return Optional.empty();
                        }
                        result = Optional.ofNullable(metacatJson.parseJsonObject(json));
                        break;
                    }
                }
            }
        } catch (SQLException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException(String.format("Failed to get data for %s", keyValue), e);
        } catch (MetacatJsonException e) {
            log.error("Invalid json '{}' for keyValue '{}'", json, keyValue, e);
            throw new UserMetadataServiceException(String.format("Invalid json %s for name %s", json, keyValue), e);
        } finally {
            DBUtil.closeReadConnection(connection);
        }
        return result;
    }

    private int executeUpdateForKey(final String query, final String... keyValues) {
        int result;
        try {
            final Connection conn = poolingDataSource.getConnection();
            try {
                result = new QueryRunner().update(conn, query, (Object[]) keyValues);
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException(
                String.format("Failed to save data for %s", Arrays.toString(keyValues)), e);
        }
        return result;
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
    public void saveMetadatas(final String user, final List<? extends HasMetadata> metadatas, final boolean merge) {
        try {
            final Connection conn = poolingDataSource.getConnection();
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
                        //Now run the queries
                        final QueryRunner runner = new QueryRunner();
                        if (!insertDefinitionMetadatas.isEmpty()) {
                            runner.batch(conn, SQL.INSERT_DEFINITION_METADATA, insertDefinitionMetadatas
                                .toArray(new Object[insertDefinitionMetadatas.size()][4]));
                        }
                        if (!updateDefinitionMetadatas.isEmpty()) {
                            runner.batch(conn, SQL.UPDATE_DEFINITION_METADATA, updateDefinitionMetadatas
                                .toArray(new Object[updateDefinitionMetadatas.size()][3]));
                        }
                        if (!insertDataMetadatas.isEmpty()) {
                            runner.batch(conn, SQL.INSERT_DATA_METADATA,
                                insertDataMetadatas.toArray(new Object[insertDataMetadatas.size()][4]));
                        }
                        if (!updateDataMetadatas.isEmpty()) {
                            runner.batch(conn, SQL.UPDATE_DATA_METADATA,
                                updateDataMetadatas.toArray(new Object[updateDataMetadatas.size()][3]));
                        }
                    }
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException("Failed to save metadata", e);
        }
    }

    @Override
    public List<DefinitionMetadataDto> searchDefinitionMetadatas(
        final Set<String> propertyNames,
        final String type,
        final String name,
        final String sortBy,
        final String sortOrder,
        final Integer offset,
        final Integer limit
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
        final Connection connection = DBUtil.getReadConnection(poolingDataSource);
        try {
            // Handler for reading the result set
            final ResultSetHandler<Void> handler = rs -> {
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
            new QueryRunner().query(connection, query.toString(), handler, paramList.toArray(params));
        } catch (SQLException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException("Failed to get definition data", e);
        } finally {
            DBUtil.closeReadConnection(connection);
        }
        return result;
    }

    @Override
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
        final Connection connection = DBUtil.getReadConnection(poolingDataSource);
        try {
            // Handler for reading the result set
            final ResultSetHandler<Void> handler = rs -> {
                while (rs.next()) {
                    final String definitionName = rs.getString("name");
                    result.add(QualifiedName.fromString(definitionName, false));
                }
                return null;
            };
            new QueryRunner().query(connection, query.toString(), handler, paramList.toArray(params));
        } catch (SQLException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException("Failed to get definition data", e);
        } finally {
            DBUtil.closeReadConnection(connection);
        }
        return result;

    }

    @Override
    public List<String> getDeletedDataMetadataUris(final Date deletedPriorTo, final Integer offset,
                                                   final Integer limit) {
        List<String> result;
        final Connection connection = DBUtil.getReadConnection(poolingDataSource);
        try {
            final ColumnListHandler<String> handler = new ColumnListHandler<>("uri");
            result = new QueryRunner().query(connection,
                String.format(SQL.GET_DELETED_DATA_METADATA_URI, offset, limit),
                handler, deletedPriorTo);
        } catch (SQLException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException(
                String.format("Failed to get deleted data metadata uris deleted prior to %s", deletedPriorTo), e);
        } finally {
            DBUtil.closeReadConnection(connection);
        }
        return result;
    }

    @Override
    public void start() throws Exception {
        //
        //Initialize properties
        initProperties();

        //
        // Finally, we create the PoolingDriver itself,
        // passing in the object pool we created.
        //
        dataSourceManager.load(NAME_DATASOURCE, connectionProperties);
        poolingDataSource = dataSourceManager.get(NAME_DATASOURCE);
    }

    private void initProperties() throws Exception {
        final String configLocation =
            System.getProperty(METACAT_USERMETADATA_CONFIG_LOCATION, "usermetadata.properties");
        final URL url = Thread.currentThread().getContextClassLoader().getResource(configLocation);
        final Path filePath;
        if (url != null) {
            filePath = Paths.get(url.toURI());
        } else {
            filePath = FileSystems.getDefault().getPath(configLocation);
        }
        Preconditions
            .checkState(filePath != null, "Unable to read from user metadata config file '%s'", configLocation);

        connectionProperties = new Properties();
        try (Reader reader = Files.newBufferedReader(filePath, Charsets.UTF_8)) {
            connectionProperties.load(reader);
        }
    }

    @Override
    public void stop() throws Exception {
        dataSourceManager.close();
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
