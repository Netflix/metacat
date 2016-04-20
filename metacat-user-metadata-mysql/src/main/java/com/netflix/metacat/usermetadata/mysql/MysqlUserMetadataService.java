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
import com.netflix.metacat.common.usermetadata.BaseUserMetadataService;
import com.netflix.metacat.common.usermetadata.UserMetadataServiceException;
import com.netflix.metacat.common.util.DataSourceManager;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

public class MysqlUserMetadataService extends BaseUserMetadataService {
    private static final Logger log = LoggerFactory.getLogger(MysqlUserMetadataService.class);
    public static final String NAME_DATASOURCE = "metacat-usermetadata";
    private static final int MAX_IN_CLAUSE_ITEMS = 5000;
    private Properties connectionProperties;
    private final DataSourceManager dataSourceManager;
    private final MetacatJson metacatJson;
    private DataSource poolingDataSource = null;

    @Inject
    public MysqlUserMetadataService(DataSourceManager dataSourceManager, MetacatJson metacatJson) {
        this.dataSourceManager = dataSourceManager;
        this.metacatJson = metacatJson;
    }

    @Override
    public void deleteDataMetadatas(@Nonnull List<String> uris) {
        try{
            Connection conn = poolingDataSource.getConnection();
            try {
                List<List<String>> subLists = Lists.partition(uris, 5000);
                for (List<String> subUris : subLists) {
                    _deleteDataMetadatas(conn, subUris);
                }
                conn.commit();
            } catch( SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.close();
            }
        }catch (SQLException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException(String.format("Failed deleting the data metadata for %s", uris), e);
        }
    }

    @Override
    public void deleteDefinitionMetadatas(@Nonnull List<QualifiedName> names) {
        try{
            Connection conn = poolingDataSource.getConnection();
            try {
                List<List<QualifiedName>> subLists = Lists.partition( names, 5000);
                for(List<QualifiedName> subNames: subLists) {
                    _deleteDefinitionMetadatas(conn, subNames);
                }
                conn.commit();
            } catch( SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.close();
            }
        }catch (SQLException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException(String.format("Failed deleting the definition metadata for %s", names), e);
        }
    }

    @Override
    public void deleteMetadatas(List<HasMetadata> holders, boolean force) {
        try{
            Connection conn = poolingDataSource.getConnection();
            try {
                List<List<HasMetadata>> subLists = Lists.partition( holders, 5000);
                for(List<HasMetadata> hasMetadatas: subLists) {
                    List<String> uris = Lists.newArrayList();
                    List<QualifiedName> names = Lists.newArrayList();
                    hasMetadatas.stream().forEach(hasMetadata -> {
                        if( hasMetadata instanceof HasDefinitionMetadata){
                            names.add(((HasDefinitionMetadata) hasMetadata).getDefinitionName());
                        }
                        if( hasMetadata instanceof HasDataMetadata){
                            HasDataMetadata dataMetadataHolder = (HasDataMetadata) hasMetadata;
                            if(dataMetadataHolder.isDataExternal()) {
                                uris.add(((HasDataMetadata) hasMetadata).getDataUri());
                            }
                        }
                    });
                    if( !names.isEmpty()) {
                        _deleteDefinitionMetadatas(conn, names);
                    }
                    // TODO check that data uris are not used by another entity so we can delete them
                    if( force){
                        _deleteDataMetadatas(conn, uris);
                    }
                }
                conn.commit();
            } catch( SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.close();
            }
        }catch (SQLException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException("Failed deleting data metadata", e);
        }
    }

    private Void _deleteDefinitionMetadatas(Connection conn, List<QualifiedName> names) throws SQLException {
        if( names != null && !names.isEmpty()) {
            List<String> paramVariables = names.stream().map(s -> "?").collect(Collectors.toList());
            String[] aNames = names.stream().map(QualifiedName::toString).toArray(String[]::new);
            new QueryRunner().update(conn, String.format(SQL.DELETE_DEFINITION_METADATA, Joiner.on(",").skipNulls().join(paramVariables)), (Object[]) aNames);
        }
        return null;
    }

    private Void _deleteDataMetadatas(Connection conn, List<String> uris) throws SQLException {
        if( uris != null && !uris.isEmpty()) {
            List<String> paramVariables = uris.stream().map(s -> "?").collect(Collectors.toList());
            String[] aUris = uris.stream().toArray(String[]::new);
            new QueryRunner().update(conn,
                    String.format(SQL.DELETE_DATA_METADATA, Joiner.on(",").skipNulls().join(paramVariables)), (Object[]) aUris);
        }
        return null;
    }

    @Nonnull
    @Override
    public Optional<ObjectNode> getDataMetadata(@Nonnull String uri) {
        return getJsonForKey(SQL.GET_DATA_METADATA, uri);
    }

    @Nonnull
    @Override
    public Map<String, ObjectNode> getDataMetadataMap(
            @Nonnull
            List<String> uris) {
        Map<String,ObjectNode> result = Maps.newHashMap();
        if( !uris.isEmpty()) {
            List<List<String>> parts = Lists.partition(uris, MAX_IN_CLAUSE_ITEMS);
            parts.stream().forEach(keys -> result.putAll(_getMetadataMap(keys, SQL.GET_DATA_METADATAS)));
        }
        return result;
    }

    @Nonnull
    @Override
    public Optional<ObjectNode> getDefinitionMetadata(@Nonnull QualifiedName name) {
        return getJsonForKey(SQL.GET_DEFINITION_METADATA, name.toString());
    }

    @Nonnull
    @Override
    public Map<String, ObjectNode> getDefinitionMetadataMap(@Nonnull List<QualifiedName> names) {
        if (!names.isEmpty()) {
            List<List<QualifiedName>> parts = Lists.partition(names, MAX_IN_CLAUSE_ITEMS);
            return parts.stream()
                    .map(keys -> _getMetadataMap(keys, SQL.GET_DEFINITION_METADATAS))
                    .flatMap(it -> it.entrySet().stream())
                    .collect(Collectors.toMap(it -> QualifiedName.fromString(it.getKey()).toString(),
                                    Map.Entry::getValue));
        } else {
            return Collections.emptyMap();
        }
    }

    private Map<String,ObjectNode> _getMetadataMap(List<?> keys, String sql) {
        Map<String,ObjectNode> result = Maps.newHashMap();
        if( keys == null || keys.isEmpty()){
            return result;
        }
        List<String> paramVariables = keys.stream().map(s -> "?").collect(Collectors.toList());
        String[] aKeys = keys.stream().map(Object::toString).toArray(String[]::new);
        String query = String.format(sql, Joiner.on(",").join(paramVariables));
        try (Connection connection = poolingDataSource.getConnection()) {
            ResultSetHandler<Void> handler = resultSet -> {
                while(resultSet.next()) {
                    String json = resultSet.getString("data");
                    String name = resultSet.getString("name");
                    if (json != null) {
                        try {
                            result.put(name, metacatJson.parseJsonObject(json));
                        } catch (MetacatJsonException e) {
                            log.error("Invalid json '{}' for name '{}'", json, name);
                            throw new UserMetadataServiceException(String.format("Invalid json %s for name %s", json, name), e);
                        }
                    }
                }
                return null;
            };
            new QueryRunner().query( connection, query, handler, (Object[]) aKeys);
        } catch (SQLException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException(String.format("Failed to get data for %s", keys), e);
        }
        return result;
    }

    protected Optional<ObjectNode> getJsonForKey(String query, String keyValue) {
        String json = null;
        try (Connection connection = poolingDataSource.getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setString(1, keyValue);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }

                json = resultSet.getString(1);
                if (Strings.isNullOrEmpty(json)) {
                    return Optional.empty();
                }

                return Optional.ofNullable(metacatJson.parseJsonObject(json));
            }
        } catch (SQLException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException(String.format("Failed to get data for %s", keyValue), e);
        } catch (MetacatJsonException e) {
            log.error("Invalid json '{}' for keyValue '{}'", json, keyValue, e);
            throw new UserMetadataServiceException(String.format("Invalid json %s for name %s", json, keyValue), e);
        }
    }

    protected int executeUpdateForKey(String query, String... keyValues) {
        int result = 0;
        try{
            Connection conn = poolingDataSource.getConnection();
            try {
                result =  new QueryRunner().update(conn, query, (Object[]) keyValues);
                conn.commit();
            } catch( SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.close();
            }
        }catch (SQLException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException(String.format("Failed to save data for %s", Arrays.toString(keyValues)), e);
        }
        return result;
    }

    @Override
    public void saveDataMetadata(@Nonnull String uri, @Nonnull String userId,
            @Nonnull Optional<ObjectNode> metadata, boolean merge) {
        Optional<ObjectNode> existingData = getDataMetadata(uri);
        int count;
        if (existingData.isPresent() && metadata.isPresent()) {
            ObjectNode merged = existingData.get();
            if( merge) {
                metacatJson.mergeIntoPrimary(merged, metadata.get());
            }
            count = executeUpdateForKey(SQL.UPDATE_DATA_METADATA, merged.toString(), userId, uri);
        }  else if (metadata.isPresent()) {
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
    public void saveDefinitionMetadata(@Nonnull QualifiedName name, @Nonnull String userId,
            @Nonnull Optional<ObjectNode> metadata, boolean merge) {
        Optional<ObjectNode> existingData = getDefinitionMetadata(name);
        int count;
        if (existingData.isPresent() && metadata.isPresent()) {
            ObjectNode merged = existingData.get();
            if( merge) {
                metacatJson.mergeIntoPrimary(merged, metadata.get());
            }
            count = executeUpdateForKey(SQL.UPDATE_DEFINITION_METADATA, merged.toString(), userId, name.toString());
        }  else if (metadata.isPresent()) {
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
    public int renameDataMetadataKey(@Nonnull String oldUri, @Nonnull String newUri) {
        return executeUpdateForKey(SQL.RENAME_DATA_METADATA, newUri, oldUri);
    }

    @Override
    public int renameDefinitionMetadataKey(@Nonnull QualifiedName oldName, @Nonnull QualifiedName newName) {
        return executeUpdateForKey(SQL.RENAME_DEFINITION_METADATA, newName.toString(), oldName.toString());
    }

    @Override
    public void saveMetadatas(String user, List<? extends HasMetadata> metadatas, boolean merge) {
        try{
            Connection conn = poolingDataSource.getConnection();
            try {
                @SuppressWarnings("unchecked")
                List<List<HasMetadata>> subLists = Lists.partition((List<HasMetadata>) metadatas, 5000);
                for (List<HasMetadata> hasMetadatas : subLists) {
                    List<String> uris = Lists.newArrayList();
                    List<QualifiedName> names = Lists.newArrayList();
                    // Get the names and uris
                    List<HasDefinitionMetadata> definitionMetadatas = Lists.newArrayList();
                    List<HasDataMetadata> dataMetadatas = Lists.newArrayList();
                    hasMetadatas.stream().forEach(hasMetadata -> {
                        if( hasMetadata instanceof HasDefinitionMetadata){
                            HasDefinitionMetadata oDef = (HasDefinitionMetadata)hasMetadata;
                            names.add(oDef.getDefinitionName());
                            if(oDef.getDefinitionMetadata() != null){
                                definitionMetadatas.add(oDef);
                            }
                        }
                        if( hasMetadata instanceof HasDataMetadata){
                            HasDataMetadata oData = (HasDataMetadata)hasMetadata;
                            if (oData.isDataExternal() && oData.getDataMetadata() != null
                                    && oData.getDataMetadata().size() > 0) {
                                uris.add(oData.getDataUri());
                                dataMetadatas.add(oData);
                            }
                        }
                    });
                    if( !definitionMetadatas.isEmpty() || !dataMetadatas.isEmpty()) {
                        // Get the existing metadata based on the names and uris
                        Map<String, ObjectNode> definitionMap = getDefinitionMetadataMap(names);
                        Map<String, ObjectNode> dataMap = getDataMetadataMap(uris);
                        // Curate the list of existing and new metadatas
                        List<Object[]> insertDefinitionMetadatas = Lists.newArrayList();
                        List<Object[]> updateDefinitionMetadatas = Lists.newArrayList();
                        List<Object[]> insertDataMetadatas = Lists.newArrayList();
                        List<Object[]> updateDataMetadatas = Lists.newArrayList();
                        definitionMetadatas.stream().forEach(oDef -> {
                            QualifiedName qualifiedName = oDef.getDefinitionName();
                            if (qualifiedName != null && oDef.getDefinitionMetadata() != null && oDef.getDefinitionMetadata().size() != 0) {
                                String name = qualifiedName.toString();
                                ObjectNode oNode = definitionMap.get(name);
                                if (oNode == null) {
                                    insertDefinitionMetadatas
                                            .add(new Object[] { metacatJson.toJsonString(oDef.getDefinitionMetadata()), user, user, name });
                                } else {
                                    metacatJson.mergeIntoPrimary(oNode, oDef.getDefinitionMetadata());
                                    updateDefinitionMetadatas
                                            .add(new Object[] { metacatJson.toJsonString(oNode), user, name });
                                }
                            }
                        });
                        dataMetadatas.stream().forEach(oData -> {
                            String uri = oData.getDataUri();
                            ObjectNode oNode = dataMap.get(uri);
                            if( oData.getDataMetadata() != null && oData.getDataMetadata().size() != 0){
                                if (oNode == null) {
                                    insertDataMetadatas.add(new Object[] { metacatJson.toJsonString(oData.getDataMetadata()), user, user, uri });
                                } else {
                                    metacatJson.mergeIntoPrimary(oNode, oData.getDataMetadata());
                                    updateDataMetadatas.add(new Object[] { metacatJson.toJsonString(oNode), user, uri });
                                }
                            }
                        });
                        //Now run the queries
                        QueryRunner runner = new QueryRunner();
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
            } catch( SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.close();
            }
        }catch (SQLException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException("Failed to save metadata", e);
        }
    }

    @Override
    public List<DefinitionMetadataDto> searchDefinitionMetadatas(Set<String> propertyNames, String type, String name,
            String sortBy, String sortOrder, Integer offset, Integer limit) {
        List<DefinitionMetadataDto> result = Lists.newArrayList();
        StringBuilder query = new StringBuilder(SQL.SEARCH_DEFINITION_METADATAS);
        List<Object> paramList = Lists.newArrayList();
        if( type != null) {
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
            }
            if (typeRegex != null) {
                query.append(" and name rlike ?");
                paramList.add(typeRegex);
            }
        }
        if(propertyNames != null && !propertyNames.isEmpty()){
            propertyNames.forEach( propertyName -> {
                query.append(" and data like ?");
                paramList.add( "%\"" + propertyName + "\":%");
            });
        }
        if( !Strings.isNullOrEmpty(name)){
            query.append(" and name like ?");
            paramList.add(name);
        }
        if( !Strings.isNullOrEmpty(sortBy)){
            query.append(" order by " ).append(sortBy);
            if(!Strings.isNullOrEmpty(sortOrder)){
                query.append(" ").append( sortOrder);
            }
        }
        if( limit != null){
            query.append(" limit ");
            if( offset != null){
                query.append(offset).append(",");
            }
            query.append( limit);
        }
        Object[] params = new Object[paramList.size()];
        try (Connection connection = poolingDataSource.getConnection()) {
            // Handler for reading the result set
            ResultSetHandler<Void> handler = rs -> {
                while (rs.next()) {
                    String definitionName = rs.getString("name");
                    String data = rs.getString("data");
                    DefinitionMetadataDto definitionMetadataDto = new DefinitionMetadataDto();
                    definitionMetadataDto.setName(QualifiedName.fromString(definitionName));
                    definitionMetadataDto.setDefinitionMetadata(metacatJson.parseJsonObject(data));
                    result.add(definitionMetadataDto);
                }
                return null;
            };
            new QueryRunner().query( connection, query.toString(), handler, paramList.toArray(params));
        } catch (SQLException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException("Failed to get definition data", e);
        }
        return result;
    }

    @Override
    public List<QualifiedName> searchByOwners(Set<String> owners) {
        List<QualifiedName> result = Lists.newArrayList();
        StringBuilder query = new StringBuilder(SQL.SEARCH_DEFINITION_METADATA_NAMES);
        List<Object> paramList = Lists.newArrayList();
        query.append(" where 1=0");
        owners.forEach(s -> {
            query.append(" or data like ?");
            paramList.add( "%\"userId\":\"" + s.trim() + "\"%");
        });
        Object[] params = new Object[paramList.size()];
        try (Connection connection = poolingDataSource.getConnection()) {
            // Handler for reading the result set
            ResultSetHandler<Void> handler = rs -> {
                while (rs.next()) {
                    String definitionName = rs.getString("name");
                    result.add(QualifiedName.fromString(definitionName, false));
                }
                return null;
            };
            new QueryRunner().query( connection, query.toString(), handler, paramList.toArray(params));
        } catch (SQLException e) {
            log.error("Sql exception", e);
            throw new UserMetadataServiceException("Failed to get definition data", e);
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

    private void initProperties() throws Exception{
        String configLocation = System.getProperty(METACAT_USERMETADATA_CONFIG_LOCATION, "usermetadata.properties");
        URL url = Thread.currentThread().getContextClassLoader().getResource(configLocation);
        Path filePath;
        if( url != null){
            filePath = Paths.get(url.toURI());
        } else {
            filePath = FileSystems.getDefault().getPath(configLocation);
        }
        checkState(filePath!=null, "Unable to read from user metadata config file '%s'", configLocation);

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
        public static final String DELETE_DATA_METADATA = "delete from data_metadata where uri in (%s)";
        public static final String DELETE_DEFINITION_METADATA = "delete from definition_metadata where name in (%s)";
        public static final String GET_DATA_METADATA = "select data from data_metadata where uri=?";
        public static final String GET_DATA_METADATAS = "select uri name,data from data_metadata where uri in (%s)";
        public static final String GET_DEFINITION_METADATA = "select data from definition_metadata where name=?";
        public static final String GET_DEFINITION_METADATAS = "select name,data from definition_metadata where name in (%s)";
        public static final String SEARCH_DEFINITION_METADATAS = "select name,data from definition_metadata where 1=1";
        public static final String SEARCH_DEFINITION_METADATA_NAMES = "select name from definition_metadata";
        public static final String INSERT_DATA_METADATA = "insert into data_metadata " +
                "(data, created_by, last_updated_by, date_created, last_updated, version, uri) values " +
                "(?, ?, ?, now(), now(), 0, ?)";
        public static final String INSERT_DEFINITION_METADATA = "insert into definition_metadata " +
                "(data, created_by, last_updated_by, date_created, last_updated, version, name) values " +
                "(?, ?, ?, now(), now(), 0, ?)";
        public static final String RENAME_DATA_METADATA = "update data_metadata set uri=? where uri=?";
        public static final String RENAME_DEFINITION_METADATA = "update definition_metadata set name=? where name=?";
        public static final String UPDATE_DATA_METADATA = "update data_metadata " +
                "set data=?, last_updated=now(), last_updated_by=? where uri=?";
        public static final String UPDATE_DEFINITION_METADATA = "update definition_metadata " +
                "set data=?, last_updated=now(), last_updated_by=? where name=?";
    }
}
