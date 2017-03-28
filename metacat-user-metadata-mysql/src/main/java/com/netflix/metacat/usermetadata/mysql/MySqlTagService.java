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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.server.model.Lookup;
import com.netflix.metacat.common.server.model.TagItem;
import com.netflix.metacat.common.server.Config;
import com.netflix.metacat.common.server.usermetadata.LookupService;
import com.netflix.metacat.common.server.usermetadata.TagService;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.common.server.usermetadata.UserMetadataServiceException;
import com.netflix.metacat.common.server.util.DBUtil;
import com.netflix.metacat.common.server.util.DataSourceManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.ColumnListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Tag service implementation.
 */
@Slf4j
public class MySqlTagService implements TagService {
    /** Lookup name for tag. */
    public static final String LOOKUP_NAME_TAG = "tag";
    private static final String NAME_TAGS = "tags";
    private static final String QUERY_SEARCH =
        "select distinct i.name from tag_item i, tag_item_tags t where i.id=t.tag_item_id"
            + " and (1=? or t.tags_string %s ) and (1=? or i.name like ?)";
    private static final String SQL_GET_TAG_ITEM =
        "select id, name, created_by createdBy, last_updated_by lastUpdatedBy, date_created dateCreated,"
            + " last_updated lastUpdated from tag_item where name=?";
    private static final String SQL_INSERT_TAG_ITEM =
        "insert into tag_item( name, version, created_by, last_updated_by, date_created, last_updated)"
            + " values (?,0, ?,?,now(),now())";
    private static final String SQL_UPDATE_TAG_ITEM =
        "update tag_item set name=?, last_updated=now() where name=?";
    private static final String SQL_INSERT_TAG_ITEM_TAGS =
        "insert into tag_item_tags( tag_item_id, tags_string) values (?,?)";
    private static final String SQL_DELETE_TAG_ITEM =
        "delete from tag_item where name=?";
    private static final String SQL_DELETE_TAG_ITEM_TAGS_BY_NAME =
        "delete from tag_item_tags where tag_item_id=(select id from tag_item where name=?)";
    private static final String SQL_DELETE_TAG_ITEM_TAGS_BY_NAME_TAGS =
        "delete from tag_item_tags where tag_item_id=(select id from tag_item where name=?) and tags_string in (%s)";
    private static final String SQL_DELETE_TAG_ITEM_TAGS =
        "delete from tag_item_tags where tag_item_id=? and tags_string in (%s)";
    private static final String SQL_GET_TAG_ITEM_TAGS =
        "select tags_string value from tag_item_tags where tag_item_id=?";
    private static final String SQL_GET_LOOKUP_VALUES_BY_NAME =
        "select lv.tags_string value from tag_item l, tag_item_tags lv where l.id=lv.tag_item_id and l.name=?";
    private final Config config;
    private final DataSourceManager dataSourceManager;
    private final LookupService lookupService;
    private final MetacatJson metacatJson;
    private final UserMetadataService userMetadataService;

    /**
     * Constructor.
     * @param config config
     * @param dataSourceManager manager
     * @param lookupService lookup service
     * @param metacatJson json util
     * @param userMetadataService user metadata service
     */
    @Inject
    public MySqlTagService(
        final Config config,
        final DataSourceManager dataSourceManager,
        final LookupService lookupService,
        final MetacatJson metacatJson,
        final UserMetadataService userMetadataService) {
        this.config = Preconditions.checkNotNull(config, "config is required");
        this.dataSourceManager = Preconditions.checkNotNull(dataSourceManager, "dataSourceManager is required");
        this.lookupService = Preconditions.checkNotNull(lookupService, "lookupService is required");
        this.metacatJson = Preconditions.checkNotNull(metacatJson, "metacatJson is required");
        this.userMetadataService = Preconditions.checkNotNull(userMetadataService, "userMetadataService is required");
    }

    private DataSource getDataSource() {
        return dataSourceManager.get(MysqlUserMetadataService.NAME_DATASOURCE);
    }

    private Lookup addTags(final Set<String> tags) {
        try {
            return lookupService.addValues(LOOKUP_NAME_TAG, tags);
        } catch (Exception e) {
            final String message = String.format("Failed adding the tags %s", tags);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }

    }

    /**
     * Get the tag item.
     * @param name name
     * @return tag item
     */
    public TagItem get(final QualifiedName name) {
        return get(name.toString());
    }

    /**
     * Returns the TagItem for the given <code>name</code>.
     * @param name tag name
     * @return TagItem
     */
    public TagItem get(final String name) {
        TagItem result = null;
        final Connection connection = DBUtil.getReadConnection(getDataSource());
        try {
            final ResultSetHandler<TagItem> handler = new BeanHandler<>(TagItem.class);
            result = new QueryRunner().query(connection, SQL_GET_TAG_ITEM, handler, name);
            if (result != null) {
                result.setValues(getValues(result.getId()));
            }
        } catch (Exception e) {
            final String message = String.format("Failed to get the tag item for name %s", name);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        } finally {
            DBUtil.closeReadConnection(connection);
        }
        return result;
    }

    /**
     * Returns the list of tags of the tag item id.
     * @param tagItemId tag item id
     * @return list of tags
     */
    public Set<String> getValues(final Long tagItemId) {
        final Connection connection = DBUtil.getReadConnection(getDataSource());
        try {
            return new QueryRunner().query(connection, SQL_GET_TAG_ITEM_TAGS, rs -> {
                final Set<String> result = Sets.newHashSet();
                while (rs.next()) {
                    result.add(rs.getString("value"));
                }
                return result;
            }, tagItemId);
        } catch (Exception e) {
            final String message = String.format("Failed to get the tags for id %s", tagItemId);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        } finally {
            DBUtil.closeReadConnection(connection);
        }
    }

    private TagItem findOrCreateTagItemByName(final String name, final Connection conn) throws SQLException {
        TagItem result = get(name);
        if (result == null) {
            final Object[] params = {name, config.getTagServiceUserAdmin(), config.getTagServiceUserAdmin() };
            final Long id = new QueryRunner().insert(conn, SQL_INSERT_TAG_ITEM, new ScalarHandler<>(1), params);
            result = new TagItem();
            result.setName(name);
            result.setId(id);
        }
        return result;
    }

    @Override
    public Void rename(final QualifiedName name, final String newTableName) {
        try {
            final Connection conn = getDataSource().getConnection();
            try {
                final QualifiedName newName = QualifiedName.ofTable(name.getCatalogName(), name.getDatabaseName(),
                    newTableName);
                new QueryRunner().update(conn, SQL_UPDATE_TAG_ITEM, newName.toString(), name.toString());
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            final String message = String.format("Failed to rename item name %s", name);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
        return null;
    }

    @Override
    public Void delete(final QualifiedName name, final boolean updateUserMetadata) {
        try {
            final Connection conn = getDataSource().getConnection();
            try {
                new QueryRunner().update(conn, SQL_DELETE_TAG_ITEM_TAGS_BY_NAME, name.toString());
                new QueryRunner().update(conn, SQL_DELETE_TAG_ITEM, name.toString());
                if (updateUserMetadata) {
                    // Set the tags in user metadata
                    final Map<String, Set<String>> data = Maps.newHashMap();
                    data.put(NAME_TAGS, Sets.newHashSet());
                    userMetadataService
                        .saveDefinitionMetadata(name, "admin", Optional.of(metacatJson.toJsonObject(data)),
                            true);
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            final String message = String.format("Failed to delete all tags for name %s", name);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
        return null;
    }

    private void remove(final QualifiedName name, final Set<String> tags, final boolean updateUserMetadata) {
        try {
            final Connection conn = getDataSource().getConnection();
            try {
                remove(conn, name, tags, updateUserMetadata);
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            final String message = String.format("Failed to remove tags for name %s", name);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
    }

    private void remove(final Connection conn, final QualifiedName name, final Set<String> tags,
        final boolean updateUserMetadata)
        throws SQLException {
        new QueryRunner().update(conn,
            String.format(SQL_DELETE_TAG_ITEM_TAGS_BY_NAME_TAGS, "'" + Joiner.on("','").skipNulls().join(tags) + "'"),
            name.toString());
        if (updateUserMetadata) {
            final TagItem tagItem = get(name);
            tagItem.getValues().removeAll(tags);
            final Map<String, Set<String>> data = Maps.newHashMap();
            data.put(NAME_TAGS, tagItem.getValues());
            userMetadataService
                .saveDefinitionMetadata(name, "admin", Optional.of(metacatJson.toJsonObject(data)),
                    true);
        }
    }

    /**
     * Returns the list of tags.
     * @return list of tag names
     */
    @Override
    public Set<String> getTags() {
        return lookupService.getValues(LOOKUP_NAME_TAG);
    }

    /**
     * Returns the list of <code>QualifiedName</code> of items that are tagged by the
     * given <code>includeTags</code> and do not contain the given <code>excludeTags</code>.
     * @param includeTags include items that contain tags
     * @param excludeTags include items that do not contain tags
     * @param sourceName catalog/source name
     * @param databaseName database name
     * @param tableName table name
     * @return list of qualified names of the items
     */
    @Override
    public List<QualifiedName> list(final Set<String> includeTags, final Set<String> excludeTags,
        final String sourceName,
        final String databaseName, final String tableName) {
        Set<String> includedNames = Sets.newHashSet();
        final Set<String> excludedNames = Sets.newHashSet();
        final Connection connection = DBUtil.getReadConnection(getDataSource());
        try {
            final QueryRunner runner = new QueryRunner();
            final String wildCardName = QualifiedName.toWildCardString(sourceName, databaseName, tableName);
            //Includes
            String query = String.format(QUERY_SEARCH, "in ('" + Joiner.on("','").skipNulls().join(includeTags) + "')");
            final Object[] params = {includeTags.size() == 0 ? 1 : 0, wildCardName == null ? 1 : 0, wildCardName };
            includedNames.addAll(runner.query(connection, query, new ColumnListHandler<>("name"), params));
            if (excludeTags != null && !excludeTags.isEmpty()) {
                //Excludes
                query = String.format(QUERY_SEARCH, "in ('" + Joiner.on("','").skipNulls().join(excludeTags) + "')");
                final Object[] eParams = {excludeTags.size() == 0 ? 1 : 0, wildCardName == null ? 1 : 0, wildCardName };
                excludedNames.addAll(runner.query(connection, query, new ColumnListHandler<>("name"), eParams));
            }
        } catch (SQLException e) {
            final String message = String.format("Failed getting the list of qualified names for tags %s", includeTags);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        } finally {
            DBUtil.closeReadConnection(connection);
        }

        if (excludeTags != null && !excludeTags.isEmpty()) {
            includedNames = Sets.difference(includedNames, excludedNames);
        }

        return includedNames.stream().map(s -> QualifiedName.fromString(s, false)).collect(Collectors.toList());
    }

    /**
     * Returns the list of <code>QualifiedName</code> of items that have tags containing the given tag text.
     * @param tag partial text of a tag
     * @param sourceName source/catalog name
     * @param databaseName database name
     * @param tableName table name
     * @return list of qualified names of the items
     */
    @Override
    public List<QualifiedName> search(final String tag, final String sourceName, final String databaseName,
        final String tableName) {
        final Connection connection = DBUtil.getReadConnection(getDataSource());
        try {
            final String wildCardName = QualifiedName.toWildCardString(sourceName, databaseName, tableName);
            //Includes
            final String query = String.format(QUERY_SEARCH, "like ?");
            final Object[] params = {tag == null ? 1 : 0, tag + "%", wildCardName == null ? 1 : 0, wildCardName };
            return new QueryRunner().query(connection, query, new ColumnListHandler<>("name"), params);
        } catch (SQLException e) {
            final String message = String.format("Failed getting the list of qualified names for tag %s", tag);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        } finally {
            DBUtil.closeReadConnection(connection);
        }
    }

    /**
     * Tags the given table with the given <code>tags</code>.
     * @param name table name
     * @param tags list of tags
     * @return return the complete list of tags associated with the table
     */
    @Override
    public Set<String> setTableTags(final QualifiedName name, final Set<String> tags,
        final boolean updateUserMetadata) {
        addTags(tags);
        try {
            final Connection conn = getDataSource().getConnection();
            try {
                final TagItem tagItem = findOrCreateTagItemByName(name.toString(), conn);
                Set<String> inserts = Sets.newHashSet();
                Set<String> deletes = Sets.newHashSet();
                Set<String> values = tagItem.getValues();
                if (values == null || values.isEmpty()) {
                    inserts = tags;
                } else {
                    inserts = Sets.difference(tags, values).immutableCopy();
                    deletes = Sets.difference(values, tags).immutableCopy();
                }
                values = tags;
                if (!inserts.isEmpty()) {
                    insertTagItemTags(tagItem.getId(), inserts, conn);
                }
                if (!deletes.isEmpty()) {
                    removeTagItemTags(tagItem.getId(), deletes, conn);
                }
                if (updateUserMetadata) {
                    // Set the tags in user metadata
                    final Map<String, Set<String>> data = Maps.newHashMap();
                    data.put(NAME_TAGS, values);
                    userMetadataService
                        .saveDefinitionMetadata(name, "admin", Optional.of(metacatJson.toJsonObject(data)),
                            true);
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            final String message = String.format("Failed to remove tags for name %s", name);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
        return tags;
    }

    private void removeTagItemTags(final Long id, final Set<String> tags, final Connection conn) throws SQLException {
        new QueryRunner()
            .update(conn, String.format(SQL_DELETE_TAG_ITEM_TAGS, "'" + Joiner.on("','").skipNulls().join(tags) + "'"),
                id);
    }

    private void insertTagItemTags(final Long id, final Set<String> tags, final Connection conn) throws SQLException {
        final Object[][] params = new Object[tags.size()][];
        final Iterator<String> iter = tags.iterator();
        int index = 0;
        while (iter.hasNext()) {
            params[index++] = ImmutableList.of(id, iter.next()).toArray();
        }
        new QueryRunner().batch(conn, SQL_INSERT_TAG_ITEM_TAGS, params);
    }

    /**
     * Removes the tags from the given table.
     * @param name table name
     * @param deleteAll if true, will delete all tags associated with the given table
     * @param tags list of tags to be removed for the given table
     */
    @Override
    public Void removeTableTags(final QualifiedName name, final Boolean deleteAll,
        final Set<String> tags, final boolean updateUserMetadata) {
        if (deleteAll != null && deleteAll) {
            delete(name, updateUserMetadata);
        } else {
            remove(name, tags, updateUserMetadata);
        }
        return null;
    }
}
