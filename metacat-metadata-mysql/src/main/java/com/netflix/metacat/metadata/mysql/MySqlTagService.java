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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.exception.MetacatBadRequestException;
import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.server.model.Lookup;
import com.netflix.metacat.common.server.model.TagItem;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.usermetadata.LookupService;
import com.netflix.metacat.common.server.usermetadata.TagService;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.common.server.usermetadata.UserMetadataServiceException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tag service implementation.
 *
 * @author amajumdar
 * @author zhenl
 */
@Slf4j
@SuppressFBWarnings
@Transactional("metadataTxManager")
public class MySqlTagService implements TagService {
    /**
     * Lookup name for tag.
     */
    private static final String LOOKUP_NAME_TAG = "tag";
    private static final String NAME_TAGS = "tags";
    private static final String QUERY_LIST =
        "select distinct i.name from tag_item i, tag_item_tags t where i.id=t.tag_item_id"
            + " and (1=? or t.tags_string in (%s) ) and (1=? or i.name like ?) and (1=? or i.name rlike ?)";

    private static final String QUERY_SEARCH =
        "select distinct i.name from tag_item i, tag_item_tags t where i.id=t.tag_item_id"
            + " and (1=? or t.tags_string %s ) and (1=? or i.name like ?)";
    private static final String SQL_GET_TAG_ITEM =
        "select id, name, created_by createdBy, last_updated_by lastUpdatedBy, date_created dateCreated,"
            + " last_updated lastUpdated from tag_item where name=?";
    private static final String SQL_INSERT_TAG_ITEM =
        "insert into tag_item( name, version, created_by, last_updated_by, date_created, last_updated)"
            + " values (?,0,?,?,now(),now())";
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
        "delete from tag_item_tags where tag_item_id=(?) and tags_string in (%s)";
    private static final String SQL_GET_TAG_ITEM_TAGS =
        "select tags_string value from tag_item_tags where tag_item_id=?";
    private static final String EMPTY_CLAUSE = "''";
    //    private static final String SQL_GET_LOOKUP_VALUES_BY_NAME =
//        "select lv.tags_string value from tag_item l, tag_item_tags lv where l.id=lv.tag_item_id and l.name=?";
    private static final int MAX_TAGS_LIST_COUNT = 16;
    private final Config config;
    private final LookupService lookupService;
    private final MetacatJson metacatJson;
    private final UserMetadataService userMetadataService;
    private final JdbcTemplate jdbcTemplate;
    private final JdbcTemplate jdbcTemplateLongTimeout;

    /**
     * Constructor.
     *
     * @param config              config
     * @param jdbcTemplate        JDBC template
     * @param lookupService       lookup service
     * @param metacatJson         json util
     * @param userMetadataService user metadata service
     */
    public MySqlTagService(
        final Config config,
        final JdbcTemplate jdbcTemplate,
        final LookupService lookupService,
        final MetacatJson metacatJson,
        final UserMetadataService userMetadataService
    ) {
        this.config = Preconditions.checkNotNull(config, "config is required");
        this.jdbcTemplate = jdbcTemplate;
        this.jdbcTemplateLongTimeout = MySqlServiceUtil.createJdbcTemplate(
            jdbcTemplate.getDataSource(), config.getLongMetadataQueryTimeout());
        this.lookupService = Preconditions.checkNotNull(lookupService, "lookupService is required");
        this.metacatJson = Preconditions.checkNotNull(metacatJson, "metacatJson is required");
        this.userMetadataService = Preconditions.checkNotNull(userMetadataService, "userMetadataService is required");
    }

    private Lookup addTags(final Set<String> tags, final boolean includeValues) {
        try {
            return lookupService.addValues(LOOKUP_NAME_TAG, tags, includeValues);
        } catch (Exception e) {
            final String message = String.format("Failed adding the tags %s", tags);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }

    }

    /**
     * Get the tag item.
     *
     * @param name name
     * @return tag item
     */
    public TagItem get(final QualifiedName name) {
        return get(name.toString());
    }

    /**
     * Returns the TagItem for the given <code>name</code>.
     *
     * @param name tag name
     * @return TagItem
     */
    @Transactional(readOnly = true)
    public TagItem get(final String name) {
        try {
            return jdbcTemplate.queryForObject(
                SQL_GET_TAG_ITEM,
                new Object[]{name}, new int[]{Types.VARCHAR},
                (rs, rowNum) -> {
                    final TagItem tagItem = new TagItem();
                    tagItem.setId(rs.getLong("id"));
                    tagItem.setName(rs.getString("name"));
                    tagItem.setCreatedBy(rs.getString("createdBy"));
                    tagItem.setLastUpdated(rs.getDate("lastUpdated"));
                    tagItem.setLastUpdatedBy(rs.getString("lastUpdatedBy"));
                    tagItem.setDateCreated(rs.getDate("dateCreated"));
                    tagItem.setValues(getValues(rs.getLong("id")));
                    return tagItem;

                });
        } catch (EmptyResultDataAccessException e) {
            return null;
        } catch (Exception e) {
            final String message = String.format("Failed to get the tag for name %s", name);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }

    }

    /**
     * Returns the list of tags of the tag item id.
     *
     * @param tagItemId tag item id
     * @return list of tags
     */
    private Set<String> getValues(final Long tagItemId) {
        try {
            return MySqlServiceUtil.getValues(jdbcTemplate, SQL_GET_TAG_ITEM_TAGS, tagItemId);
        } catch (EmptyResultDataAccessException e) {
            return Sets.newHashSet();
        } catch (Exception e) {
            final String message = String.format("Failed to get the tags for id %s", tagItemId);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
    }

    /**
     * findOrCreateTagItemByName.
     *
     * @param name name to find or create
     * @return Tag Item
     * @throws SQLException sql exception
     */
    private TagItem findOrCreateTagItemByName(final String name) throws SQLException {
        TagItem result = get(name);
        if (result == null) {
            final KeyHolder holder = new GeneratedKeyHolder();
            jdbcTemplate.update(connection -> {
                final PreparedStatement ps = connection.prepareStatement(SQL_INSERT_TAG_ITEM,
                    Statement.RETURN_GENERATED_KEYS);
                ps.setString(1, name);
                ps.setString(2, config.getTagServiceUserAdmin());
                ps.setString(3, config.getTagServiceUserAdmin());
                return ps;
            }, holder);
            final Long id = holder.getKey().longValue();
            result = new TagItem();
            result.setName(name);
            result.setId(id);
        }
        return result;
    }

    @Override
    public void renameTableTags(final QualifiedName name, final String newTableName) {
        try {
            final QualifiedName newName = QualifiedName.ofTable(name.getCatalogName(), name.getDatabaseName(),
                newTableName);
            if (get(newName) != null) {
                delete(newName, false /*don't delete existing definition metadata with the new name*/);
            }
            jdbcTemplate.update(SQL_UPDATE_TAG_ITEM, new String[]{newName.toString(), name.toString()},
                new int[]{Types.VARCHAR, Types.VARCHAR});
        } catch (Exception e) {
            final String message = String.format("Failed to rename item name %s", name);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
    }

    @Override
    public void delete(final QualifiedName name, final boolean updateUserMetadata) {
        try {
            jdbcTemplate
                .update(SQL_DELETE_TAG_ITEM_TAGS_BY_NAME, new SqlParameterValue(Types.VARCHAR, name.toString()));
            jdbcTemplate.update(SQL_DELETE_TAG_ITEM, new SqlParameterValue(Types.VARCHAR, name.toString()));
            if (updateUserMetadata) {
                // Set the tags in user metadata
                final Map<String, Set<String>> data = Maps.newHashMap();
                data.put(NAME_TAGS, Sets.newHashSet());
                userMetadataService
                    .saveDefinitionMetadata(name, "admin", Optional.of(metacatJson.toJsonObject(data)),
                        true);
            }
        } catch (Exception e) {
            final String message = String.format("Failed to delete all tags for name %s", name);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
    }

    /**
     * remove.
     *
     * @param name               qualifiedName
     * @param tags               tags
     * @param updateUserMetadata flag to update user metadata
     */
    public void remove(final QualifiedName name, final Set<String> tags, final boolean updateUserMetadata) {
        try {
            final TagItem tagItem = get(name);
            if (tagItem == null || tagItem.getValues().isEmpty()) {
                log.info(String.format("No tags or tagItems found for table %s", name));
                return;
            }
            final List<SqlParameterValue> params = Lists.newArrayList();
            params.add(new SqlParameterValue(Types.VARCHAR, name.toString()));
            jdbcTemplate.update(String.format(SQL_DELETE_TAG_ITEM_TAGS_BY_NAME_TAGS,
                buildParametrizedInClause(tags, params, params.size())),
                params.toArray());
            if (updateUserMetadata) {
                tagItem.getValues().removeAll(tags);
                final Map<String, Set<String>> data = Maps.newHashMap();
                data.put(NAME_TAGS, tagItem.getValues());
                userMetadataService
                    .saveDefinitionMetadata(name, "admin", Optional.of(metacatJson.toJsonObject(data)),
                        true);
            }
        } catch (Exception e) {
            final String message = String.format("Failed to remove tags for name %s", name);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
    }

    /**
     * Returns the list of tags.
     *
     * @return list of tag names
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getTags() {
        return lookupService.getValues(LOOKUP_NAME_TAG);
    }

    /**
     * Returns the list of <code>QualifiedName</code> of items that are tagged by the
     * given <code>includeTags</code> and do not contain the given <code>excludeTags</code>.
     *
     * @param includeTags  include items that contain tags
     * @param excludeTags  include items that do not contain tags
     * @param sourceName   catalog/source name
     * @param databaseName database name
     * @param tableName    table name
     * @param type         metacat data category
     * @return list of qualified names of the items
     */
    @Override
    @Transactional(readOnly = true)
    public List<QualifiedName> list(
        @Nullable final Set<String> includeTags,
        @Nullable final Set<String> excludeTags,
        @Nullable final String sourceName,
        @Nullable final String databaseName,
        @Nullable final String tableName,
        @Nullable final QualifiedName.Type type
    ) {
        Set<String> includedNames = Sets.newHashSet();
        final Set<String> excludedNames = Sets.newHashSet();
        final String wildCardName =
            QualifiedName.qualifiedNameToWildCardQueryString(sourceName, databaseName, tableName);
        final Set<String> localIncludes = includeTags != null ? includeTags : Sets.newHashSet();
        validateRequestTagCount(localIncludes);
        try {
            includedNames.addAll(queryTaggedItems(wildCardName, type, localIncludes));
            if (excludeTags != null && !excludeTags.isEmpty()) {
                excludedNames.addAll(queryTaggedItems(wildCardName, type, excludeTags));
            }
        } catch (Exception e) {
            final String message = String.format("Failed getting the list of qualified names for tags %s", includeTags);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
        if (excludeTags != null && !excludeTags.isEmpty()) {
            includedNames = Sets.difference(includedNames, excludedNames);
        }
        return includedNames.stream().map(s -> QualifiedName.fromString(s, false)).collect(Collectors.toList());
    }

    /**
     * Returns the list of <code>QualifiedName</code> of items that have tags containing the given tag text.
     *
     * @param tag          partial text of a tag
     * @param sourceName   source/catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @return list of qualified names of the items
     */
    @Override
    @Transactional(readOnly = true)
    public List<QualifiedName> search(
        @Nullable final String tag,
        @Nullable final String sourceName,
        @Nullable final String databaseName,
        @Nullable final String tableName
    ) {
        final Set<String> result = Sets.newHashSet();
        try {
            final String wildCardName =
                QualifiedName.qualifiedNameToWildCardQueryString(sourceName, databaseName, tableName);
            //Includes
            final String query = String.format(QUERY_SEARCH, "like ?");
            final Object[] params = {tag == null ? 1 : 0, tag + "%", wildCardName == null ? 1 : 0, wildCardName};
            result.addAll(jdbcTemplate.query(query, params,
                new int[]{Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.VARCHAR},
                (rs, rowNum) -> rs.getString("name")));
        } catch (Exception e) {
            final String message = String.format("Failed getting the list of qualified names for tag %s", tag);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
        return result.stream().map(QualifiedName::fromString).collect(Collectors.toList());
    }

    /**
     * Tags the given table with the given <code>tags</code>.
     *
     * @param name resource name
     * @param tags list of tags
     * @return return the complete list of tags associated with the table
     */
    @Override
    public Set<String> setTags(final QualifiedName name, final Set<String> tags,
                               final boolean updateUserMetadata) {
        addTags(tags, false);
        try {
            final TagItem tagItem = findOrCreateTagItemByName(name.toString());
            final Set<String> inserts;
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
                insertTagItemTags(tagItem.getId(), inserts);
            }
            if (!deletes.isEmpty()) {
                removeTagItemTags(tagItem.getId(), deletes);
            }
            if (updateUserMetadata) {
                // Set the tags in user metadata
                final Map<String, Set<String>> data = Maps.newHashMap();
                data.put(NAME_TAGS, values);
                userMetadataService
                    .saveDefinitionMetadata(name, "admin", Optional.of(metacatJson.toJsonObject(data)),
                        true);
            }
        } catch (Exception e) {
            final String message = String.format("Failed to remove tags for name %s", name);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
        return tags;
    }

    private void removeTagItemTags(final Long id, final Set<String> tags) {
        final List<SqlParameterValue> params = Lists.newArrayList();
        params.add(new SqlParameterValue(Types.BIGINT, id));
        jdbcTemplate
            .update(String.format(SQL_DELETE_TAG_ITEM_TAGS, buildParametrizedInClause(
                tags,
                params,
                params.size()
            )), params.toArray());
    }

    private void insertTagItemTags(final Long id, final Set<String> tags) {
        jdbcTemplate.batchUpdate(SQL_INSERT_TAG_ITEM_TAGS, tags.stream().map(tag -> new Object[]{id, tag})
            .collect(Collectors.toList()), new int[]{Types.BIGINT, Types.VARCHAR});
    }

    /**
     * Removes the tags from the given resource.
     *
     * @param name      qualified name
     * @param deleteAll if true, will delete all tags associated with the given table
     * @param tags      list of tags to be removed for the given table
     */
    @Override
    public void removeTags(final QualifiedName name, final Boolean deleteAll,
                           final Set<String> tags, final boolean updateUserMetadata) {
        if (deleteAll != null && deleteAll) {
            delete(name, updateUserMetadata);
        } else {
            remove(name, tags, updateUserMetadata);
        }
    }

    private List<String> queryTaggedItems(final String name,
                                          final QualifiedName.Type type,
                                          final Set<String> tags) {
        final List<SqlParameterValue> sqlParams = Lists.newArrayList();
        sqlParams.add(new SqlParameterValue(Types.INTEGER, tags.size() == 0 ? 1 : 0));
        final String query = String.format(QUERY_LIST,
            buildParametrizedInClause(tags, sqlParams, sqlParams.size()));
        sqlParams.addAll(Stream.of(
            new SqlParameterValue(Types.INTEGER, name == null ? 1 : 0),
            new SqlParameterValue(Types.VARCHAR, name),
            new SqlParameterValue(Types.INTEGER, type == null ? 1 : 0),
            new SqlParameterValue(Types.VARCHAR, type == null ? ".*" : type.getRegexValue())
        ).collect(Collectors.toList()));
        return jdbcTemplateLongTimeout.query(query,
            sqlParams.toArray(),
            (rs, rowNum) -> rs.getString("name"));
    }

    private static String buildParametrizedInClause(final Set<String> tags,
                                                    final List<SqlParameterValue> params,
                                                    final int index) {
        final String tagList = tags.stream().filter(StringUtils::isNotBlank)
            .map(v -> "?").collect(Collectors.joining(", "));
        params.addAll(index, tags.stream().filter(StringUtils::isNotBlank)
            .map(p -> new SqlParameterValue(Types.VARCHAR, p))
            .collect(Collectors.toList()));
        return StringUtils.isBlank(tagList) ? EMPTY_CLAUSE : tagList;
    }

    private static void validateRequestTagCount(final Set<String> tags) {
        final int totalTags = tags.size();
        if (totalTags > MAX_TAGS_LIST_COUNT) {
            throw new MetacatBadRequestException(String.format("Too many tags in request. Count %s", totalTags));
        }
    }
}
