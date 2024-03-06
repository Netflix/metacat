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

import com.google.common.collect.Sets;
import com.netflix.metacat.common.server.model.Lookup;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.usermetadata.LookupService;
import com.netflix.metacat.common.server.usermetadata.UserMetadataServiceException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * User metadata service impl using Mysql.
 */
@Slf4j
@SuppressFBWarnings
@Transactional("metadataTxManager")
public class MySqlLookupService implements LookupService {
    private static final String SQL_GET_LOOKUP =
        "select id, name, type, created_by createdBy, last_updated_by lastUpdatedBy, date_created dateCreated,"
            + " last_updated lastUpdated from lookup where name=?";
    private static final String SQL_INSERT_LOOKUP =
        "insert into lookup( name, version, type, created_by, last_updated_by, date_created, last_updated)"
            + " values (?,0,?,?,?,now(),now())";
    private static final String SQL_INSERT_LOOKUP_VALUES =
        "insert into lookup_values( lookup_id, values_string) values (?,?)";
    private static final String SQL_INSERT_LOOKUP_VALUE_IF_NOT_EXIST =
        "INSERT INTO lookup_values(lookup_id, values_string) VALUES (?,?) "
            + "ON DUPLICATE KEY UPDATE lookup_id=lookup_id, values_string=values_string";

    private static final String SQL_GET_LOOKUP_VALUES =
        "select values_string value from lookup_values where lookup_id=?";
    private static final String SQL_GET_LOOKUP_VALUES_BY_NAME =
        "select lv.values_string value from lookup l, lookup_values lv where l.id=lv.lookup_id and l.name=?";
    private static final String STRING_TYPE = "string";
    private final Config config;
    private JdbcTemplate jdbcTemplate;

    /**
     * Constructor.
     *
     * @param config     config
     * @param jdbcTemplate jdbc template
     */
    public MySqlLookupService(final Config config, final JdbcTemplate jdbcTemplate) {
        this.config = config;
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Returns the lookup for the given <code>name</code>.
     *
     * @param name lookup name
     * @return lookup
     */
    @Override
    @Transactional(readOnly = true)
    public Lookup get(final String name, final boolean includeValues) {
        try {
            return jdbcTemplate.queryForObject(
                SQL_GET_LOOKUP,
                new Object[]{name}, new int[]{Types.VARCHAR},
                (rs, rowNum) -> {
                    final Lookup lookup = new Lookup();
                    lookup.setId(rs.getLong("id"));
                    lookup.setName(rs.getString("name"));
                    lookup.setType(rs.getString("type"));
                    lookup.setCreatedBy(rs.getString("createdBy"));
                    lookup.setLastUpdated(rs.getDate("lastUpdated"));
                    lookup.setLastUpdatedBy(rs.getString("lastUpdatedBy"));
                    lookup.setDateCreated(rs.getDate("dateCreated"));
                    lookup.setValues(includeValues ? getValues(rs.getLong("id")) : Collections.EMPTY_SET);
                    return lookup;
                });
        } catch (EmptyResultDataAccessException e) {
            return null;
        } catch (Exception e) {
            final String message = String.format("Failed to get the lookup for name %s", name);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
    }

    /**
     * Returns the value of the lookup name.
     *
     * @param name lookup name
     * @return scalar lookup value
     */
    @Override
    @Transactional(readOnly = true)
    public String getValue(final String name) {
        String result = null;
        final Set<String> values = getValues(name);
        if (values != null && values.size() > 0) {
            result = values.iterator().next();
        }
        return result;
    }

    /**
     * Returns the list of values of the lookup name.
     *
     * @param lookupId lookup id
     * @return list of lookup values
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getValues(final Long lookupId) {
        try {
            return MySqlServiceUtil.getValues(jdbcTemplate, SQL_GET_LOOKUP_VALUES, lookupId);
        } catch (EmptyResultDataAccessException e) {
            return Sets.newHashSet();
        } catch (Exception e) {
            final String message = String.format("Failed to get the lookup values for id %s", lookupId);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
    }

    /**
     * Returns the list of values of the lookup name.
     *
     * @param name lookup name
     * @return list of lookup values
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getValues(final String name) {
        try {
            return MySqlServiceUtil.getValues(jdbcTemplate, SQL_GET_LOOKUP_VALUES_BY_NAME, name);
        } catch (EmptyResultDataAccessException e) {
            return Sets.newHashSet();
        } catch (Exception e) {
            final String message = String.format("Failed to get the lookup values for name %s", name);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
    }

    private void insertLookupValuesIfNotExist(final Long id, final Set<String> inserts) {
        jdbcTemplate.batchUpdate(SQL_INSERT_LOOKUP_VALUE_IF_NOT_EXIST,
            inserts.stream().map(insert -> new Object[]{id, insert})
            .collect(Collectors.toList()), new int[]{Types.BIGINT, Types.VARCHAR});
    }

    /**
     * findOrCreateLookupByName.
     *
     * @param name name to find or create
     * @param includeValues whether to include the values in the Lookup Object
     * @return Look up object
     * @throws SQLException sql exception
     */
    private Lookup findOrCreateLookupByName(final String name, final boolean includeValues) throws SQLException {
        Lookup lookup = get(name, includeValues);
        if (lookup == null) {
            final KeyHolder holder = new GeneratedKeyHolder();
            jdbcTemplate.update(connection -> {
                final PreparedStatement ps = connection.prepareStatement(SQL_INSERT_LOOKUP,
                    Statement.RETURN_GENERATED_KEYS);
                ps.setString(1, name);
                ps.setString(2, STRING_TYPE);
                ps.setString(3, config.getLookupServiceUserAdmin());
                ps.setString(4, config.getLookupServiceUserAdmin());
                return ps;
            }, holder);
            final Long lookupId = holder.getKey().longValue();
            lookup = new Lookup();
            lookup.setName(name);
            lookup.setId(lookupId);
        }
        return lookup;
    }

    /**
     * Saves the lookup value.
     *
     * @param name   lookup name
     * @param values multiple values
     * @return returns the lookup with the given name.
     */
    @Override
    public Lookup addValues(final String name, final Set<String> values, final boolean includeValues) {
        try {
            final Lookup lookup = findOrCreateLookupByName(name, includeValues);
            if (!values.isEmpty()) {
                insertLookupValuesIfNotExist(lookup.getId(), values);
            }
            if (includeValues) {
                lookup.getValues().addAll(values);
            }
            return lookup;
        } catch (Exception e) {
            final String message = String.format("Failed to set the lookup values for name %s", name);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
    }
}
