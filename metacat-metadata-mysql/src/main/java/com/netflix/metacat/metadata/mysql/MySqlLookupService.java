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
import com.netflix.metacat.common.server.util.JdbcUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

/**
 * User metadata service impl using Mysql.
 */
@Slf4j
@SuppressFBWarnings
public class MySqlLookupService implements LookupService {
    private static final String SQL_GET_LOOKUP =
        "select id, name, type, created_by createdBy, last_updated_by lastUpdatedBy, date_created dateCreated,"
            + " last_updated lastUpdated from lookup where name=?";
    private static final String SQL_INSERT_LOOKUP =
        "insert into lookup( name, version, type, created_by, last_updated_by, date_created, last_updated)"
            + " values (?,0,?,?,?,now(),now())";
    private static final String SQL_INSERT_LOOKUP_VALUES =
        "insert into lookup_values( lookup_id, values_string) values (?,?)";
    private static final String SQL_DELETE_LOOKUP_VALUES =
        "delete from lookup_values where lookup_id=? and values_string in (%s)";
    private static final String SQL_GET_LOOKUP_VALUES =
        "select values_string value from lookup_values where lookup_id=?";
    private static final String SQL_GET_LOOKUP_VALUES_BY_NAME =
        "select lv.values_string value from lookup l, lookup_values lv where l.id=lv.lookup_id and l.name=?";
    private static final String STRING_TYPE = "string";
    private final Config config;
    private JdbcUtil jdbcUtil;

    /**
     * Constructor.
     *
     * @param config     config
     * @param dataSource datasource to user
     */
    public MySqlLookupService(final Config config, final DataSource dataSource) {
        this.config = config;
        this.jdbcUtil = new JdbcUtil(dataSource);
    }

    /**
     * Returns the lookup for the given <code>name</code>.
     *
     * @param name lookup name
     * @return lookup
     */
    @Override
    @Transactional(readOnly = true)
    public Lookup get(final String name) {
        try {
            return jdbcUtil.getJdbcTemplate().queryForObject(
                SQL_GET_LOOKUP,
                new Object[]{name},
                (rs, rowNum) -> {
                    final Lookup lookup = new Lookup();
                    lookup.setId(rs.getLong("id"));
                    lookup.setName(rs.getString("name"));
                    lookup.setType(rs.getString("type"));
                    lookup.setCreatedBy(rs.getString("createdBy"));
                    lookup.setLastUpdated(rs.getDate("lastUpdated"));
                    lookup.setLastUpdatedBy(rs.getString("lastUpdatedBy"));
                    lookup.setDateCreated(rs.getDate("dateCreated"));
                    lookup.setValues(getValues(rs.getLong("id")));
                    return lookup;
                });
        } catch (EmptyResultDataAccessException e) {
            return null;
        } catch (DataAccessException e) {
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
            return MySqlServiceUtil.getValues(jdbcUtil, SQL_GET_LOOKUP_VALUES, lookupId);
        } catch (EmptyResultDataAccessException e) {
            return Sets.newHashSet();
        } catch (DataAccessException e) {
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
            return MySqlServiceUtil.getValues(jdbcUtil, SQL_GET_LOOKUP_VALUES_BY_NAME, name);
        } catch (EmptyResultDataAccessException e) {
            return Sets.newHashSet();
        } catch (DataAccessException e) {
            final String message = String.format("Failed to get the lookup values for name %s", name);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
    }

    /**
     * Saves the lookup value.
     *
     * @param name   lookup name
     * @param values multiple values
     * @return returns the lookup with the given name.
     */
    @Override
    @Transactional
    public Lookup setValues(final String name, final Set<String> values) {
        try {
            return jdbcUtil.getTransactionTemplate().execute(new TransactionCallback<Lookup>() {
                @Override
                public Lookup doInTransaction(final TransactionStatus status) {
                    Lookup lookup = null;
                    try {
                        lookup = findOrCreateLookupByName(name);
                    } catch (SQLException e) {
                        throw new CannotCreateTransactionException(
                            "findOrCreateTagItemByName failed " + name, e);
                    }
                    final Set<String> inserts;
                    Set<String> deletes = Sets.newHashSet();
                    final Set<String> lookupValues = lookup.getValues();
                    if (lookupValues == null || lookupValues.isEmpty()) {
                        inserts = values;
                    } else {
                        inserts = Sets.difference(values, lookupValues).immutableCopy();
                        deletes = Sets.difference(lookupValues, values).immutableCopy();
                    }
                    lookup.setValues(values);
                    if (!inserts.isEmpty()) {
                        insertLookupValues(lookup.getId(), inserts);
                    }
                    if (!deletes.isEmpty()) {
                        deleteLookupValues(lookup.getId(), deletes);
                    }
                    return lookup;
                }
            });
        } catch (CannotCreateTransactionException | DataAccessException e) {
            final String message = String.format("Failed to set the lookup values for name %s", name);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
    }

    private void insertLookupValues(final Long id, final Set<String> inserts)
        throws DataAccessException {
        MySqlServiceUtil.batchInsertValues(jdbcUtil, SQL_INSERT_LOOKUP_VALUES, id, inserts);
    }

    private void deleteLookupValues(final Long id, final Set<String> deletes)
        throws DataAccessException {
        MySqlServiceUtil.batchDeleteValues(jdbcUtil, SQL_DELETE_LOOKUP_VALUES, id, deletes);
    }

    /**
     * findOrCreateLookupByName.
     *
     * @param name name to find or create
     * @return Look up object
     * @throws SQLException sql exception
     */
    @Transactional
    public Lookup findOrCreateLookupByName(final String name) throws SQLException {
        Lookup lookup = get(name);
        if (lookup == null) {
            final KeyHolder holder = new GeneratedKeyHolder();
            jdbcUtil.getTransactionTemplate().execute(new TransactionCallbackWithoutResult() {
                @Override
                protected void doInTransactionWithoutResult(final TransactionStatus status) {
                    jdbcUtil.getJdbcTemplate().update(new PreparedStatementCreator() {
                        @Override
                        public PreparedStatement createPreparedStatement(final Connection connection)
                            throws SQLException {
                            final PreparedStatement ps = connection.prepareStatement(SQL_INSERT_LOOKUP,
                                Statement.RETURN_GENERATED_KEYS);
                            ps.setString(1, name);
                            ps.setString(2, STRING_TYPE);
                            ps.setString(3, config.getLookupServiceUserAdmin());
                            ps.setString(4, config.getLookupServiceUserAdmin());
                            return ps;
                        }
                    }, holder);
                }
            });
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
    @Transactional
    public Lookup addValues(final String name, final Set<String> values) {
        try {
            return jdbcUtil.getTransactionTemplate().execute(new TransactionCallback<Lookup>() {
                @Override
                public Lookup doInTransaction(final TransactionStatus status) {
                    Lookup lookup = null;
                    try {
                        lookup = findOrCreateLookupByName(name);
                    } catch (SQLException e) {
                        throw new CannotCreateTransactionException(
                            "findOrCreateTagItemByName failed " + name, e);
                    }

                    final Set<String> inserts;
                    final Set<String> lookupValues = lookup.getValues();
                    if (lookupValues == null || lookupValues.isEmpty()) {
                        inserts = values;
                        lookup.setValues(values);
                    } else {
                        inserts = Sets.difference(values, lookupValues);
                    }
                    if (!inserts.isEmpty()) {
                        insertLookupValues(lookup.getId(), inserts);
                    }
                    return lookup;
                }
            });
        } catch (CannotCreateTransactionException | DataAccessException e) {
            final String message = String.format("Failed to set the lookup values for name %s", name);
            log.error(message, e);
            throw new UserMetadataServiceException(message, e);
        }
    }

    /**
     * Saves the lookup value.
     *
     * @param name  lookup name
     * @param value lookup value
     * @return returns the lookup with the given name.
     */
    @Override
    public Lookup setValue(final String name, final String value) {
        return setValues(name, Sets.newHashSet(value));
    }
}
