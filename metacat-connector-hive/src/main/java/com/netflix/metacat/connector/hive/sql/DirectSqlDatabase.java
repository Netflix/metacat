/*
 *  Copyright 2019 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */
package com.netflix.metacat.connector.hive.sql;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.exception.DatabaseNotFoundException;
import com.netflix.metacat.common.server.connectors.model.AuditInfo;
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo;
import com.netflix.metacat.connector.hive.monitoring.HiveMetrics;
import com.netflix.metacat.connector.hive.util.HiveConnectorFastServiceMetric;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nullable;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class makes direct sql calls to update database metadata.
 *
 * @author amajumdar
 * @since 1.3.0
 */
@Slf4j
@Transactional("hiveTxManager")
public class DirectSqlDatabase {
    private static final String COL_URI = "uri";
    private static final String COL_OWNER = "owner";
    private static final String COL_PARAM_KEY = "param_key";
    private static final String COL_PARAM_VALUE = "param_value";
    private final Registry registry;
    private final JdbcTemplate jdbcTemplate;
    private final HiveConnectorFastServiceMetric fastServiceMetric;

    /**
     * Constructor.
     *
     * @param connectorContext       server context
     * @param jdbcTemplate           JDBC template
     * @param fastServiceMetric      fast service metric
     */
    public DirectSqlDatabase(
        final ConnectorContext connectorContext,
        final JdbcTemplate jdbcTemplate,
        final HiveConnectorFastServiceMetric fastServiceMetric
    ) {
        this.registry = connectorContext.getRegistry();
        this.jdbcTemplate = jdbcTemplate;
        this.fastServiceMetric = fastServiceMetric;
    }

    /**
     * Returns the database internal id.
     * @param databaseName database name
     * @return database id
     */
    private Long getDatabaseId(final QualifiedName databaseName) {
        try {
            return jdbcTemplate.queryForObject(SQL.GET_DATABASE_ID,
                new String[]{databaseName.getDatabaseName()},
                new int[]{Types.VARCHAR}, Long.class);
        } catch (EmptyResultDataAccessException e) {
            log.debug("Database {} not found.", databaseName);
            throw new DatabaseNotFoundException(databaseName);
        }
    }

    /**
     * Returns the database.
     * @param databaseName database name
     * @return database
     */
    @Transactional(readOnly = true)
    public DatabaseInfo getDatabase(final QualifiedName databaseName) {
        final Long id = getDatabaseId(databaseName);
        return getDatabaseById(id, databaseName);
    }

    private DatabaseInfo getDatabaseById(final Long id, final QualifiedName databaseName) {
        DatabaseInfo result = null;
        try {
            // Retrieve databaseRowSet info record
            final SqlRowSet databaseRowSet = jdbcTemplate.queryForRowSet(SQL.GET_DATABASE,
                new Object[]{id}, new int[]{Types.BIGINT});
            if (databaseRowSet.first()) {
                final AuditInfo auditInfo =
                    AuditInfo.builder().createdBy(databaseRowSet.getString(COL_OWNER)).build();

                //Retrieve databaseRowSet params
                final Map<String, String> metadata = Maps.newHashMap();
                try {
                    final SqlRowSet paramRowSet = jdbcTemplate.queryForRowSet(SQL.GET_DATABASE_PARAMS,
                        new Object[]{id}, new int[]{Types.BIGINT});
                    while (paramRowSet.next()) {
                        metadata.put(paramRowSet.getString(COL_PARAM_KEY),
                            paramRowSet.getString(COL_PARAM_VALUE));
                    }
                } catch (EmptyResultDataAccessException ignored) { }
                result = DatabaseInfo.builder()
                    .name(databaseName)
                    .uri(databaseRowSet.getString(COL_URI))
                    .auditInfo(auditInfo).metadata(metadata).build();
            }
        } catch (EmptyResultDataAccessException e) {
            log.debug("Database {} not found.", databaseName);
            throw new DatabaseNotFoundException(databaseName);
        }
        return result;
    }

    /**
     * Updates the database object.
     * @param databaseInfo database object
     */
    public void update(final DatabaseInfo databaseInfo) {
        log.debug("Start: Database update using direct sql for {}", databaseInfo.getName());
        final long start = registry.clock().wallTime();
        try {
            final Long databaseId = getDatabaseId(databaseInfo.getName());
            final DatabaseInfo existingDatabaseInfo = getDatabaseById(databaseId, databaseInfo.getName());
            final Map<String, String> newMetadata = databaseInfo.getMetadata() == null ? Maps.newHashMap()
                : databaseInfo.getMetadata();
            final MapDifference<String, String> diff = Maps.difference(existingDatabaseInfo.getMetadata(), newMetadata);
            insertDatabaseParams(databaseId, diff.entriesOnlyOnRight());
            final Map<String, String> updateParams = diff.entriesDiffering().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, s -> s.getValue().rightValue()));
            updateDatabaseParams(databaseId, updateParams);
            final String uri =
                Strings.isNullOrEmpty(databaseInfo.getUri()) ? existingDatabaseInfo.getUri() : databaseInfo.getUri();
            final String newOwner = getOwner(databaseInfo.getAudit());
            final String owner =
                Strings.isNullOrEmpty(newOwner) ? newOwner : existingDatabaseInfo.getAudit().getCreatedBy();
            jdbcTemplate.update(SQL.UPDATE_DATABASE, new SqlParameterValue(Types.VARCHAR, uri),
                new SqlParameterValue(Types.VARCHAR, owner),
                new SqlParameterValue(Types.BIGINT, databaseId));
        } finally {
            this.fastServiceMetric.recordTimer(
                HiveMetrics.TagAlterDatabase.getMetricName(), registry.clock().wallTime() - start);
            log.debug("End: Database update using direct sql for {}", databaseInfo.getName());
        }
    }

    private String getOwner(@Nullable final AuditInfo audit) {
        return audit != null ? audit.getCreatedBy() : null;
    }

    private void insertDatabaseParams(final Long id, final Map<String, String> params) {
        if (!params.isEmpty()) {
            final List<Object[]> paramsList = params.entrySet().stream()
                .map(s -> new Object[]{id, s.getKey(), s.getValue()}).collect(Collectors.toList());
            jdbcTemplate.batchUpdate(SQL.INSERT_DATABASE_PARAMS, paramsList,
                new int[]{Types.BIGINT, Types.VARCHAR, Types.VARCHAR});
        }
    }

    private void updateDatabaseParams(final Long id, final Map<String, String> params) {
        if (!params.isEmpty()) {
            final List<Object[]> paramsList = params.entrySet().stream()
                .map(s -> new Object[]{s.getValue(), id, s.getKey()}).collect(Collectors.toList());
            jdbcTemplate.batchUpdate(SQL.UPDATE_DATABASE_PARAMS, paramsList,
                new int[]{Types.VARCHAR, Types.BIGINT, Types.VARCHAR});
        }
    }

    @VisibleForTesting
    private static class SQL {
        static final String GET_DATABASE_ID =
            "select d.db_id from DBS d where d.name=?";
        static final String GET_DATABASE =
            "select d.desc, d.name, d.db_location_uri uri, d.owner_name owner from DBS d where d.db_id=?";
        static final String GET_DATABASE_PARAMS =
            "select param_key, param_value from DATABASE_PARAMS where db_id=?";
        static final String UPDATE_DATABASE_PARAMS =
            "update DATABASE_PARAMS set param_value=? WHERE db_id=? and param_key=?";
        static final String INSERT_DATABASE_PARAMS =
            "insert into DATABASE_PARAMS(db_id,param_key,param_value) values (?,?,?)";
        static final String UPDATE_DATABASE =
            "UPDATE DBS SET db_location_uri=?, owner_name=? WHERE db_id=?";
    }
}
