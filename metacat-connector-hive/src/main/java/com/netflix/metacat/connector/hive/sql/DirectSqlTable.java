/*
 *
 *  Copyright 2018 Netflix, Inc.
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
 *
 */
package com.netflix.metacat.connector.hive.sql;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.exception.ConnectorException;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException;
import com.netflix.metacat.common.server.connectors.exception.TableNotFoundException;
import com.netflix.metacat.connector.hive.monitoring.HiveMetrics;
import com.netflix.metacat.connector.hive.util.HiveConnectorFastServiceMetric;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Types;
import java.util.List;
import java.util.Map;

/**
 * This class makes direct sql calls to get/set table metadata.
 *
 * @author amajumdar
 * @since 1.2.0
 */
@Slf4j
@Transactional("hiveTxManager")
public class DirectSqlTable {
    /**
     * Defines the table type.
     */
    public static final String PARAM_TABLE_TYPE = "table_type";
    /**
     * Defines the current metadata location of the iceberg table.
     */
    public static final String PARAM_METADATA_LOCATION = "metadata_location";
    /**
     * Defines the previous metadata location of the iceberg table.
     */
    public static final String PARAM_PREVIOUS_METADATA_LOCATION = "previous_metadata_location";
    /**
     * Iceberg table type.
     */
    public static final String ICEBERG_TABLE_TYPE = "ICEBERG";
    protected static final String PARAM_METADATA_LOCK = "metadata_lock";
    private final Registry registry;
    private final JdbcTemplate jdbcTemplate;
    private final HiveConnectorFastServiceMetric fastServiceMetric;
    private final String catalogName;
    private final DirectSqlSavePartition directSqlSavePartition;

    /**
     * Constructor.
     *
     * @param connectorContext       server context
     * @param jdbcTemplate           JDBC template
     * @param fastServiceMetric      fast service metric
     * @param directSqlSavePartition direct sql partition service
     */
    public DirectSqlTable(
        final ConnectorContext connectorContext,
        final JdbcTemplate jdbcTemplate,
        final HiveConnectorFastServiceMetric fastServiceMetric,
        final DirectSqlSavePartition directSqlSavePartition
    ) {
        this.catalogName = connectorContext.getCatalogName();
        this.registry = connectorContext.getRegistry();
        this.jdbcTemplate = jdbcTemplate;
        this.fastServiceMetric = fastServiceMetric;
        this.directSqlSavePartition = directSqlSavePartition;
    }

    /**
     * Returns true if table exists with the given name.
     * @param name table name
     * @return true if table exists with the given name.
     */
    @Transactional(readOnly = true)
    public boolean exists(final QualifiedName name) {
        final long start = registry.clock().wallTime();
        boolean result = false;
        try {
            final Object qResult = jdbcTemplate.queryForObject(SQL.EXIST_TABLE_BY_NAME,
                new String[]{name.getDatabaseName(), name.getTableName()},
                new int[]{Types.VARCHAR, Types.VARCHAR}, Integer.class);
            if (qResult != null) {
                result = true;
            }
        } catch (EmptyResultDataAccessException e) {
            log.debug("Table {} does not exist.", name);
            return false;
        } finally {
            this.fastServiceMetric.recordTimer(
                HiveMetrics.TagTableExists.getMetricName(), registry.clock().wallTime() - start);
        }
        return result;
    }

    /**
     * Returns all the table names referring to the given <code>uris</code>.
     *
     * @param uris         locations
     * @param prefixSearch if true, we look for tables whose location starts with the given <code>uri</code>
     * @return map of uri to list of partition names
     * @throws UnsupportedOperationException If the connector doesn't implement this method
     */
    @Transactional(readOnly = true)
    public Map<String, List<QualifiedName>> getTableNames(final List<String> uris, final boolean prefixSearch) {
        final long start = registry.clock().wallTime();
        // Create the sql
        final StringBuilder queryBuilder = new StringBuilder(SQL.GET_TABLE_NAMES_BY_URI);
        final List<SqlParameterValue> params = Lists.newArrayList();
        if (prefixSearch) {
            queryBuilder.append(" and (1=0");
            uris.forEach(uri -> {
                queryBuilder.append(" or location like ?");
                params.add(new SqlParameterValue(Types.VARCHAR, uri + "%"));
            });
            queryBuilder.append(" )");
        } else {
            queryBuilder.append(" and location in (");
            uris.forEach(uri -> {
                queryBuilder.append("?,");
                params.add(new SqlParameterValue(Types.VARCHAR, uri));
            });
            queryBuilder.deleteCharAt(queryBuilder.length() - 1).append(")");
        }
        ResultSetExtractor<Map<String, List<QualifiedName>>> handler = rs -> {
            final Map<String, List<QualifiedName>> result = Maps.newHashMap();
            while (rs.next()) {
                final String schemaName = rs.getString("schema_name");
                final String tableName = rs.getString("table_name");
                final String uri = rs.getString("location");
                final List<QualifiedName> names = result.computeIfAbsent(uri, k -> Lists.newArrayList());
                names.add(QualifiedName.ofTable(catalogName, schemaName, tableName));
            }
            return result;
        };
        try {
            return jdbcTemplate.query(queryBuilder.toString(), params.toArray(), handler);
        } finally {
            this.fastServiceMetric.recordTimer(
                HiveMetrics.TagGetTableNames.getMetricName(), registry.clock().wallTime() - start);
        }
    }

    /**
     * Locks the iceberg table for update so that no other request can modify the table.
     * @param tableId table internal id
     * @param tableName table name
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void lockIcebergTable(final Long tableId, final QualifiedName tableName) {
        String tableType = null;
        try {
            tableType = jdbcTemplate.queryForObject(SQL.TABLE_PARAM_LOCK,
                new SqlParameterValue[] {new SqlParameterValue(Types.BIGINT, tableId),
                    new SqlParameterValue(Types.VARCHAR, PARAM_TABLE_TYPE), }, String.class);
        } catch (EmptyResultDataAccessException ignored) { }
        if (tableType == null || !ICEBERG_TABLE_TYPE.equalsIgnoreCase(tableType)) {
            final String message = String.format("Originally table %s is not of type iceberg", tableName);
            log.info(message);
            throw new InvalidMetaException(tableName, message, null);
        }
        Boolean isLocked = null;
        try {
            isLocked = jdbcTemplate.queryForObject(SQL.TABLE_PARAM_LOCK,
                new SqlParameterValue[] {new SqlParameterValue(Types.BIGINT, tableId),
                    new SqlParameterValue(Types.VARCHAR, PARAM_METADATA_LOCK), }, Boolean.class);
        } catch (EmptyResultDataAccessException ignored) { }
        if (isLocked == null) {
            insertTableParam(tableId, PARAM_METADATA_LOCK, 1);
        } else if (isLocked) {
            final String message = String.format("Iceberg table %s is locked.", tableName);
            log.info(message);
            throw new IllegalStateException(message);
        } else {
            updateTableParam(tableId, PARAM_METADATA_LOCK, 1);
        }
    }

    /**
     * Unlocks the iceberg table after update so that another request can modify the table.
     * @param tableId table internal id
     */
    public void unlockIcebergTable(final Long tableId) {
        updateTableParam(tableId, PARAM_METADATA_LOCK, 0);
    }

    private void insertTableParam(final Long tableId, final String parameter, final int value) {
        jdbcTemplate.update(SQL.INSERT_TABLE_PARAMS, new SqlParameterValue(Types.BIGINT, tableId),
            new SqlParameterValue(Types.VARCHAR, parameter), new SqlParameterValue(Types.VARCHAR, value));
    }

    private void updateTableParam(final Long tableId, final String parameter, final int value) {
        jdbcTemplate.update(SQL.UPDATE_TABLE_PARAMS, new SqlParameterValue(Types.VARCHAR, value),
            new SqlParameterValue(Types.BIGINT, tableId), new SqlParameterValue(Types.VARCHAR, parameter));
    }

    /**
     * Returns the table internal id.
     * @param tableName table name
     * @return table id
     */
    @Transactional(readOnly = true)
    public Long getTableId(final QualifiedName tableName) {
        try {
            return jdbcTemplate.queryForObject(SQL.GET_TABLE_ID,
                new String[]{tableName.getDatabaseName(), tableName.getTableName()},
                new int[]{Types.VARCHAR, Types.VARCHAR}, Long.class);
        } catch (EmptyResultDataAccessException e) {
            throw new TableNotFoundException(tableName);
        }
    }

    /**
     * Deletes all the table related information from the store.
     * @param tableName table name
     */
    public void delete(final QualifiedName tableName) {
        try {
            final TableSequenceIds ids = getSequenceIds(tableName);
            directSqlSavePartition.delete(tableName);
            jdbcTemplate.update(SQL.UPDATE_SDS_CD, new SqlParameterValue(Types.BIGINT, null),
                new SqlParameterValue(Types.BIGINT, ids.getSdsId()));
            jdbcTemplate.update(SQL.UPDATE_SDS_SERDE, new SqlParameterValue(Types.BIGINT, null),
                new SqlParameterValue(Types.BIGINT, ids.getSdsId()));
            //
            // Ignore the error. We should be ignoring the error when table does not exist.
            // In certain hive metastore versions, these tables might not be present.
            // TODO: Better handle this non-existing tables.
            //
            try {
                jdbcTemplate.update(SQL.DELETE_COLUMNS_OLD, new SqlParameterValue(Types.BIGINT, ids.getSdsId()));
            } catch (DataAccessException ignored) {
                log.debug("Ignore. Probably table COLUMNS_OLD does not exist.");
            }
            try {
                jdbcTemplate.update(SQL.DELETE_TBL_PRIVS, new SqlParameterValue(Types.BIGINT, ids.getTableId()));
            } catch (DataAccessException ignored) {
                log.debug("Ignore. Probably table TBL_PRIVS does not exist.");
            }
            try {
                jdbcTemplate.update(SQL.DELETE_TBL_COL_PRIVS, new SqlParameterValue(Types.BIGINT, ids.getTableId()));
            } catch (DataAccessException ignored) {
                log.debug("Ignore. Probably table TBL_COL_PRIVS does not exist.");
            }
            jdbcTemplate.update(SQL.DELETE_COLUMNS_V2, new SqlParameterValue(Types.BIGINT, ids.getCdId()));
            jdbcTemplate.update(SQL.DELETE_CDS, new SqlParameterValue(Types.BIGINT, ids.getCdId()));
            jdbcTemplate.update(SQL.DELETE_PARTITION_KEYS, new SqlParameterValue(Types.BIGINT, ids.getTableId()));
            jdbcTemplate.update(SQL.DELETE_TABLE_PARAMS, new SqlParameterValue(Types.BIGINT, ids.getTableId()));
            jdbcTemplate.update(SQL.DELETE_TAB_COL_STATS, new SqlParameterValue(Types.BIGINT, ids.getTableId()));
            jdbcTemplate.update(SQL.UPDATE_TABLE_SD, new SqlParameterValue(Types.BIGINT, null),
                new SqlParameterValue(Types.BIGINT, ids.getTableId()));
            jdbcTemplate.update(SQL.DELETE_SKEWED_COL_NAMES, new SqlParameterValue(Types.BIGINT, ids.getSdsId()));
            jdbcTemplate.update(SQL.DELETE_BUCKETING_COLS, new SqlParameterValue(Types.BIGINT, ids.getSdsId()));
            jdbcTemplate.update(SQL.DELETE_SORT_COLS, new SqlParameterValue(Types.BIGINT, ids.getSdsId()));
            jdbcTemplate.update(SQL.DELETE_SD_PARAMS, new SqlParameterValue(Types.BIGINT, ids.getSdsId()));
            jdbcTemplate.update(SQL.DELETE_SKEWED_COL_VALUE_LOC_MAP,
                new SqlParameterValue(Types.BIGINT, ids.getSdsId()));
            jdbcTemplate.update(SQL.DELETE_SKEWED_VALUES, new SqlParameterValue(Types.BIGINT, ids.getSdsId()));
            jdbcTemplate.update(SQL.DELETE_SERDE_PARAMS, new SqlParameterValue(Types.BIGINT, ids.getSerdeId()));
            jdbcTemplate.update(SQL.DELETE_SERDES, new SqlParameterValue(Types.BIGINT, ids.getSerdeId()));
            jdbcTemplate.update(SQL.DELETE_SDS, new SqlParameterValue(Types.BIGINT, ids.getSdsId()));
            jdbcTemplate.update(SQL.DELETE_TBLS, new SqlParameterValue(Types.BIGINT, ids.getTableId()));
        } catch (DataAccessException e) {
            throw new ConnectorException(String.format("Failed delete hive table %s", tableName), e);
        }
    }

    private TableSequenceIds getSequenceIds(final QualifiedName tableName) {
        try {
            return jdbcTemplate.queryForObject(
                SQL.TABLE_SEQUENCE_IDS,
                new Object[]{tableName.getDatabaseName(), tableName.getTableName()},
                new int[]{Types.VARCHAR, Types.VARCHAR},
                (rs, rowNum) -> new TableSequenceIds(rs.getLong("tbl_id"), rs.getLong("cd_id"),
                    rs.getLong("sd_id"), rs.getLong("serde_id")));
        } catch (EmptyResultDataAccessException e) {
            throw new TableNotFoundException(tableName);
        }
    }

    @VisibleForTesting
    private static class SQL {
        static final String GET_TABLE_NAMES_BY_URI =
            "select d.name schema_name, t.tbl_name table_name, s.location"
                + " from DBS d, TBLS t, SDS s where d.DB_ID=t.DB_ID and t.sd_id=s.sd_id";
        static final String EXIST_TABLE_BY_NAME =
            "select 1 from DBS d join TBLS t on d.DB_ID=t.DB_ID where d.name=? and t.tbl_name=?";
        static final String GET_TABLE_ID =
            "select t.tbl_id from DBS d join TBLS t on d.DB_ID=t.DB_ID where d.name=? and t.tbl_name=?";
        static final String TABLE_PARAM_LOCK =
            "SELECT param_value FROM TABLE_PARAMS WHERE tbl_id=? and param_key=? FOR UPDATE";
        static final String UPDATE_TABLE_PARAMS =
            "update TABLE_PARAMS set param_value=? WHERE tbl_id=? and param_key=?";
        static final String INSERT_TABLE_PARAMS =
            "insert into TABLE_PARAMS(tbl_id,param_key,param_value) values (?,?,?)";
        static final String UPDATE_SDS_CD = "UPDATE SDS SET CD_ID=? WHERE SD_ID=?";
        static final String DELETE_COLUMNS_OLD = "DELETE FROM COLUMNS_OLD WHERE SD_ID=?";
        static final String DELETE_COLUMNS_V2 = "DELETE FROM COLUMNS_V2 WHERE CD_ID=?";
        static final String DELETE_CDS = "DELETE FROM CDS WHERE CD_ID=?";
        static final String DELETE_PARTITION_KEYS = "DELETE FROM PARTITION_KEYS WHERE TBL_ID=?";
        static final String DELETE_TABLE_PARAMS = "DELETE FROM TABLE_PARAMS WHERE TBL_ID=?";
        static final String DELETE_TAB_COL_STATS = "DELETE FROM TAB_COL_STATS WHERE TBL_ID=?";
        static final String UPDATE_TABLE_SD = "UPDATE TBLS SET SD_ID=? WHERE TBL_ID=?";
        static final String DELETE_SKEWED_COL_NAMES = "DELETE FROM SKEWED_COL_NAMES WHERE SD_ID=?";
        static final String DELETE_BUCKETING_COLS = "DELETE FROM BUCKETING_COLS WHERE SD_ID=?";
        static final String DELETE_SORT_COLS = "DELETE FROM SORT_COLS WHERE SD_ID=?";
        static final String DELETE_SD_PARAMS = "DELETE FROM SD_PARAMS WHERE SD_ID=?";
        static final String DELETE_SKEWED_COL_VALUE_LOC_MAP = "DELETE FROM SKEWED_COL_VALUE_LOC_MAP WHERE SD_ID=?";
        static final String DELETE_SKEWED_VALUES = "DELETE FROM SKEWED_VALUES WHERE SD_ID_OID=?";
        static final String UPDATE_SDS_SERDE = "UPDATE SDS SET SERDE_ID=? WHERE SD_ID=?";
        static final String DELETE_SERDE_PARAMS = "DELETE FROM SERDE_PARAMS WHERE SERDE_ID=?";
        static final String DELETE_SERDES = "DELETE FROM SERDES WHERE SERDE_ID=?";
        static final String DELETE_SDS = "DELETE FROM SDS WHERE SD_ID=?";
        static final String DELETE_TBL_PRIVS = "DELETE FROM TBL_PRIVS WHERE TBL_ID=?";
        static final String DELETE_TBL_COL_PRIVS = "DELETE FROM TBL_COL_PRIVS WHERE TBL_ID=?";
        static final String DELETE_TBLS = "DELETE FROM TBLS WHERE TBL_ID=?";
        static final String TABLE_SEQUENCE_IDS = "select t.tbl_id, s.sd_id, s.cd_id, s.serde_id"
            + " from DBS d join TBLS t on d.db_id=t.db_id join SDS s on t.sd_id=s.sd_id"
            + " where d.name=? and t.tbl_name=?";
    }
}
