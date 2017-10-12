/*
 *  Copyright 2017 Netflix, Inc.
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.connector.hive.HiveConnectorDatabaseService;
import com.netflix.metacat.connector.hive.HiveConnectorTableService;
import com.netflix.metacat.connector.hive.IMetacatHiveClient;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.monitoring.HiveMetrics;
import com.netflix.metacat.connector.hive.util.HiveConnectorFastServiceMetric;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Types;
import java.util.List;
import java.util.Map;

/**
 * HiveConnectorFastTableService.
 *
 * @author zhenl
 * @since 1.0.0
 */
@Slf4j
@Transactional("hiveTxManager")
public class HiveConnectorFastTableService extends HiveConnectorTableService {
    private static final String SQL_GET_TABLE_NAMES_BY_URI =
        "select d.name schema_name, t.tbl_name table_name, s.location"
            + " from DBS d, TBLS t, SDS s where d.DB_ID=t.DB_ID and t.sd_id=s.sd_id";
    private static final String SQL_EXIST_TABLE_BY_NAME =
        "select 1 from DBS d join TBLS t on d.DB_ID=t.DB_ID where d.name=? and t.tbl_name=?";
    private final Registry registry;
    private JdbcTemplate jdbcTemplate;
    private final HiveConnectorFastServiceMetric fastServiceMetric;

    /**
     * Constructor.
     *
     * @param catalogName                  catalog name
     * @param metacatHiveClient            hive client
     * @param hiveConnectorDatabaseService databaseService
     * @param hiveMetacatConverters        hive converter
     * @param connectorContext             serverContext
     * @param jdbcTemplate                 JDBC template
     * @param fastServiceMetric     fast service metric
     */
    @Autowired
    public HiveConnectorFastTableService(
        final String catalogName,
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorDatabaseService hiveConnectorDatabaseService,
        final HiveConnectorInfoConverter hiveMetacatConverters,
        final ConnectorContext connectorContext,
        final JdbcTemplate jdbcTemplate,
        final HiveConnectorFastServiceMetric fastServiceMetric
    ) {
        super(catalogName, metacatHiveClient, hiveConnectorDatabaseService, hiveMetacatConverters, connectorContext);
        this.registry = connectorContext.getRegistry();
        this.jdbcTemplate = jdbcTemplate;
        this.fastServiceMetric = fastServiceMetric;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public boolean exists(final ConnectorRequestContext requestContext, final QualifiedName name) {
        final long start = registry.clock().wallTime();
        boolean result = false;
        try {
            final Object qResult = jdbcTemplate.queryForObject(SQL_EXIST_TABLE_BY_NAME,
                new String[]{name.getDatabaseName(), name.getTableName()},
                new int[]{Types.VARCHAR, Types.VARCHAR}, Integer.class);
            if (qResult != null) {
                result = true;
            }
        } catch (EmptyResultDataAccessException e) {
            return false;
        } finally {
            this.fastServiceMetric.recordTimer(
                HiveMetrics.TagTableExists.getMetricName(), registry.clock().wallTime() - start);
        }
        return result;
    }

    @Override
    public Map<String, List<QualifiedName>> getTableNames(
        final ConnectorRequestContext context,
        final List<String> uris,
        final boolean prefixSearch
    ) {
        final long start = registry.clock().wallTime();
        // Create the sql
        final StringBuilder queryBuilder = new StringBuilder(SQL_GET_TABLE_NAMES_BY_URI);
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
        final Map<String, List<QualifiedName>> result = Maps.newHashMap();
        ResultSetExtractor<Map<String, List<QualifiedName>>> handler = rs -> {
            while (rs.next()) {
                final String schemaName = rs.getString("schema_name");
                final String tableName = rs.getString("table_name");
                final String uri = rs.getString("location");
                final List<QualifiedName> names = result.computeIfAbsent(uri, k -> Lists.newArrayList());
                names.add(QualifiedName.ofTable(this.getCatalogName(), schemaName, tableName));
            }
            return result;
        };
        try {
            jdbcTemplate.query(queryBuilder.toString(), params.toArray(), handler);
        } finally {
            this.fastServiceMetric.recordTimer(
                HiveMetrics.TagGetTableNames.getMetricName(), registry.clock().wallTime() - start);
        }
        return result;
    }

}
