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

package com.netflix.metacat.connector.hive;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.util.DataSourceManager;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import lombok.NonNull;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * HiveConnectorFastTableService.
 *
 * @author zhenl
 * @since 1.0.0
 */
public class HiveConnectorFastTableService extends HiveConnectorTableService {
    private static final String SQL_GET_TABLE_NAMES_BY_URI =
        "select d.name schema_name, t.tbl_name table_name, s.location "
            + "from DBS d, TBLS t, SDS s where d.DB_ID=t.DB_ID and t.sd_id=s.sd_id";
    private static final String SQL_EXIST_TABLE_BY_NAME =
        "select 1 from DBS d join TBLS t on d.DB_ID=t.DB_ID where d.name=? and t.tbl_name=?";

    /**
     * Constructor.
     *
     * @param catalogName                  catalogname
     * @param metacatHiveClient            hive client
     * @param hiveConnectorDatabaseService databaseService
     * @param hiveMetacatConverters        hive converter
     * @param allowRenameTable             allow rename table
     */
    @Inject
    public HiveConnectorFastTableService(@Named("catalogName") final String catalogName,
                                         @Nonnull @NonNull final IMetacatHiveClient metacatHiveClient,
    @Nonnull @NonNull final HiveConnectorDatabaseService hiveConnectorDatabaseService,
                                         @Nonnull @NonNull final HiveConnectorInfoConverter hiveMetacatConverters,
                                         @Named("allowRenameTable") final boolean allowRenameTable) {
        super(catalogName, metacatHiveClient, hiveConnectorDatabaseService, hiveMetacatConverters, allowRenameTable);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public boolean exists(@Nonnull final ConnectorContext requestContext, @Nonnull final QualifiedName name) {
        boolean result = false;
        // Get data source
        final DataSource dataSource = DataSourceManager.get().get(catalogName);
        try (Connection conn = dataSource.getConnection()) {
            final Object qResult = new QueryRunner().query(conn, SQL_EXIST_TABLE_BY_NAME,
                new ScalarHandler(1), name.getDatabaseName(), name.getTableName());
            if (qResult != null) {
                result = true;
            }
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
        return result;
    }

    @Override
    public Map<String, List<QualifiedName>> getTableNames(@Nonnull final ConnectorContext context,
        @Nonnull final  List<String> uris, final boolean prefixSearch) {
        final Map<String, List<QualifiedName>> result = Maps.newHashMap();
        // Get data source
        final DataSource dataSource = DataSourceManager.get().get(catalogName);
        // Create the sql
        final StringBuilder queryBuilder = new StringBuilder(SQL_GET_TABLE_NAMES_BY_URI);
        final List<String> params = Lists.newArrayList();
        if (prefixSearch) {
            queryBuilder.append(" and (1=0");
            uris.forEach(uri -> {
                queryBuilder.append(" or location like ?");
                params.add(uri + "%");
            });
            queryBuilder.append(" )");
        } else {
            queryBuilder.append(" and location in (");
            uris.forEach(uri -> {
                queryBuilder.append("?,");
                params.add(uri);
            });
            queryBuilder.deleteCharAt(queryBuilder.length() - 1).append(")");
        }
        // Handler for reading the result set
        ResultSetHandler<Map<String, List<QualifiedName>>> handler = rs -> {
            while (rs.next()) {
                final String schemaName = rs.getString("schema_name");
                final String tableName = rs.getString("table_name");
                final String uri = rs.getString("location");
                List<QualifiedName> names = result.get(uri);
                if (names == null) {
                    names = Lists.newArrayList();
                    result.put(uri, names);
                }
                names.add(QualifiedName.ofTable(catalogName, schemaName, tableName));
            }
            return result;
        };
        try (Connection conn = dataSource.getConnection()) {
            new QueryRunner()
                .query(conn, queryBuilder.toString(), handler, params.toArray());
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
        return result;
    }

}
