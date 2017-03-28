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
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.util.DataSourceManager;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import lombok.NonNull;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * HiveConnectorFastTableService.
 *
 * @author zhenl
 * @since 1.0.0
 */
public class HiveConnectorFastTableService extends HiveConnectorTableService {
    private static final String SQL_GET_TABLE_NAMES_BY_URI =
            "select d.name schema_name, t.tbl_name table_name"
                    + " from DBS d, TBLS t, SDS s where d.DB_ID=t.DB_ID and t.sd_id=s.sd_id";
    private final boolean allowRenameTable;
    private final ThreadServiceManager threadServiceManager;

    /**
     * Constructor.
     *
     * @param catalogName                  catalogname
     * @param metacatHiveClient            hive client
     * @param hiveConnectorDatabaseService databaseService
     * @param hiveMetacatConverters        hive converter
     * @param threadServiceManager         threadservicemanager
     * @param allowRenameTable             allow rename table
     */
    @Inject
    public HiveConnectorFastTableService(@Named("catalogName") final String catalogName,
                                         @Nonnull @NonNull final IMetacatHiveClient metacatHiveClient,
    @Nonnull @NonNull final HiveConnectorDatabaseService hiveConnectorDatabaseService,
                                         @Nonnull @NonNull final HiveConnectorInfoConverter hiveMetacatConverters,
                                         final ThreadServiceManager threadServiceManager,
                                         @Named("allowRenameTable") final boolean allowRenameTable) {
        super(catalogName, metacatHiveClient, hiveConnectorDatabaseService, hiveMetacatConverters, allowRenameTable);
        this.allowRenameTable = allowRenameTable;
        this.threadServiceManager = threadServiceManager;
        this.threadServiceManager.start();
    }

    /**
     * listNames.
     *
     * @param uri          uri
     * @param prefixSearch prefixSearch
     * @return list of tables matching the prefixSearch
     */
    public List<QualifiedName> listNames(final String uri, final boolean prefixSearch) {
        final List<QualifiedName> result = Lists.newArrayList();
        // Get data source
        final DataSource dataSource = DataSourceManager.get().get(catalogName);
        // Create the sql
        final StringBuilder queryBuilder = new StringBuilder(SQL_GET_TABLE_NAMES_BY_URI);
        String param = uri;
        if (prefixSearch) {
            queryBuilder.append(" and location like ?");
            param = uri + "%";
        } else {
            queryBuilder.append(" and location = ?");
        }
        // Handler for reading the result set
        final ResultSetHandler<List<QualifiedName>> handler = rs -> {
            while (rs.next()) {
                final String schemaName = rs.getString("schema_name");
                final String tableName = rs.getString("table_name");
                result.add(QualifiedName.ofTable(catalogName, schemaName, tableName));
            }
            return result;
        };
        try (Connection conn = dataSource.getConnection()) {
            new QueryRunner()
                    .query(conn, queryBuilder.toString(), handler, param);
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
        return result;
    }

}
