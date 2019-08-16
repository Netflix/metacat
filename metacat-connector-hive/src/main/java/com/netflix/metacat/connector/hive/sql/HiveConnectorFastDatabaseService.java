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

import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo;
import com.netflix.metacat.connector.hive.HiveConnectorDatabaseService;
import com.netflix.metacat.connector.hive.IMetacatHiveClient;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;

/**
 * This class provides the database service using direct sql.
 *
 * @author amajumdar
 * @since 1.3.0
 */
public class HiveConnectorFastDatabaseService extends HiveConnectorDatabaseService {
    private final DirectSqlDatabase directSqlDatabase;
    /**
     * Constructor.
     *
     * @param metacatHiveClient     hive client
     * @param hiveMetacatConverters hive converter
     * @param directSqlDatabase     database sql data service
     */
    public HiveConnectorFastDatabaseService(final IMetacatHiveClient metacatHiveClient,
                                            final HiveConnectorInfoConverter hiveMetacatConverters,
                                            final DirectSqlDatabase directSqlDatabase) {
        super(metacatHiveClient, hiveMetacatConverters);
        this.directSqlDatabase = directSqlDatabase;
    }

    @Override
    public void update(final ConnectorRequestContext context, final DatabaseInfo databaseInfo) {
        directSqlDatabase.update(databaseInfo);
    }
}
