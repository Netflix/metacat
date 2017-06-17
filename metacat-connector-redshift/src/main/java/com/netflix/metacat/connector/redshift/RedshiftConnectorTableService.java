/*
 *
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
 *
 */
package com.netflix.metacat.connector.redshift;

import com.google.inject.Inject;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.connector.jdbc.JdbcExceptionMapper;
import com.netflix.metacat.connector.jdbc.JdbcTypeConverter;
import com.netflix.metacat.connector.jdbc.services.JdbcConnectorTableService;

import javax.sql.DataSource;

/**
 * Redshift table service implementation.
 *
 * @author tgianos
 * @since 1.0.8
 */
public class RedshiftConnectorTableService extends JdbcConnectorTableService {

    /**
     * Constructor.
     *
     * @param dataSource      the datasource to use to connect to the database
     * @param typeConverter   The type converter to use from the SQL type to Metacat canonical type
     * @param exceptionMapper The exception mapper to use
     */
    @Inject
    public RedshiftConnectorTableService(
        final DataSource dataSource,
        final JdbcTypeConverter typeConverter,
        final JdbcExceptionMapper exceptionMapper
    ) {
        super(dataSource, typeConverter, exceptionMapper);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getRenameTableSql(
        final QualifiedName oldName,
        final String finalOldTableName,
        final String finalNewTableName
    ) {
        return "ALTER TABLE "
            + oldName.getDatabaseName()
            + "."
            + finalOldTableName
            + " RENAME TO "
            + finalNewTableName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getDropTableSql(final QualifiedName name, final String finalTableName) {
        return "DROP TABLE " + name.getCatalogName() + "." + name.getDatabaseName() + "." + finalTableName;
    }
}
