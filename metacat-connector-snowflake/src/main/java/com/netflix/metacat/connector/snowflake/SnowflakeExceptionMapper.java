/*
 *
 * Copyright 2018 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */
package com.netflix.metacat.connector.snowflake;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.exception.ConnectorException;
import com.netflix.metacat.common.server.connectors.exception.DatabaseAlreadyExistsException;
import com.netflix.metacat.common.server.connectors.exception.DatabaseNotFoundException;
import com.netflix.metacat.common.server.connectors.exception.TableAlreadyExistsException;
import com.netflix.metacat.common.server.connectors.exception.TableNotFoundException;
import com.netflix.metacat.connector.jdbc.JdbcExceptionMapper;

import java.sql.SQLException;

/**
 * Exception mapper for Snowflake SQLExceptions.
 *
 * @author amajumdar
 * @see SQLException
 * @see ConnectorException
 * @since 1.2.0
 */
public class SnowflakeExceptionMapper implements JdbcExceptionMapper {

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorException toConnectorException(
        final SQLException se,
        final QualifiedName name
    ) {
        final int errorCode = se.getErrorCode();

        switch (errorCode) {
            case 2042: //database already exists
                return new DatabaseAlreadyExistsException(name, se);
            case 2002: //table already exists
                return new TableAlreadyExistsException(name, se);
            case 2043: //database does not exist
                return new DatabaseNotFoundException(name, se);
            case 2003: //table doesn't exist
                return new TableNotFoundException(name, se);
            default:
                return new ConnectorException(se.getMessage(), se);
        }
    }

}
