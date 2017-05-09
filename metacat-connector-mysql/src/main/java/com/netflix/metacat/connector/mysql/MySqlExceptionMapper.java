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
package com.netflix.metacat.connector.mysql;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.exception.ConnectorException;
import com.netflix.metacat.common.server.connectors.exception.DatabaseAlreadyExistsException;
import com.netflix.metacat.common.server.connectors.exception.DatabaseNotFoundException;
import com.netflix.metacat.common.server.connectors.exception.TableAlreadyExistsException;
import com.netflix.metacat.common.server.connectors.exception.TableNotFoundException;
import com.netflix.metacat.connector.jdbc.JdbcExceptionMapper;
import lombok.NonNull;

import javax.annotation.Nonnull;
import java.sql.SQLException;

/**
 * Convert MySQL exceptions into generic connector exceptions for use higher up in the system.
 *
 * @author tgianos
 * @author zhenl
 * @see SQLException
 * @see ConnectorException
 * @see <a href="https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-error-sqlstates.html">MySQL Ref</a>
 * @since 1.0.0
 */
public class MySqlExceptionMapper implements JdbcExceptionMapper {

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorException toConnectorException(
        @NonNull @Nonnull final SQLException se,
        @Nonnull @NonNull final QualifiedName name
    ) {
        switch (se.getErrorCode()) {
            case 1007: //database already exists
                return new DatabaseAlreadyExistsException(name, se);
            case 1050: //table already exists
                return new TableAlreadyExistsException(name, se);
            case 1008: //database does not exist
                return new DatabaseNotFoundException(name, se);
            case 1146: //table doesn't exist
                return new TableNotFoundException(name, se);
            default:
                return new ConnectorException(se.getMessage(), se);
        }
    }
}
