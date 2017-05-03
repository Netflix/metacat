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
package com.netflix.metacat.connector.jdbc;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.exception.ConnectorException;
import lombok.NonNull;

import javax.annotation.Nonnull;
import java.sql.SQLException;

/**
 * An interface to map JDBC SQLExceptions to Metacat Connector Exceptions.
 *
 * @author tgianos
 * @author zhenl
 * @see ConnectorException
 * @see SQLException
 * @since 1.0.0
 */
public interface JdbcExceptionMapper {

    /**
     * Convert JDBC exception to MetacatException.
     *
     * @param se   The sql exception to map
     * @param name The qualified name of the resource that was attempting to be accessed when the exception occurred
     * @return A best attempt at a corresponding connector exception or generic with the SQLException as the cause
     */
    default ConnectorException toConnectorException(
        @NonNull @Nonnull final SQLException se,
        @Nonnull @NonNull final QualifiedName name
    ) {
        return new ConnectorException(se.getMessage(), se);
    }
}
