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
package com.netflix.metacat.connector.cassandra;

import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.exceptions.DriverException;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.exception.ConnectorException;
import com.netflix.metacat.common.server.exception.DatabaseAlreadyExistsException;
import com.netflix.metacat.common.server.exception.TableAlreadyExistsException;
import lombok.NonNull;

import javax.annotation.Nonnull;

/**
 * Convert Cassandra driver exceptions to connector exceptions.
 *
 * @author tgianos
 * @see com.datastax.driver.core.exceptions.DriverException
 * @see com.netflix.metacat.common.server.exception.ConnectorException
 * @since 1.0.0
 */
public class CassandraExceptionMapper {

    /**
     * Convert the given Cassandra driver exception to a corresponding ConnectorException if possible, otherwise
     * return a generic ConnectorException.
     *
     * @param de   The Cassandra driver exception
     * @param name The fully qualified name of the resource which was attempting to be accessed or modified at time of
     *             error
     * @return A connector exception wrapping the DriverException
     */
    public ConnectorException toConnectorException(
        @Nonnull @NonNull final DriverException de,
        @Nonnull @NonNull final QualifiedName name
    ) {
        if (de instanceof AlreadyExistsException) {
            final AlreadyExistsException ae = (AlreadyExistsException) de;
            if (ae.wasTableCreation()) {
                return new TableAlreadyExistsException(name, ae);
            } else {
                return new DatabaseAlreadyExistsException(name, ae);
            }
        } else {
            return new ConnectorException(de.getMessage(), de);
        }
    }
}
