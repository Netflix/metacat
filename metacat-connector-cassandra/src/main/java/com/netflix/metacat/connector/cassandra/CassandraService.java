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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import lombok.Getter;
import lombok.NonNull;

import javax.annotation.Nonnull;

/**
 * Abstract class for common Cassandra methods based around the Cluster.
 *
 * @author tgianos
 * @since 1.0.0
 */
@Getter
abstract class CassandraService {

    private final Cluster cluster;
    private final CassandraExceptionMapper exceptionMapper;

    CassandraService(
        @Nonnull @NonNull final Cluster cluster,
        @Nonnull @NonNull final CassandraExceptionMapper exceptionMapper
    ) {
        this.cluster = cluster;
        this.exceptionMapper = exceptionMapper;
    }

    /**
     * Execute a query on the Cassandra cluster pointed to by the given Cluster configuration.
     *
     * @param query The query to execute
     * @return The query results if any
     * @throws com.datastax.driver.core.exceptions.NoHostAvailableException if no host in the cluster can be
     *                                                                      contacted successfully to execute this
     *                                                                      query.
     * @throws com.datastax.driver.core.exceptions.QueryExecutionException  if the query triggered an execution
     *                                                                      exception, i.e. an exception thrown by
     *                                                                      Cassandra when it cannot execute
     *                                                                      the query with the requested consistency
     *                                                                      level successfully.
     * @throws com.datastax.driver.core.exceptions.QueryValidationException if the query if invalid (syntax error,
     *                                                                      unauthorized or any other validation
     *                                                                      problem).
     */
    ResultSet executeQuery(@Nonnull @NonNull final String query) {
        try (final Session session = this.cluster.connect()) {
            // From documentation it doesn't look like ResultSet needs to be closed
            return session.execute(query);
        }
    }
}
