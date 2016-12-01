/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.plugin.mysql;

import com.facebook.presto.plugin.jdbc.JdbcConnector;
import com.facebook.presto.plugin.jdbc.JdbcHandleResolver;
import com.facebook.presto.plugin.jdbc.JdbcRecordSetProvider;
import com.facebook.presto.plugin.jdbc.JdbcRecordSinkProvider;
import com.facebook.presto.plugin.jdbc.JdbcSplitManager;
import io.airlift.bootstrap.LifeCycleManager;

import javax.inject.Inject;

/**
 * Mysql connector.
 */
public class MySqlJdbcConnector extends JdbcConnector {
    /**
     * Constructor.
     * @param lifeCycleManager manager
     * @param jdbcMetadata jdbc metadata
     * @param jdbcSplitManager split manager
     * @param jdbcRecordSetProvider provider
     * @param jdbcHandleResolver resolver
     * @param jdbcRecordSinkProvider provider
     */
    @Inject
    public MySqlJdbcConnector(final LifeCycleManager lifeCycleManager,
        final MySqlJdbcMetadata jdbcMetadata,
        final JdbcSplitManager jdbcSplitManager,
        final JdbcRecordSetProvider jdbcRecordSetProvider,
        final JdbcHandleResolver jdbcHandleResolver,
        final JdbcRecordSinkProvider jdbcRecordSinkProvider) {
        super(lifeCycleManager, jdbcMetadata, jdbcSplitManager, jdbcRecordSetProvider, jdbcHandleResolver,
            jdbcRecordSinkProvider);
    }
}
