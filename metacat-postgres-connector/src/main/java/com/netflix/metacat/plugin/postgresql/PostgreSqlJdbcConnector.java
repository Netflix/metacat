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

package com.netflix.metacat.plugin.postgresql;

import com.facebook.presto.plugin.jdbc.JdbcConnector;
import com.facebook.presto.plugin.jdbc.JdbcHandleResolver;
import com.facebook.presto.plugin.jdbc.JdbcRecordSetProvider;
import com.facebook.presto.plugin.jdbc.JdbcRecordSinkProvider;
import com.facebook.presto.plugin.jdbc.JdbcSplitManager;
import io.airlift.bootstrap.LifeCycleManager;

import javax.inject.Inject;

/**
 * Created by amajumdar on 9/30/15.
 */
public class PostgreSqlJdbcConnector extends JdbcConnector{
    @Inject
    public PostgreSqlJdbcConnector(LifeCycleManager lifeCycleManager,
            PostgreSqlJdbcMetadata jdbcMetadata,
            JdbcSplitManager jdbcSplitManager,
            JdbcRecordSetProvider jdbcRecordSetProvider,
            JdbcHandleResolver jdbcHandleResolver,
            JdbcRecordSinkProvider jdbcRecordSinkProvider) {
        super(lifeCycleManager, jdbcMetadata, jdbcSplitManager, jdbcRecordSetProvider, jdbcHandleResolver,
                jdbcRecordSinkProvider);
    }
}
