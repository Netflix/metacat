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
