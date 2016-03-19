package com.netflix.metacat.plugin.mysql;

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
public class MySqlJdbcConnector extends JdbcConnector{
    @Inject
    public MySqlJdbcConnector(LifeCycleManager lifeCycleManager,
            MySqlJdbcMetadata jdbcMetadata,
            JdbcSplitManager jdbcSplitManager,
            JdbcRecordSetProvider jdbcRecordSetProvider,
            JdbcHandleResolver jdbcHandleResolver,
            JdbcRecordSinkProvider jdbcRecordSinkProvider) {
        super(lifeCycleManager, jdbcMetadata, jdbcSplitManager, jdbcRecordSetProvider, jdbcHandleResolver,
                jdbcRecordSinkProvider);
    }
}
