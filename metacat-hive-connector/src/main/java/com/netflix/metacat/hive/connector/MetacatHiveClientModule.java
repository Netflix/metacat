package com.netflix.metacat.hive.connector;

import com.facebook.presto.hive.NoAccessControl;
import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.security.ConnectorAccessControl;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

/**
 * Created by amajumdar on 4/17/15.
 */
public class MetacatHiveClientModule implements Module {

    @Override
    public void configure(Binder binder) {
        binder.bind(HiveMetastore.class).to(BaseMetacatHiveMetastore.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorMetadata.class).to(HiveDetailMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(HiveSplitDetailManager.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorAccessControl.class).to(NoAccessControl.class).in(Scopes.SINGLETON);
    }
}
