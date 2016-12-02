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

package com.netflix.metacat.hive.connector;

import com.facebook.presto.hive.NoAccessControl;
import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.security.ConnectorAccessControl;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.netflix.metacat.hive.connector.util.ConverterUtil;

/**
 * Created by amajumdar on 4/17/15.
 */
public class MetacatHiveClientModule implements Module {

    @Override
    public void configure(final Binder binder) {
        binder.bind(HiveMetastore.class).to(BaseMetacatHiveMetastore.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorMetadata.class).to(HiveDetailMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(HiveSplitDetailManager.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorAccessControl.class).to(NoAccessControl.class).in(Scopes.SINGLETON);
        binder.bind(ConverterUtil.class).in(Scopes.SINGLETON);
    }
}
