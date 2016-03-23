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

import com.facebook.presto.hive.HiveClientModule;
import com.facebook.presto.hive.HiveConnector;
import com.facebook.presto.hive.HiveSessionProperties;
import com.facebook.presto.hive.HiveTableProperties;
import com.facebook.presto.hive.RebindSafeMBeanServer;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.security.ConnectorAccessControl;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import com.netflix.metacat.common.server.CommonModule;
import com.netflix.metacat.converters.ConvertersModule;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeModule;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Created by amajumdar on 1/21/15.
 */
public class MetacatHiveConnectorFactory implements ConnectorFactory {
    private final String name;
    private final Map<String, String> optionalConfig;
    private final ClassLoader classLoader;
    private final TypeManager typeManager;

    public MetacatHiveConnectorFactory(String name, Map<String, String> optionalConfig, ClassLoader classLoader,
            TypeManager typeManager)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.optionalConfig = checkNotNull(optionalConfig, "optionalConfig is null");
        this.classLoader = checkNotNull(classLoader, "classLoader is null");
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config)
    {
        checkNotNull(config, "config is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            HiveClientModule hiveClientModule = new HiveClientModule(connectorId, null, typeManager);
            MetacatHiveClientModule metacatHiveClientModule = new MetacatHiveClientModule();
            Module module = Modules.override(hiveClientModule).with(metacatHiveClientModule);
            Bootstrap app = new Bootstrap(
                    new NodeModule(),
                    new MBeanModule(),
                    new JsonModule(),
                    new CommonModule(),
                    new ConvertersModule(),
                    module,
                    binder -> {
                        MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
                        binder.bind(MBeanServer.class).toInstance(new RebindSafeMBeanServer(platformMBeanServer));
                    }
            );

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();
            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            ConnectorMetadata metadata = injector.getInstance(ConnectorMetadata.class);
            ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
            ConnectorPageSourceProvider connectorPageSource = injector.getInstance(ConnectorPageSourceProvider.class);
            ConnectorRecordSinkProvider recordSinkProvider = injector.getInstance(ConnectorRecordSinkProvider.class);
            ConnectorHandleResolver handleResolver = injector.getInstance(ConnectorHandleResolver.class);
            HiveSessionProperties hiveSessionProperties = injector.getInstance(HiveSessionProperties.class);
            HiveTableProperties hiveTableProperties = injector.getInstance(HiveTableProperties.class);
            ConnectorAccessControl accessControl = injector.getInstance(ConnectorAccessControl.class);

            return new HiveConnector(
                    lifeCycleManager,
                    metadata,
                    splitManager,
                    connectorPageSource,
                    recordSinkProvider,
                    handleResolver,
                    ImmutableSet.of(),
                    hiveSessionProperties.getSessionProperties(),
                    hiveTableProperties.getTableProperties(),
                    accessControl);
        }
        catch (Throwable e) {
            throw Throwables.propagate(e);
        }
    }
}
