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

package com.netflix.metacat.connector.s3;

import com.facebook.presto.hive.ConditionalModule;
import com.facebook.presto.hive.HiveClientModule;
import com.facebook.presto.hive.HiveConnector;
import com.facebook.presto.hive.HiveConnectorFactory;
import com.facebook.presto.hive.HiveSessionProperties;
import com.facebook.presto.hive.HiveTableProperties;
import com.facebook.presto.hive.NoSecurityModule;
import com.facebook.presto.hive.ReadOnlySecurityModule;
import com.facebook.presto.hive.RebindSafeMBeanServer;
import com.facebook.presto.hive.SecurityConfig;
import com.facebook.presto.hive.SqlStandardSecurityModule;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorHandleResolver;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorRecordSinkProvider;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.security.ConnectorAccessControl;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.persist.PersistService;
import com.google.inject.persist.jpa.JpaPersistModule;
import com.google.inject.util.Modules;
import com.netflix.metacat.common.server.CommonModule;
import com.netflix.metacat.common.util.DataSourceManager;
import com.netflix.metacat.converters.ConvertersModule;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeModule;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;
import java.util.Map;

/**
 * s3 connector factory.
 */
public class S3ConnectorFactory extends HiveConnectorFactory {
    private static final String NAME = "metacat-s3";
    private final Map<String, String> optionalConfig;
    private final ClassLoader classLoader;
    private final TypeManager typeManager;

    /**
     * Constructor.
     * @param typeManager amanager
     * @param optionalConfig config
     * @param classLoader class loader
     */
    public S3ConnectorFactory(final TypeManager typeManager, final Map<String, String> optionalConfig,
        final ClassLoader classLoader) {
        super(NAME, optionalConfig, classLoader, null, typeManager);
        this.optionalConfig = optionalConfig;
        this.classLoader = classLoader;
        this.typeManager = typeManager;
    }

    @Override
    public Connector create(final String connectorId, final Map<String, String> config) {
        Preconditions.checkNotNull(config, "config is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            final HiveClientModule hiveClientModule = new HiveClientModule(connectorId, null, typeManager);
            final Module module = Modules.override(hiveClientModule).with(new S3Module());

            //JPA module
            final Map<String, Object> props = Maps.newHashMap(config);
            props.put("hibernate.connection.datasource",
                DataSourceManager.get().load(connectorId, config).get(connectorId));
            final Module jpaModule = new JpaPersistModule("s3").properties(props);

            final Bootstrap app = new Bootstrap(
                new NodeModule(),
                new MBeanModule(),
                new JsonModule(),
                module,
                ConditionalModule.installModuleIf(
                    SecurityConfig.class,
                    security -> "none".equalsIgnoreCase(security.getSecuritySystem()),
                    new NoSecurityModule()),
                ConditionalModule.installModuleIf(
                    SecurityConfig.class,
                    security -> "read-only".equalsIgnoreCase(security.getSecuritySystem()),
                    new ReadOnlySecurityModule()),
                ConditionalModule.installModuleIf(
                    SecurityConfig.class,
                    security -> "sql-standard".equalsIgnoreCase(security.getSecuritySystem()),
                    new SqlStandardSecurityModule()),
                binder -> {
                    final MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
                    binder.bind(MBeanServer.class).toInstance(new RebindSafeMBeanServer(platformMBeanServer));
                },
                jpaModule,
                new CommonModule(),
                new ConvertersModule()
            );

            final Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .setOptionalConfigurationProperties(optionalConfig)
                .initialize();

            final PersistService persistService = injector.getInstance(PersistService.class);
            persistService.start();

            final LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            final ConnectorMetadata metadata = injector.getInstance(ConnectorMetadata.class);
            final ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
            final ConnectorPageSourceProvider connectorPageSource =
                injector.getInstance(ConnectorPageSourceProvider.class);
            final ConnectorRecordSinkProvider recordSinkProvider =
                injector.getInstance(ConnectorRecordSinkProvider.class);
            final ConnectorHandleResolver handleResolver = injector.getInstance(ConnectorHandleResolver.class);
            final HiveSessionProperties hiveSessionProperties = injector.getInstance(HiveSessionProperties.class);
            final HiveTableProperties hiveTableProperties = injector.getInstance(HiveTableProperties.class);
            final ConnectorAccessControl accessControl = injector.getInstance(ConnectorAccessControl.class);

            return new HiveConnector(
                lifeCycleManager,
                metadata,
                splitManager,
                new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSource, classLoader),
                new ClassLoaderSafeConnectorRecordSinkProvider(recordSinkProvider, classLoader),
                new ClassLoaderSafeConnectorHandleResolver(handleResolver, classLoader),
                ImmutableSet.of(),
                hiveSessionProperties.getSessionProperties(),
                hiveTableProperties.getTableProperties(),
                accessControl);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

}
