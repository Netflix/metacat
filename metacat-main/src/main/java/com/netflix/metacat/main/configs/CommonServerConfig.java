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
package com.netflix.metacat.main.configs;

import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.json.MetacatJsonLocator;
import com.netflix.metacat.common.server.converter.ConverterUtil;
import com.netflix.metacat.common.server.converter.DozerTypeConverter;
import com.netflix.metacat.common.server.converter.TypeConverterFactory;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.model.Lookup;
import com.netflix.metacat.common.server.model.TagItem;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.properties.MetacatProperties;
import com.netflix.metacat.common.server.util.DataSourceManager;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.context.event.SimpleApplicationEventMulticaster;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Common configuration for Metacat based on classes found in the common server module.
 *
 * @author tgianos
 * @since 1.1.0
 */
@Configuration
public class CommonServerConfig {

    /**
     * Metacat JSON Handler.
     *
     * @return The JSON handler
     */
    @Bean
    public MetacatJson metacatJson() {
        // TODO: Static reference boo
        return MetacatJsonLocator.INSTANCE;
    }

    /**
     * The data source manager to use.
     *
     * @return The data source manager
     */
    @Bean
    public DataSourceManager dataSourceManager() {
        // TODO: Static reference boo
        return DataSourceManager.get();
    }

    /**
     * The event bus abstraction to use.
     *
     * @param eventPublisher   The synchronous event publisher
     * @param eventMulticaster The asynchronous event publisher
     * @return The event bus to use.
     */
    @Bean
    public MetacatEventBus metacatEventBus(
        final ApplicationEventPublisher eventPublisher,
        final ApplicationEventMulticaster eventMulticaster
    ) {
        return new MetacatEventBus(eventPublisher, eventMulticaster);
    }

    /**
     * Get a task executor for executing tasks asynchronously that don't need to be scheduled at a recurring rate.
     *
     * @param metacatProperties The metacat properties to get number of executor threads from.
     *                          Likely best to do one more than number of CPUs
     * @return The task executor the system to use
     */
    @Bean
    public AsyncTaskExecutor taskExecutor(final MetacatProperties metacatProperties) {
        final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(metacatProperties.getEvent().getBus().getExecutor().getThread().getCount());
        return executor;
    }

    /**
     * A multicast (async) event publisher to replace the synchronous one used by Spring via the ApplicationContext.
     *
     * @param taskExecutor The task executor to use
     * @return The application event multicaster to use
     */
    @Bean
    public ApplicationEventMulticaster applicationEventMulticaster(final TaskExecutor taskExecutor) {
        final SimpleApplicationEventMulticaster applicationEventMulticaster = new SimpleApplicationEventMulticaster();
        applicationEventMulticaster.setTaskExecutor(taskExecutor);
        return applicationEventMulticaster;
    }

    /**
     * The type converter factory to use.
     *
     * @param config The system configuration
     * @return The type converter factory
     */
    @Bean
    public TypeConverterFactory typeConverterFactory(final Config config) {
        return new TypeConverterFactory(config);
    }

    /**
     * The dozer type converter to use.
     *
     * @param typeConverterFactory The type converter factory to use
     * @return type converter
     */
    @Bean
    public DozerTypeConverter dozerTypeConverter(final TypeConverterFactory typeConverterFactory) {
        return new DozerTypeConverter(typeConverterFactory);
    }

    /**
     * Converter utility bean.
     *
     * @param dozerTypeConverter The Dozer type converter to use.
     * @return The converter util instance
     */
    @Bean
    public ConverterUtil converterUtil(final DozerTypeConverter dozerTypeConverter) {
        return new ConverterUtil(dozerTypeConverter);
    }

    /**
     * The Lookup model object.
     *
     * @param config System configuration
     * @return Lookup bean
     */
    @Bean
    public Lookup lookup(final Config config) {
        return new Lookup(config);
    }

    /**
     * The tag item bean.
     *
     * @param config System configuration
     * @return The tag item bean
     */
    @Bean
    public TagItem tagItem(final Config config) {
        return new TagItem(config);
    }

    /**
     * Get the ThreadServiceManager.
     *
     * @param config System configuration
     * @return The thread service manager to use
     */
    @Bean
    public ThreadServiceManager threadServiceManager(final Config config) {
        return new ThreadServiceManager(config);
    }
}
