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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.json.MetacatJsonLocator;
import com.netflix.metacat.common.server.converter.ConverterUtil;
import com.netflix.metacat.common.server.converter.DozerTypeConverter;
import com.netflix.metacat.common.server.converter.TypeConverterFactory;
import com.netflix.metacat.common.server.events.MetacatApplicationEventMulticaster;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.events.MetacatEventListenerFactory;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.properties.MetacatProperties;
import com.netflix.metacat.common.server.util.DataSourceManager;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.spectator.api.Registry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.AnnotationConfigUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListenerFactory;

/**
 * Common configuration for Metacat based on classes found in the common server module.
 *
 * @author tgianos
 * @since 1.1.0
 */
@Configuration
public class CommonServerConfig {

    /**
     * An object mapper bean to use if none already exists.
     *
     * @return JSON object mapper
     */
    @Bean
    @ConditionalOnMissingBean(ObjectMapper.class)
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .setSerializationInclusion(JsonInclude.Include.ALWAYS);
    }

    /**
     * Metacat JSON Handler.
     *
     * @return The JSON handler
     */
    @Bean
    public MetacatJson metacatJson() {
        return new MetacatJsonLocator();
    }

    /**
     * The data source manager to use.
     *
     * @return The data source manager
     */
    @Bean
    public DataSourceManager dataSourceManager() {
        return DataSourceManager.get();
    }

    /**
     * The event bus abstraction to use.
     *
     * @param applicationEventMulticaster The asynchronous event publisher
     * @param registry         registry for spectator
     * @return The event bus to use.
     */
    @Bean
    public MetacatEventBus eventBus(
        final MetacatApplicationEventMulticaster applicationEventMulticaster,
        final Registry registry
    ) {
        return new MetacatEventBus(applicationEventMulticaster, registry);
    }

    /**
     * The application event multicaster to use.
     * @param registry         registry for spectator
     * @param metacatProperties The metacat properties to get number of executor threads from.
     *                          Likely best to do one more than number of CPUs
     * @return The application event multicaster to use.
     */
    @Bean
    public MetacatApplicationEventMulticaster applicationEventMulticaster(final Registry registry,
                                                                          final MetacatProperties metacatProperties) {
        return new MetacatApplicationEventMulticaster(registry, metacatProperties);
    }

    /**
     * Default event listener factory.
     * @return The application event multicaster to use.
     */
    @ConditionalOnMissingBean
    @Bean(AnnotationConfigUtils.EVENT_LISTENER_FACTORY_BEAN_NAME)
    public EventListenerFactory eventListenerFactory() {
        return new MetacatEventListenerFactory();
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
     * Get the ThreadServiceManager.
     * @param registry registry for spectator
     * @param config System configuration
     * @return The thread service manager to use
     */
    @Bean
    public ThreadServiceManager threadServiceManager(final Registry registry, final Config config) {
        return new ThreadServiceManager(registry, config);
    }
}
