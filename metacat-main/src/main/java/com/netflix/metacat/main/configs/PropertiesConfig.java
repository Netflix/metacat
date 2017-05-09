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

import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.properties.DefaultConfigImpl;
import com.netflix.metacat.common.server.properties.MetacatProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for binding Metacat properties.
 *
 * @author tgianos
 * @since 1.1.0
 */
@Configuration
public class PropertiesConfig {

    /**
     * Static properties bindings.
     *
     * @return The metacat properties.
     */
    @Bean
    @ConfigurationProperties("metacat")
    public MetacatProperties metacatProperties() {
        return new MetacatProperties();
    }

    /**
     * Get the configuration abstraction for use in metacat.
     *
     * @param metacatProperties The overall metacat properties to use
     * @return The configuration object
     */
    @Bean
    public Config config(final MetacatProperties metacatProperties) {
        return new DefaultConfigImpl(metacatProperties);
    }
}
