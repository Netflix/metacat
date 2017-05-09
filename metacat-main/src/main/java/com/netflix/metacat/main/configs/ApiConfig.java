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

import com.netflix.metacat.main.api.IndexResource;
import com.netflix.metacat.main.api.MetacatJsonProvider;
import com.netflix.metacat.main.api.MetacatRestFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration for the API tier.
 *
 * @author tgianos
 * @since 1.1.0
 */
@Configuration
public class ApiConfig {

    /**
     * Index resource.
     *
     * @return The index resource
     */
    @Bean
    public IndexResource indexResource() {
        return new IndexResource();
    }

    /**
     * The rest filter.
     *
     * @return The rest filter
     */
    @Bean
    public MetacatRestFilter metacatRestFilter() {
        return new MetacatRestFilter();
    }

    /**
     * Json Provider for Jersey.
     *
     * @return The JSON Provider.
     */
    @Bean
    public MetacatJsonProvider metacatJsonProvider() {
        return new MetacatJsonProvider();
    }
}
