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
package com.netflix.metacat.main.api;

import com.netflix.metacat.common.server.properties.Config;
import io.swagger.jaxrs.listing.ApiListingResource;
import io.swagger.jaxrs.listing.SwaggerSerializers;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Jersey resource registrations for Metacat to configure Spring.
 *
 * @author tgianos
 * @since 1.1.0
 */
@Component
public class MetacatJerseyResources extends ResourceConfig {

    /**
     * Constructor registers the resource classes.
     *
     * @param config System configuration
     */
    @Autowired
    public MetacatJerseyResources(final Config config) {
        this.register(IndexResource.class);
        this.register(MetacatV1Resource.class);
        this.register(MetadataV1Resource.class);
        this.register(PartitionV1Resource.class);
        this.register(ResolverV1Resource.class);
        this.register(TagV1Resource.class);
        this.register(MetacatRestFilter.class);
        this.register(MetacatJsonProvider.class);
        this.property(ServletProperties.FILTER_STATIC_CONTENT_REGEX, "/(web|docs)/.*|/favicon.ico");

        if (config.isElasticSearchEnabled()) {
            this.register(SearchMetacatV1Resource.class);
        }

        // Swagger
        this.register(ApiListingResource.class);
        this.register(SwaggerSerializers.class);
    }
}
