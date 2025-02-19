/*
 *
 *  Copyright 2015 Netflix, Inc.
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
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration for Swagger via SpringDoc.
 *
 * see: https://springdoc.org/#migrating-from-springfox
 * @author tgianos
 * @since 1.1.0
 */
@Configuration
@ConditionalOnProperty(value = "springdoc.documentation.swagger-ui.enabled", havingValue = "true")
public class SwaggerConfig {
    /**
     * Configure Spring Fox.
     *
     * @param config The configuration
     * @return The spring fox docket.
     */
    @Bean
    public OpenAPI api(final Config config) {
        return new OpenAPI()
            .info(
                new Info()
                    .title("Metacat API")
                    .description("The set of APIs available in this version of metacat")
                    .version("1.1.0")
                    .license(new License().name("Apache 2.0").url("http://springdoc.org"))
                    .contact(new Contact().name("Netflix, Inc.").url("https://jobs.netflix.com/"))
            );
    }

    //TODO: Update with more detailed swagger configurations
    //      see: http://tinyurl.com/glla6vc
}
