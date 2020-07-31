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

import com.google.common.collect.Lists;
import com.netflix.metacat.common.server.properties.Config;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import springfox.bean.validators.configuration.BeanValidatorPluginsConfiguration;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * Spring configuration for Swagger via SpringFox.
 *
 * see: https://github.com/springfox/springfox
 * @author tgianos
 * @since 1.1.0
 */
@Configuration
@ConditionalOnProperty(value = "springfox.documentation.swagger-ui.enabled", havingValue = "true")
@EnableSwagger2
@Import(BeanValidatorPluginsConfiguration.class)
public class SwaggerConfig {
    /**
     * Configure Spring Fox.
     *
     * @param config The configuration
     * @return The spring fox docket.
     */
    @Bean
    public Docket api(final Config config) {
        return new Docket(DocumentationType.SWAGGER_2)
            .apiInfo(
                /**
                 * public ApiInfo(
                 String title,
                 String description,
                 String version,
                 String termsOfServiceUrl,
                 Contact contact,
                 String license,
                 String licenseUrl,
                 Collection<VendorExtension> vendorExtensions)
                 */
                new ApiInfo(
                    "Metacat API",
                    "The set of APIs available in this version of metacat",
                    "1.1.0", // TODO: Swap out with dynamic from config
                    null,
                    new Contact("Netflix, Inc.", "https://jobs.netflix.com/", null),
                    "Apache 2.0",
                    "http://www.apache.org/licenses/LICENSE-2.0",
                    Lists.newArrayList()
                )
            )
            .select()
            .apis(RequestHandlerSelectors.basePackage("com.netflix.metacat.main.api"))
            .paths(PathSelectors.any())
            .build()
            .pathMapping("/")
            .useDefaultResponseMessages(false);
    }

    //TODO: Update with more detailed swagger configurations
    //      see: http://tinyurl.com/glla6vc
}
