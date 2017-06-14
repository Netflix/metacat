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
package com.netflix.metacat;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

/**
 * Servlet entry point for Spring when deployed as a WAR file.
 *
 * @author tgianos
 * @since 1.1.0
 */
@SpringBootApplication
@ComponentScan(excludeFilters = @ComponentScan.Filter(
    type = FilterType.REGEX, pattern = "com.netflix.metacat.connector.hive.*"))
public class MetacatWar extends SpringBootServletInitializer {

    /**
     * Constructor.
     */
    public MetacatWar() {
    }

    /**
     * Main.
     *
     * @param args Program arguments.
     */
    public static void main(final String[] args) {
        new MetacatWar()
            .configure(new SpringApplicationBuilder(MetacatWar.class))
            .run(args);
    }
}
