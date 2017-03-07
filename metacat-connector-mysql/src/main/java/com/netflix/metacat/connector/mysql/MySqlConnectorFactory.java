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
package com.netflix.metacat.connector.mysql;

import com.google.common.collect.Lists;
import com.netflix.metacat.connector.jdbc.JdbcConnectorFactory;
import lombok.NonNull;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * MySql implementation of a connector factory.
 *
 * @author tgianos
 * @since 0.1.52
 */
class MySqlConnectorFactory extends JdbcConnectorFactory {

    /**
     * Constructor.
     *
     * @param name          The catalog name
     * @param configuration The catalog configuration
     */
    MySqlConnectorFactory(
        @Nonnull @NonNull final String name,
        @Nonnull @NonNull final Map<String, String> configuration
    ) {
        super(name, Lists.newArrayList(new MySqlConnectorModule(name, configuration)));
    }
}
