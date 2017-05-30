/*
 *       Copyright 2017 Netflix, Inc.
 *          Licensed under the Apache License, Version 2.0 (the "License");
 *          you may not use this file except in compliance with the License.
 *          You may obtain a copy of the License at
 *              http://www.apache.org/licenses/LICENSE-2.0
 *          Unless required by applicable law or agreed to in writing, software
 *          distributed under the License is distributed on an "AS IS" BASIS,
 *          WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *          See the License for the specific language governing permissions and
 *          limitations under the License.
 */

package com.netflix.metacat.usermetadata.mysql;

import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.common.server.util.DataSourceManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * MySql UserMetadata Config.
 *
 * @author zhenl
 * @since 1.1.0
 */
@Configuration
@Profile({"mysql-usermetadata"})
public class MySqlUserMetadataConfig {


    /**
     * User Metadata service.
     *
     * @param dataSourceManager The datasource manager to use
     * @param config            System config to use
     * @param metacatJson       Json Utilities to use
     * @return User metadata service based on MySql
     */
    @Bean
    public UserMetadataService userMetadataService(
        final DataSourceManager dataSourceManager,
        final Config config,
        final MetacatJson metacatJson
    ) {
        return new MysqlUserMetadataService(dataSourceManager, metacatJson, config);
    }
}
