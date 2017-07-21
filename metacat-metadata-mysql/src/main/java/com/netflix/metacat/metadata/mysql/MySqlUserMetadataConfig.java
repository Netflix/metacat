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

package com.netflix.metacat.metadata.mysql;

import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.properties.MetacatProperties;
import com.netflix.metacat.common.server.usermetadata.LookupService;
import com.netflix.metacat.common.server.usermetadata.TagService;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.common.server.util.DataSourceManager;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * MySql UserMetadata Config.
 *
 * @author zhenl
 * @since 1.1.0
 */
@Configuration
@ConditionalOnProperty(value = "metacat.mysqlmetadataservice.enabled", havingValue = "true")
public class MySqlUserMetadataConfig {


    /**
     * User Metadata service.
     *
     * @param dataSource  The data ource to use
     * @param config      System config to use
     * @param metacatJson Json Utilities to use
     * @return User metadata service based on MySql
     */
    @Bean
    public UserMetadataService userMetadataService(
        @Qualifier("mySqlDataSource") final DataSource dataSource,
        final Config config,
        final MetacatJson metacatJson
    ) {
        return new MysqlUserMetadataService(dataSource, metacatJson, config);
    }

    /**
     * Lookup service.
     *
     * @param dataSource The data source to use
     * @param config     System configuration to use
     * @return Lookup service backed by MySQL
     */
    @Bean
    public LookupService lookupService(
        @Qualifier("mySqlDataSource") final DataSource dataSource,
        final Config config) {
        return new MySqlLookupService(config, dataSource);
    }

    /**
     * The tag service to use.
     *
     * @param dataSource          The data source to use
     * @param config              System config to use
     * @param metacatJson         Json Utilities to use
     * @param lookupService       Look up service implementation to use
     * @param userMetadataService User metadata service implementation to use
     * @return The tag service implementation backed by MySQL
     */
    @Bean
    public TagService tagService(
        @Qualifier("mySqlDataSource") final DataSource dataSource,
        final Config config,
        final MetacatJson metacatJson,
        final LookupService lookupService,
        final UserMetadataService userMetadataService
    ) {
        return new MySqlTagService(config, dataSource, lookupService, metacatJson, userMetadataService);
    }

    /**
     * mySql DataSource.
     *
     * @param dataSourceManager data source manager
     * @param metacatProperties metacat properties
     * @return data source
     * @throws Exception exception
     */
    @Bean
    public DataSource mySqlDataSource(final DataSourceManager dataSourceManager,
        final MetacatProperties metacatProperties) throws Exception {
        MySqlServiceUtil.loadMySqlDataSource(dataSourceManager,
            metacatProperties.getUsermetadata().getConfig().getLocation());
        return dataSourceManager.get(UserMetadataService.NAME_DATASOURCE);
    }

}
