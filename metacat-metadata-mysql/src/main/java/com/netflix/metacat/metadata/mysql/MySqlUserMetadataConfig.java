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
import com.netflix.metacat.common.server.converter.ConverterUtil;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.properties.MetacatProperties;
import com.netflix.metacat.common.server.usermetadata.MetadataPreMergeInterceptor;
import com.netflix.metacat.common.server.usermetadata.MetadataPreMergeInterceptorImpl;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.common.server.usermetadata.LookupService;
import com.netflix.metacat.common.server.usermetadata.TagService;
import com.netflix.metacat.common.server.usermetadata.ParentChildRelMetadataService;
import com.netflix.metacat.common.server.usermetadata.MetadataInterceptor;
import com.netflix.metacat.common.server.usermetadata.MetadataInterceptorImpl;
import com.netflix.metacat.common.server.util.DataSourceManager;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

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
     * business Metadata Manager.
     * @return business Metadata Manager
     */
    @Bean
    @ConditionalOnMissingBean(MetadataInterceptor.class)
    public MetadataInterceptor businessMetadataManager(
    ) {
        return new MetadataInterceptorImpl();
    }

    /**
     * MetadataSQLInterceptor layer.
     * @return MetadataSQLInterceptor
     */
    @Bean
    @ConditionalOnMissingBean(MetadataPreMergeInterceptor.class)
    public MetadataPreMergeInterceptor metadataPreMergeInterceptor(
    ) {
        return new MetadataPreMergeInterceptorImpl();
    }

    /**
     * User Metadata service.
     *
     * @param jdbcTemplate JDBC template
     * @param config       System config to use
     * @param metacatJson  Json Utilities to use
     * @param metadataInterceptor  business metadata manager
     * @return User metadata service based on MySql
     */
    @Bean
    public UserMetadataService userMetadataService(
        @Qualifier("metadataJdbcTemplate") final JdbcTemplate jdbcTemplate,
        final Config config,
        final MetacatJson metacatJson,
        final MetadataInterceptor metadataInterceptor,
        final MetadataPreMergeInterceptor metadataPreMergeInterceptor
        ) {
        return new MysqlUserMetadataService(jdbcTemplate, metacatJson, config, metadataInterceptor, metadataPreMergeInterceptor);
    }


    /**
     * Lookup service.
     *
     * @param jdbcTemplate JDBC template
     * @param config       System configuration to use
     * @return Lookup service backed by MySQL
     */
    @Bean
    public LookupService lookupService(
        @Qualifier("metadataJdbcTemplate") final JdbcTemplate jdbcTemplate,
        final Config config) {
        return new MySqlLookupService(config, jdbcTemplate);
    }

    /**
     * The tag service to use.
     *
     * @param jdbcTemplate        JDBC template
     * @param config              System config to use
     * @param metacatJson         Json Utilities to use
     * @param lookupService       Look up service implementation to use
     * @param userMetadataService User metadata service implementation to use
     * @return The tag service implementation backed by MySQL
     */
    @Bean
    public TagService tagService(
        @Qualifier("metadataJdbcTemplate") final JdbcTemplate jdbcTemplate,
        final Config config,
        final MetacatJson metacatJson,
        final LookupService lookupService,
        final UserMetadataService userMetadataService
    ) {
        return new MySqlTagService(config, jdbcTemplate, lookupService, metacatJson, userMetadataService);
    }

    /**
     * The parentChildRelMetadataService to use.
     *
     * @param jdbcTemplate        JDBC template
     * @return The parentChildRelMetadataService implementation backed by MySQL
     */
    @Bean
    ParentChildRelMetadataService parentChildRelMetadataService(
        @Qualifier("metadataJdbcTemplate") final JdbcTemplate jdbcTemplate,
        final ConverterUtil converterUtil
    ) {
        return new MySqlParentChildRelMetaDataService(jdbcTemplate, converterUtil);
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
    public DataSource metadataDataSource(final DataSourceManager dataSourceManager,
                                         final MetacatProperties metacatProperties) throws Exception {
        MySqlServiceUtil.loadMySqlDataSource(dataSourceManager,
            metacatProperties.getUsermetadata().getConfig().getLocation());
        return dataSourceManager.get(UserMetadataService.NAME_DATASOURCE);
    }

    /**
     * mySql metadata Transaction Manager.
     *
     * @param mySqlDataSource metadata data source
     * @return metadata transaction manager
     */
    @Bean
    public DataSourceTransactionManager metadataTxManager(
        @Qualifier("metadataDataSource") final DataSource mySqlDataSource) {
        return new DataSourceTransactionManager(mySqlDataSource);
    }

    /**
     * mySql metadata JDBC template.
     *
     * @param mySqlDataSource metadata data source
     * @param config System config to use
     * @return metadata JDBC template
     */
    @Bean
    public JdbcTemplate metadataJdbcTemplate(
        @Qualifier("metadataDataSource") final DataSource mySqlDataSource,
        final Config config) {
        return MySqlServiceUtil.createJdbcTemplate(
            mySqlDataSource, config.getMetadataQueryTimeout());
    }
}
