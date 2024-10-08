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

import com.google.common.collect.Sets;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.common.server.util.DataSourceManager;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.FileSystems;
import java.util.Properties;
import java.util.Set;

/**
 * MySqlServiceUtil.
 *
 * @author zhenl
 * @since 1.1.0
 */
public final class MySqlServiceUtil {
    private MySqlServiceUtil() {
    }

    /**
     * Returns the list of string having the input ids.
     *
     * @param jdbcTemplate jdbc template
     * @param sql          query sql
     * @param item         identifier
     * @return list of results
     */
    public static Set<String> getValues(final JdbcTemplate jdbcTemplate,
                                        final String sql,
                                        final Object item) {
        try {
            return jdbcTemplate.query(sql, rs -> {
                final Set<String> result = Sets.newHashSet();
                while (rs.next()) {
                    result.add(rs.getString("value"));
                }
                return result;
            }, item);
        } catch (EmptyResultDataAccessException e) {
            return Sets.newHashSet();
        }
    }

    /**
     * load mysql data source.
     *
     * @param dataSourceManager data source manager to use
     * @param configLocation    usermetadata config location
     * @throws Exception exception to throw
     */
    public static void loadMySqlDataSource(final DataSourceManager dataSourceManager,
                                           final String configLocation) throws Exception {

        URL url = Thread.currentThread().getContextClassLoader().getResource(configLocation);
        if (url == null) {
            url = FileSystems.getDefault().getPath(configLocation).toUri().toURL();
        }
        final Properties connectionProperties = new Properties();
        try (InputStream is = url.openStream()) {
            connectionProperties.load(is);
        } catch (Exception e) {
            throw new Exception(String.format("Unable to read from user metadata config file %s", configLocation), e);
        }
        dataSourceManager.load(UserMetadataService.NAME_DATASOURCE, connectionProperties);
    }

    /**
     * Create a JDBC template with a query timeout.
     *
     * @param dataSource data source
     * @param timeoutSec query timeout, in sec
     * @return the JDBC template
     */
    public static JdbcTemplate createJdbcTemplate(final DataSource dataSource, final int timeoutSec) {
        final JdbcTemplate result = new JdbcTemplate(dataSource);
        result.setQueryTimeout(timeoutSec);
        return result;
    }
}



