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

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.common.server.util.DataSourceManager;
import com.netflix.metacat.common.server.util.JdbcUtil;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;

import java.io.Reader;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
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
     * @param jdbcUtil jdbc util
     * @param sql      query sql
     * @param item     identifier
     * @return list of results
     * @throws DataAccessException data access exception
     */
    @Transactional(readOnly = true)
    public static Set<String> getValues(final JdbcUtil jdbcUtil,
                                        final String sql,
                                        final Object item) throws DataAccessException {
        try {
            return jdbcUtil.getJdbcTemplate().query(sql, rs -> {
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
     * insertLookupValues.
     *
     * @param jdbcUtil jdbc util
     * @param sql      query sql
     * @param id       id
     * @param inserts  data to insert
     * @throws DataAccessException data access exception
     */
    @Transactional
    public static void batchInsertValues(final JdbcUtil jdbcUtil,
                                         final String sql,
                                         final Long id,
                                         final Set<String> inserts)
        throws DataAccessException {
        final List<Object[]> batch = new ArrayList<Object[]>();
        for (String insert : inserts) {
            batch.add(ImmutableList.of(id, insert).toArray());
        }
        jdbcUtil.getTransactionTemplate().execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(final TransactionStatus status) {
                jdbcUtil.getJdbcTemplate().batchUpdate(sql, batch);
            }
        });
    }

    /**
     * deleteLookupValues.
     *
     * @param jdbcUtil jdbc util
     * @param sql      query sql
     * @param id       identifier
     * @param deletes  data to delete
     * @throws DataAccessException data access exception
     */
    @Transactional
    public static void batchDeleteValues(final JdbcUtil jdbcUtil,
                                         final String sql,
                                         final Long id,
                                         final Set<String> deletes)
        throws DataAccessException {
        jdbcUtil.getTransactionTemplate().execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(final TransactionStatus status) {
                jdbcUtil.getJdbcTemplate()
                    .update(String.format(sql, "'" + Joiner.on("','").skipNulls().join(deletes) + "'"), id);
            }
        });
    }

    /**
     * deleteLookupValues.
     *
     * @param jdbcUtil jdbc util
     * @param sql      update sql
     * @param updates  data to delete
     * @throws DataAccessException data access exception
     */
    public static void batchUpdateValues(final JdbcUtil jdbcUtil,
                                         final String sql,
                                         final List<Object[]> updates)
        throws DataAccessException {
        jdbcUtil.getJdbcTemplate()
            .batchUpdate(sql, updates);
    }

    /**
     * load mysql data source.
     *
     * @param dataSourceManager data source manager to use
     * @param configLocation usermetadata config location
     * @throws Exception exception to throw
     */
    public static void loadMySqlDataSource(final DataSourceManager dataSourceManager,
        final String configLocation) throws Exception {

        final URL url = Thread.currentThread().getContextClassLoader().getResource(configLocation);
        final Path filePath;
        if (url != null) {
            filePath = Paths.get(url.toURI());
        } else {
            filePath = FileSystems.getDefault().getPath(configLocation);
        }
        Preconditions
            .checkState(filePath != null, "Unable to read from user metadata config file '%s'", configLocation);
        final Properties connectionProperties = new Properties();
        try (Reader reader = Files.newBufferedReader(filePath, Charsets.UTF_8)) {
            connectionProperties.load(reader);
        }
        dataSourceManager.load(UserMetadataService.NAME_DATASOURCE, connectionProperties);
    }
}



