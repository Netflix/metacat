/*
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
 */

package com.netflix.metacat.connector.hive.sql;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.netflix.metacat.common.server.connectors.exception.ConnectorException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Types;
import java.util.List;

/**
 * This class is used to generate the sequence ids.
 * @author amajumdar
 * @since 1.1.x
 */
@Slf4j
@Transactional("hiveTxManager")
public class SequenceGeneration {
    private static final String SEQUENCE_NAME_PARTITION = "org.apache.hadoop.hive.metastore.model.MPartition";
    private static final String SEQUENCE_NAME_SERDES = "org.apache.hadoop.hive.metastore.model.MSerDeInfo";
    private static final String SEQUENCE_NAME_SDS = "org.apache.hadoop.hive.metastore.model.MStorageDescriptor";
    private final JdbcTemplate jdbcTemplate;

    /**
     * Constructor.
     * @param jdbcTemplate JDBC template
     */
    public SequenceGeneration(final JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Returns the current sequence ids and increments the sequence ids by the given <code>size</code>.
     * @param size number of records getting inserted
     * @return current sequence ids
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.REPEATABLE_READ)
    public PartitionSequenceIds newPartitionSequenceIds(final int size) {
        //
        // Get the sequence ids for partitions, sds and serde tables. Lock and update the sequence id to the number of
        // partitions we need to insert.
        //
        final PartitionSequenceIds result = new PartitionSequenceIds();
        try {
            jdbcTemplate.query(SQL.SEQUENCE_NEXT_VAL,
                new SqlParameterValue[] {new SqlParameterValue(Types.VARCHAR, SEQUENCE_NAME_PARTITION),
                    new SqlParameterValue(Types.VARCHAR, SEQUENCE_NAME_SERDES),
                    new SqlParameterValue(Types.VARCHAR, SEQUENCE_NAME_SDS), },
                (RowCallbackHandler) rs -> {
                    final String sequenceName = rs.getString("sequence_name");
                    final Long nextVal = rs.getLong("next_val");
                    switch (sequenceName) {
                    case SEQUENCE_NAME_PARTITION:
                        result.setPartId(nextVal);
                        break;
                    case SEQUENCE_NAME_SDS:
                        result.setSdsId(nextVal);
                        break;
                    case SEQUENCE_NAME_SERDES:
                        result.setSerdeId(nextVal);
                        break;
                    default:
                        throw new IllegalStateException("Failed retrieving the sequence numbers.");
                    }
                });
        } catch (EmptyResultDataAccessException e) {
            log.warn("Failed getting the sequence ids for partition", e);
        } catch (Exception e) {
            throw new IllegalStateException("Failed retrieving the sequence numbers.");
        }

        try {
            final List<Object[]> insertValues = Lists.newArrayList();
            final List<Object[]> updateValues = Lists.newArrayList();
            if (result.getPartId() == null) {
                result.setPartId(1L);
                insertValues.add(new Object[] {result.getPartId() + size, SEQUENCE_NAME_PARTITION });
            } else {
                updateValues.add(new Object[] {result.getPartId() + size, SEQUENCE_NAME_PARTITION });
            }
            if (result.getSdsId() == null) {
                result.setSdsId(1L);
                insertValues.add(new Object[] {result.getSdsId() + size, SEQUENCE_NAME_SDS });
            } else {
                updateValues.add(new Object[] {result.getSdsId() + size, SEQUENCE_NAME_SDS });
            }
            if (result.getSerdeId() == null) {
                result.setSerdeId(1L);
                insertValues.add(new Object[] {result.getSerdeId() + size, SEQUENCE_NAME_SERDES });
            } else {
                updateValues.add(new Object[] {result.getSerdeId() + size, SEQUENCE_NAME_SERDES });
            }
            if (!insertValues.isEmpty()) {
                jdbcTemplate.batchUpdate(SQL.SEQUENCE_INSERT_VAL, insertValues,
                    new int[] {Types.BIGINT, Types.VARCHAR });
            }
            if (!updateValues.isEmpty()) {
                jdbcTemplate.batchUpdate(SQL.SEQUENCE_UPDATE_VAL, updateValues,
                    new int[] {Types.BIGINT, Types.VARCHAR });
            }
            return result;
        } catch (Exception e) {
            throw new ConnectorException("Failed updating the sequence ids for partition", e);
        }
    }

    @VisibleForTesting
    private static class SQL {
        static final String SEQUENCE_NEXT_VAL =
            "SELECT NEXT_VAL, SEQUENCE_NAME FROM SEQUENCE_TABLE WHERE SEQUENCE_NAME in (?,?,?) FOR UPDATE";
        static final String SEQUENCE_INSERT_VAL =
            "INSERT INTO SEQUENCE_TABLE(NEXT_VAL,SEQUENCE_NAME) VALUES (?,?)";
        static final String SEQUENCE_UPDATE_VAL =
            "UPDATE SEQUENCE_TABLE SET NEXT_VAL=? WHERE SEQUENCE_NAME=?";
    }
}
