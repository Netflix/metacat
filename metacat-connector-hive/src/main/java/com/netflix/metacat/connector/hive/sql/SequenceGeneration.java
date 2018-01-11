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
import com.netflix.metacat.common.server.connectors.exception.ConnectorException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * This class is used to generate the sequence ids.
 *
 * @author amajumdar
 * @since 1.1.x
 */
@Slf4j
@Transactional("hiveTxManager")
public class SequenceGeneration {
    /**
     * MPartition sequence number.
     **/
    public static final String SEQUENCE_NAME_PARTITION = "org.apache.hadoop.hive.metastore.model.MPartition";
    /**
     * MSerDeInfo sequence number.
     **/
    public static final String SEQUENCE_NAME_SERDES = "org.apache.hadoop.hive.metastore.model.MSerDeInfo";
    /**
     * MStorageDescriptor sequence number.
     **/
    public static final String SEQUENCE_NAME_SDS = "org.apache.hadoop.hive.metastore.model.MStorageDescriptor";
    private final JdbcTemplate jdbcTemplate;

    /**
     * Constructor.
     *
     * @param jdbcTemplate JDBC template
     */
    public SequenceGeneration(final JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Returns the current sequence ids and increments the sequence ids by the given <code>size</code>.
     *
     * @param size              number of records getting inserted
     * @param sequenceParamName the sequence Parameter Name
     * @return current sequence ids
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Long newPartitionSequenceIdByName(final int size, final String sequenceParamName) {
        Long result = null;
        try {
            //Get current sequence number
            result = jdbcTemplate.queryForObject(SQL.SEQUENCE_NEXT_VAL_BYNAME,
                new Object[]{sequenceParamName}, Long.class);
        } catch (DataAccessException e) {
            log.warn("Failed getting the sequence ids for partition", e);
        } catch (Exception e) {
            throw new IllegalStateException("Failed retrieving the sequence numbers.");
        }

        try {
            if (result == null) {
                result = 1L; //init to 1L in case there's no records
                jdbcTemplate.update(SQL.SEQUENCE_INSERT_VAL, result + size, sequenceParamName);
            } else {
                jdbcTemplate.update(SQL.SEQUENCE_UPDATE_VAL, result + size, sequenceParamName);
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
        static final String SEQUENCE_NEXT_VAL_BYNAME =
            "SELECT NEXT_VAL FROM SEQUENCE_TABLE WHERE SEQUENCE_NAME=? FOR UPDATE";
    }
}
