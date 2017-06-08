/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.metacat.common.server.connectors.exception;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.netflix.metacat.common.QualifiedName;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Exception when partition already exists.
 *
 * @author zhenl
 */
public class PartitionAlreadyExistsException extends AlreadyExistsException {
    private static final Joiner COMMA_JOINER = Joiner.on(',');

    /**
     * Constructor.
     *
     * @param tableName     table name
     * @param partitionName partition name
     */
    public PartitionAlreadyExistsException(final QualifiedName tableName, final String partitionName) {
        this(tableName, partitionName, null);
    }

    /**
     * Constructor.
     *
     * @param tableName     table name
     * @param partitionName partition name
     * @param cause         error cause
     */
    public PartitionAlreadyExistsException(
        final QualifiedName tableName,
        final String partitionName,
        @Nullable final Throwable cause
    ) {
        this(tableName, Lists.newArrayList(partitionName), cause);
    }

    /**
     * Constructor.
     *
     * @param tableName      table name
     * @param partitionNames partition names
     * @param cause          error cause
     */
    public PartitionAlreadyExistsException(
        final QualifiedName tableName,
        final List<String> partitionNames,
        @Nullable final Throwable cause
    ) {
        super(tableName,
            String.format("One or more of the partitions '%s' already exists for table '%s'",
                COMMA_JOINER.join(partitionNames), tableName), cause, false, false);
    }
}
