/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.facebook.presto.exception;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import com.google.common.base.Joiner;

import java.util.List;

import static com.google.common.base.Strings.nullToEmpty;

/**
 * Created by amajumdar on 4/30/15.
 */
public class PartitionAlreadyExistsException extends PrestoException {
    private static final Joiner COMMA_JOINER = Joiner.on(',');

    public PartitionAlreadyExistsException(SchemaTableName tableName, String partitionId) {
        this(tableName, partitionId, null);
    }

    public PartitionAlreadyExistsException(SchemaTableName tableName, String partitionId, Throwable cause) {
        super(StandardErrorCode.ALREADY_EXISTS,
                String.format("Partition '%s' already exists for table '%s'", nullToEmpty(partitionId), tableName),
                cause);
    }

    public PartitionAlreadyExistsException(SchemaTableName tableName, List<String> partitionIds, Throwable cause) {
        super(StandardErrorCode.ALREADY_EXISTS,
                String.format("One or more of the partitions '%s' already exists for table '%s'",
                        COMMA_JOINER.join(partitionIds), tableName), cause);
    }
}
