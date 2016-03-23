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

/**
 * Created by amajumdar on 5/11/15.
 */
public class InvalidMetaException extends PrestoException {
    private SchemaTableName tableName;
    private String partitionId;

    public InvalidMetaException(SchemaTableName tableName, Throwable cause) {
        super(StandardErrorCode.USER_ERROR
                , String.format("Invalid metadata for %s.", tableName)
                , cause);
        this.tableName = tableName;
    }

    public InvalidMetaException(SchemaTableName tableName, String partitionId, Throwable cause) {
        super(StandardErrorCode.USER_ERROR
                , String.format("Invalid metadata for %s for partition %s.", tableName, partitionId)
                , cause);
        this.tableName = tableName;
        this.partitionId = partitionId;
    }

    public InvalidMetaException(String message, Throwable cause) {
        super(StandardErrorCode.USER_ERROR
                , message
                , cause);
    }
}
