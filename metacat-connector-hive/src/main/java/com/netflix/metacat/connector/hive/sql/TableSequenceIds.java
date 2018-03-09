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

import lombok.Data;

import javax.annotation.Nullable;

/**
 * Class representing the ids for a table.
 *
 * @author amajumdar
 */
@Data
public class TableSequenceIds {
    private final Long tableId;
    private final Long cdId;
    private final Long sdsId;
    private final Long serdeId;

    /**
     * Constructor.
     * @param tableId table id
     * @param cdId column id
     */
    public TableSequenceIds(final Long tableId,
                            final Long cdId) {
        this(tableId, cdId, null, null);
    }

    /**
     * Constructor.
     * @param tableId table id
     * @param cdId column id
     * @param sdsId sds id
     * @param serdeId serde id
     */
    public TableSequenceIds(final Long tableId,
                            final Long cdId,
                            @Nullable final Long sdsId,
                            @Nullable final Long serdeId) {
        this.tableId = tableId;
        this.cdId = cdId;
        this.sdsId = sdsId;
        this.serdeId = serdeId;
    }
}
