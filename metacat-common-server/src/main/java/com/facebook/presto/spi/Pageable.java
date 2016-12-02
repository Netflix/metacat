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

package com.facebook.presto.spi;

import lombok.Data;

/**
 * Represents the pagination information.
 * @author amajumdar
 */
@Data
public class Pageable {
    private Integer limit;
    private Integer offset;

    /**
     * Default constructor.
     */
    public Pageable() {
    }

    /**
     * Constructor.
     * @param limit size of the list
     * @param offset offset of the list
     */
    public Pageable(final Integer limit, final Integer offset) {
        this.limit = limit;
        this.offset = offset;
    }

    public Integer getOffset() {
        return offset == null ? Integer.valueOf(0) : offset;
    }

    public boolean isPageable() {
        return limit != null;
    }
}
