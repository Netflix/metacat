/*
 *  Copyright 2016 Netflix, Inc.
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *         http://www.apache.org/licenses/LICENSE-2.0
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.netflix.metacat.canonical.common.spi;

import lombok.Data;

/**
 * Audit info.
 */
@Data
public class AuditInfo {
    private String createdBy;
    private String lastUpdatedBy;
    private Long createdDate;
    private Long lastUpdatedDate;

    /**
     * Default constructor.
     */
    public AuditInfo() {
    }

    /**
     * Constructor.
     * @param createdBy creator name
     * @param lastUpdatedBy name who updated
     * @param createdDate creator time in epoch
     * @param lastUpdatedDate updated time in epoch
     */
    public AuditInfo(final String createdBy, final String lastUpdatedBy, final Long createdDate,
                     final Long lastUpdatedDate) {
        this.createdBy = createdBy;
        this.lastUpdatedBy = lastUpdatedBy;
        this.createdDate = createdDate;
        this.lastUpdatedDate = lastUpdatedDate;
    }
}
