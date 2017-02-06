/*
 *
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
 *
 */
package com.netflix.metacat.common.server.connectors.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import javax.annotation.Nullable;
import java.util.Date;

/**
 * Audit information.
 *
 * @author amajumdar
 * @author tgianos
 * @since 0.1.52
 */
@Getter
@ToString
@EqualsAndHashCode
public class AuditInfo {
    private final String createdBy;
    private final Date createdDate;
    private final String lastModifiedBy;
    private final Date lastModifiedDate;

    private AuditInfo(final Builder builder) {
        this.createdBy = builder.createdBy;
        this.createdDate = new Date(builder.createdDate.getTime());
        this.lastModifiedBy = builder.lastModifiedBy;
        this.lastModifiedDate = new Date(builder.lastModifiedDate.getTime());
    }

    /**
     * Builder for an AuditInfo class.
     *
     * @author tgianos
     * @since 0.1.52
     */
    @NoArgsConstructor
    public static class Builder {

        private String createdBy;
        private Date createdDate;
        private String lastModifiedBy;
        private Date lastModifiedDate;

        /**
         * Set a created by field.
         *
         * @param createdBy Created by
         * @return The builder
         */
        public Builder withCreatedBy(@Nullable final String createdBy) {
            this.createdBy = createdBy;
            return this;
        }

        /**
         * Set a created date field.
         *
         * @param createdDate Created date
         * @return The builder
         */
        public Builder withCreatedDate(@Nullable final Date createdDate) {
            this.createdDate = createdDate == null ? null : new Date(createdDate.getTime());
            return this;
        }

        /**
         * Set a last modified by field.
         *
         * @param lastModifiedBy Last modified by
         * @return The builder
         */
        public Builder withLastModifiedBy(@Nullable final String lastModifiedBy) {
            this.lastModifiedBy = lastModifiedBy;
            return this;
        }

        /**
         * Set a last modified date field.
         *
         * @param lastModifiedDate last modified date
         * @return The builder
         */
        public Builder withLastModifiedDate(@Nullable final Date lastModifiedDate) {
            this.lastModifiedDate = lastModifiedDate == null ? null : new Date(lastModifiedDate.getTime());
            return this;
        }

        /**
         * Create a new AuditInfo instance.
         *
         * @return the new instance
         */
        public AuditInfo build() {
            return new AuditInfo(this);
        }
    }
}
