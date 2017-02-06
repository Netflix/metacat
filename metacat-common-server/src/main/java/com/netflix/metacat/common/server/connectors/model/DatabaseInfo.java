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

import com.netflix.metacat.common.QualifiedName;
import lombok.EqualsAndHashCode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Database information.
 *
 * @author amajumndar
 * @author tgianos
 * @since 0.1.52
 */
@EqualsAndHashCode(callSuper = true)
public class DatabaseInfo extends BaseInfo {
    /* location of the database */
    private final String uri;

    private DatabaseInfo(@Nonnull final Builder builder) {
        super(builder);
        this.uri = builder.uri;
    }

    /**
     * Get the location of the database.
     *
     * @return Optional of the database location
     */
    public Optional<String> getUri() {
        return Optional.ofNullable(this.uri);
    }

    /**
     * Builder for DatabaseInfo instances.
     *
     * @author tgianos
     * @since 0.1.52
     */
    public static class Builder extends BaseInfo.Builder<Builder> {

        private String uri;

        /**
         * New builder with the required fields.
         *
         * @param name The fully qualified name of this database
         */
        public Builder(@Nonnull final QualifiedName name) {
            super(name);
        }

        /**
         * Add a uri for this database.
         *
         * @param uri The location of the database
         * @return The builder
         */
        public Builder withUri(@Nullable String uri) {
            this.uri = uri;
            return this;
        }

        /**
         * Create a new instance of DatabaseInfo.
         *
         * @return The new instance
         */
        public DatabaseInfo build() {
            return new DatabaseInfo(this);
        }
    }
}
