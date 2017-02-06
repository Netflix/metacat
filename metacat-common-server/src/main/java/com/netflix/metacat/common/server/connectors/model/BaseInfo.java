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

import com.google.common.collect.Maps;
import com.netflix.metacat.common.QualifiedName;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Base class for connector resources.
 *
 * @author amajumdar
 * @author tgianos
 * @since 0.1.52
 */
@Getter
@ToString
@EqualsAndHashCode
public class BaseInfo {
    /* Name of the resource */
    private final QualifiedName name;
    /* Audit information of the resource */
    private final AuditInfo audit;
    /* Metadata properties of the resource */
    private final Map<String, String> metadata;

    @SuppressWarnings("unchecked")
    BaseInfo(final Builder builder) {
        this.name = builder.name;
        this.audit = builder.audit;
        this.metadata = new HashMap<>();
        this.metadata.putAll(builder.metadata);
    }

    /**
     * Get the audit info if any is available.
     *
     * @return Optional audit info
     */
    public Optional<AuditInfo> getAudit() {
        return Optional.ofNullable(this.audit);
    }

    /**
     * Get a copy of the metadata map.
     *
     * @return The metadata
     */
    public Map<String, String> getMetadata() {
        return Maps.newHashMap(this.metadata);
    }

    /**
     * Builder pattern to save constructor arguments.
     *
     * @param <T> Type of builder that extends this
     * @author tgianos
     * @since 3.0.0
     */
    @SuppressWarnings("unchecked")
    public static class Builder<T extends Builder> {

        private final QualifiedName name;
        private final Map<String, String> metadata = new HashMap<>();
        private AuditInfo audit;

        /**
         * Create a new builder with the required parameters.
         *
         * @param name The fully qualified name of this resource.
         */
        public Builder(@Nonnull final QualifiedName name) {
            this.name = name;
        }

        /**
         * Set any extra metadata for the resource.
         *
         * @param metadata Any extra metadata for the resource
         * @return The builder
         */
        public T withMetadata(@Nullable final Map<String, String> metadata) {
            this.metadata.clear();
            if (metadata != null) {
                this.metadata.putAll(metadata);
            }
            return (T) this;
        }

        /**
         * Set any audit info for this resource.
         *
         * @param audit The audit info to use
         * @return The builder
         */
        public T withAudit(@Nullable final AuditInfo audit) {
            this.audit = audit;
            return (T) this;
        }

        /**
         * Construct a new instance of BaseInfo.
         *
         * @return The instance built by this builder.
         */
        public BaseInfo build() {
            return new BaseInfo(this);
        }
    }
}
