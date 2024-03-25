/*
 *  Copyright 2018 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.netflix.metacat.common.server.usermetadata;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetadataException;

import javax.annotation.Nullable;


/**
 * Business Metadata Manager.
 *
 * @author zhenl
 * @since 1.2.0
 */
public interface MetadataInterceptor {

    /**
     * Apply business rules before retrieving back. These rules may change or override existing
     * business metadata.
     *
     * @param userMetadataService user metadata service
     * @param name                qualified name
     * @param objectNode          input metadata object node
     * @param getMetadataInterceptorParameters  get Metadata Interceptor Parameters
     */
    default void onRead(final UserMetadataService userMetadataService,
                        final QualifiedName name, @Nullable final ObjectNode objectNode,
                        final GetMetadataInterceptorParameters getMetadataInterceptorParameters) {
    }

    /**
     * Validate ObjectNode before storing it.
     * @param userMetadataService user metadata service
     * @param name                qualified name
     * @param objectNode          input metadata object node
     * @throws InvalidMetadataException business validation exception
     */
    default void onWrite(final UserMetadataService userMetadataService,
                         final QualifiedName name, final ObjectNode objectNode)
        throws InvalidMetadataException {
    }

    /**
     * Validate ObjectNode before deleting it.
     * @param userMetadataService user metadata service
     * @param name                qualified name
     * @param objectNode          to-be-deleted metadata object node
     * @throws InvalidMetadataException business validation exception
     */
    default void onDelete(final UserMetadataService userMetadataService,
                          final QualifiedName name, final ObjectNode objectNode)
        throws InvalidMetadataException {
    }
}
