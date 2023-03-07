/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.metacat.common.server.api.ratelimiter;

import com.netflix.metacat.common.QualifiedName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Context pertaining to a rate-limited API request.
 *
 * @author rveeramacheneni
 */
@AllArgsConstructor
@Builder
@Getter
@EqualsAndHashCode
public class RateLimiterRequestContext {
    /**
     * The API request. eg. getTable, updateTable etc.
     */
    private String requestName;

    /**
     * The QualifiedName of the resource.
     */
    private QualifiedName name;
}
