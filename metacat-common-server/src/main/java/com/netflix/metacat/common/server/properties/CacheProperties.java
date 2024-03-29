/*
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.metacat.common.server.properties;

import lombok.Data;

/**
 * Properties related to cache configuration.
 *
 * @author amajumdar
 * @since 1.2.0
 */
@Data
public class CacheProperties {
    private static final int DEFAULT_CACHE_MUTATION_TIMEOUT_MILLIS = 20;

    private boolean enabled;
    private boolean throwOnEvictionFailure;
    private int evictionTimeoutMs = DEFAULT_CACHE_MUTATION_TIMEOUT_MILLIS;
    private String name;
}

