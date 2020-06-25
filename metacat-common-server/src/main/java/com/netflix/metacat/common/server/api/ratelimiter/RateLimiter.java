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

import lombok.NonNull;

/**
 * Request rate-limiter service API.
 *
 * @author rveeramacheneni
 */
public interface RateLimiter {

    /**
     * Whether a given request has exceeded
     * the pre-configured API request rate-limit.
     *
     * @param context The rate-limiter request context.
     * @return True if it has exceeded the API request rate-limit.
     */
    default boolean hasExceededRequestLimit(@NonNull RateLimiterRequestContext context) {
        return false;
    }
}
