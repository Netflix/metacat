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
package com.netflix.metacat.common.server.properties;

import lombok.Data;
import lombok.NonNull;

/**
 * Usermetadata service related properties.
 *
 * @author amajumdar
 * @since 1.1.0
 */
@Data
public class UserMetadata {

    public static final int QUERY_TIMEOUT_IN_SEC = 60;
    public static final int LONG_QUERY_TIMEOUT_IN_SEC = 120;

    @NonNull
    private Config config = new Config();
    private int queryTimeoutInSeconds = QUERY_TIMEOUT_IN_SEC;

    /**
     * config related properties.
     *
     * @author amajumdar
     * @since 1.1.0
     */
    @Data
    public static class Config {
        private String location = "usermetadata.properties";
    }
}
