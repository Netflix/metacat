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
 * Properties related to Metacat internal Events.
 *
 * @author tgianos
 * @since 1.1.0
 */
@Data
public class EventProperties {
    private boolean updateIcebergTableAsyncPostEventEnabled;

    @NonNull
    private Thread thread = new Thread();
    @NonNull
    private Bus bus = new Bus();

    /**
     * Properties related to event threads.
     *
     * @author tgianos
     * @since 1.1.0
     */
    @Data
    public static class Thread {
        private int count = 10;
    }

    /**
     * Properties related to the event bus.
     *
     * @author tgianos
     * @since 1.1.0
     */
    @Data
    public static class Bus {

        @NonNull
        private Executor executor = new Executor();

        /**
         * Properties related to bus executor.
         *
         * @author tgianos
         * @since 1.1.0
         */
        @Data
        public static class Executor {

            @NonNull
            private Thread thread = new Thread();

            /**
             * Properties related to event bus executor threads.
             *
             * @author tgianos
             * @since 1.1.0
             */
            @Data
            public static class Thread {
                private int count = 10;
            }
        }
    }
}
