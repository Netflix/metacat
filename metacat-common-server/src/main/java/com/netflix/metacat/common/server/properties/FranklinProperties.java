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
 * Franklin related properties.
 *
 * @author tgianos
 * @since 1.1.0
 */
@Data
public class FranklinProperties {

    @NonNull
    private Connector connector = new Connector();

    /**
     * Connector related properties.
     *
     * @author tgianos
     * @since 1.1.0
     */
    @Data
    public static class Connector {

        @NonNull
        private Use use = new Use();

        /**
         * Connector use related properties.
         *
         * @author tgianos
         * @since 1.1.0
         */
        @Data
        public static class Use {

            @NonNull
            private Pig pig = new Pig();

            /**
             * Connector use pig related properties.
             *
             * @author tgianos
             * @since 1.1.0
             */
            @Data
            public static class Pig {
                private boolean type = true;
            }
        }
    }
}
