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
 * Properties related to notifications.
 *
 * @author tgianos
 * @since 1.1.0
 */
@Data
public class NotificationsProperties {

    @NonNull
    private Sns sns = new Sns();

    /**
     * SNS Properties.
     *
     * @author tgianos
     * @since 1.1.0
     */
    @Data
    public static class Sns {
        private boolean enabled = false;
        private Topic topic = new Topic();

        /**
         * SNS Topic settings.
         *
         * @author tgianos
         * @since 1.1.0
         */
        @Data
        public static class Topic {

            @NonNull
            private Table table = new Table();
            @NonNull
            private Partition partition = new Partition();

            /**
             * Table notification settings.
             *
             * @author tgianos
             * @since 1.1.0
             */
            @Data
            public static class Table {
                private String arn;
            }

            /**
             * Partition notification settings.
             *
             * @author tgianos
             * @since 1.1.0
             */
            @Data
            public static class Partition {
                private String arn;
            }
        }
    }
}
