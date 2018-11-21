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

import com.google.common.collect.Lists;
import com.netflix.metacat.common.QualifiedName;
import lombok.Data;
import lombok.NonNull;

/**
 * Service related properties.
 *
 * @author tgianos
 * @since 1.1.0
 */
@Data
public class ServiceProperties {

    @NonNull
    private Max max = new Max();
    @NonNull
    private Tables tables = new Tables();

    /**
     * Max related properties.
     *
     * @author tgianos
     * @since 1.1.0
     */
    @Data
    public static class Max {

        @NonNull
        private Number number = new Number();

        /**
         * Max number related properties.
         *
         * @author tgianos
         * @since 1.1.0
         */
        @Data
        public static class Number {
            private int threads = 50;
        }
    }

    /**
     * Service tables related properties.
     *
     * @author tgianos
     * @since 1.1.0
     */
    @Data
    public static class Tables {

        @NonNull
        private Error error = new Error();

        /**
         * Service tables error related properties.
         *
         * @author tgianos
         * @since 1.1.0
         */
        @Data
        public static class Error {

            @NonNull
            private List list = new List();

            /**
             * Service tables error list related properties.
             *
             * @author tgianos
             * @since 1.1.0
             */
            @Data
            public static class List {

                @NonNull
                private Partitions partitions = new Partitions();

                /**
                 * Service tables error list partitions related properties.
                 *
                 * @author tgianos
                 * @since 1.1.0
                 */
                @Data
                public static class Partitions {
                    private int threshold = Integer.MAX_VALUE;
                    @NonNull
                    private No no = new No();


                    /**
                     * Service tables error list partitions no related properties.
                     *
                     * @author tgianos
                     * @since 1.1.0
                     */
                    @Data
                    public static class No {
                        private String filter;
                        private java.util.List<QualifiedName> filterList;

                        /**
                         * Get the filter list as a list of qualified names.
                         *
                         * @return The filtered list
                         */
                        public java.util.List<QualifiedName> getFilterAsListOfQualifiedNames() {
                            if (filterList == null) {
                                filterList = filter == null ? Lists.newArrayList()
                                    : PropertyUtils.delimitedStringsToQualifiedNamesList(filter, ',');
                            }
                            return  filterList;
                        }
                    }
                }
            }
        }
    }
}
