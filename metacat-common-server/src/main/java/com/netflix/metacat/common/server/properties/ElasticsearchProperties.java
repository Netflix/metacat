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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;
import com.netflix.metacat.common.QualifiedName;
import lombok.Data;
import lombok.NonNull;

import java.util.List;

/**
 * Properties related to Elasticsearch configuration.
 *
 * @author tgianos
 * @since 1.1.0
 */
@Data
public class ElasticsearchProperties {
    private boolean enabled;
    private long timeout = 30;
    private long bulkTimeout = 120;

    @NonNull
    private Index index = new Index();
    @NonNull
    private MergeIndex mergeIndex = new MergeIndex();
    @NonNull
    private Cluster cluster = new Cluster();
    @NonNull
    private Refresh refresh = new Refresh();
    @NonNull
    private Scroll scroll = new Scroll();
    @NonNull
    private Publish publish = new Publish();

    /**
     * Elasticsearch index related properties.
     *
     * @author tgianos
     * @since 1.1.0
     */
    @Data
    public static class Index {
        private String name = "metacat";
    }

    /**
     * Elasticsearch merge index related properties.
     *
     * @author tgianos
     * @since 1.1.0
     */
    @Data
    public static class MergeIndex {
        private String name;
    }

    /**
     * Elasticsearch cluster related properties.
     *
     * @author tgianos
     * @since 1.1.0
     */
    @Data
    public static class Cluster {
        private String name;
        private String nodes;
        private int port = 7102;
        @NonNull
        private Discovery discovery = new Discovery();

        /**
         * Whether to use discovery or the lookup service to find Elasticsearch nodes.
         *
         * @author tgianos
         * @since 1.1.0
         */
        @Data
        public static class Discovery {
            private boolean useLookup;
        }
    }

    /**
     * Elasticsearch refresh related properties.
     *
     * @author tgianos
     * @since 1.1.0
     */
    @Data
    public static class Refresh {

        @NonNull
        private Include include = new Include();
        @NonNull
        private Exclude exclude = new Exclude();
        @NonNull
        private Partitions partitions = new Partitions();
        @NonNull
        private Threshold threshold = new Threshold();

        /**
         * Elasticsearch refresh inclusion related properties.
         *
         * @author tgianos
         * @since 1.1.0
         */
        @Data
        public static class Include {
            private String catalogs;
            private String databases;
            private List<QualifiedName> databasesList;

            /**
             * Get the databases stored in the variable as a List of fully qualified names.
             *
             * @return The databases as a list or empty list if {@code names} is null or empty
             */
            @JsonIgnore
            public List<QualifiedName> getDatabasesAsListOfQualfiedNames() {
                if (databasesList == null) {
                    databasesList = this.databases == null
                        ? Lists.newArrayList()
                        : PropertyUtils.delimitedStringsToQualifiedNamesList(this.databases, ',');
                }
                return databasesList;
            }
        }

        /**
         * Elasticsearch refresh exclusion related properties.
         *
         * @author tgianos
         * @since 1.1.0
         */
        @Data
        public static class Exclude {

            @NonNull
            private Qualified qualified = new Qualified();

            /**
             * Elasticsearch refresh exclusion qualified related properties.
             *
             * @author tgianos
             * @since 1.1.0
             */
            @Data
            public static class Qualified {
                private String names;
                private List<QualifiedName> namesList;

                /**
                 * Get the names stored in the variable as a List of fully qualified names.
                 *
                 * @return The names as a list or empty list if {@code names} is null or empty
                 */
                @JsonIgnore
                public List<QualifiedName> getNamesAsListOfQualifiedNames() {
                    if (namesList == null) {
                        namesList = this.names == null
                            ? Lists.newArrayList()
                            : PropertyUtils.delimitedStringsToQualifiedNamesList(this.names, ',');
                    }
                    return namesList;
                }
            }
        }

        /**
         * Elasticsearch refresh partition related properties.
         *
         * @author tgianos
         * @since 1.1.0
         */
        @Data
        public static class Partitions {

            @NonNull
            private Include include = new Include();

            /**
             * Elasticsearch refresh partitions inclusion related properties.
             *
             * @author tgianos
             * @since 1.1.0
             */
            @Data
            public static class Include {
                private String catalogs = "prodhive,testhive,s3,aegisthus";
            }
        }

        /**
         * Elasticsearch refresh threshold related properties.
         *
         * @author tgianos
         * @since 1.1.0
         */
        @Data
        public static class Threshold {

            @NonNull
            private Unmarked unmarked = new Unmarked();

            /**
             * Elasticsearch refresh threshold unmarked related properties.
             *
             * @author tgianos
             * @since 1.1.0
             */
            @Data
            public static class Unmarked {

                @NonNull
                private Databases databases = new Databases();
                @NonNull
                private Tables tables = new Tables();

                /**
                 * Elasticsearch refresh threshold unmarked databases related properties.
                 *
                 * @author tgianos
                 * @since 1.1.0
                 */
                @Data
                public static class Databases {
                    private int delete = 100;
                }

                /**
                 * Elasticsearch refresh threshold unmarked tables related properties.
                 *
                 * @author tgianos
                 * @since 1.1.0
                 */
                @Data
                public static class Tables {
                    private int delete = 1000;
                }
            }
        }
    }

    /**
     * Elasticsearch scroll related properties.
     *
     * @author tgianos
     * @since 1.1.0
     */
    @Data
    public static class Scroll {

        @NonNull
        private Fetch fetch = new Fetch();
        @NonNull
        private Timeout timeout = new Timeout();

        /**
         * Elasticsearch scroll fetch related properties.
         *
         * @author tgianos
         * @since 1.1.0
         */
        @Data
        public static class Fetch {
            private int size = 50000;
        }

        /**
         * Elasticsearch scroll timeout related properties.
         *
         * @author tgianos
         * @since 1.1.0
         */
        @Data
        public static class Timeout {
            private int ms = 600000;
        }
    }

    /**
     * Elasticsearch publish properties.
     *
     * @author tgianos
     * @since 1.1.0
     */
    @Data
    public static class Publish {
        private boolean partitionEnabled;
        private boolean updateTablesWithSameUriEnabled;
        private boolean metacatLogEnabled;
    }
}
