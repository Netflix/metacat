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
 * Hive related properties for Metacat.
 *
 * @author tgianos
 * @since 1.0.0
 */
@Data
//TODO: This shouldn't be in the common module. This should be in the Hive connector
public class HiveProperties {

    @NonNull
    private Metastore metastore = new Metastore();
    @NonNull
    private Iceberg iceberg = new Iceberg();
    @NonNull
    private CommonView commonview = new CommonView();
    /**
     * Metastore related properties.
     *
     * @author tgianos
     * @since 1.1.0
     */
    @Data
    public static class Metastore {

        @NonNull
        private Partition partition = new Partition();
        private int fetchSize = 2500;
        private int batchSize = 2500;

        /**
         * Metastore partition related properties.
         *
         * @author tgianos
         * @since 1.1.0
         */
        @Data
        public static class Partition {

            @NonNull
            private Name name = new Name();

            /**
             * Metastore partition name related properties.
             *
             * @author tgianos
             * @since 1.1.0
             */
            @Data
            public static class Name {

                @NonNull
                private Whitelist whitelist = new Whitelist();

                /**
                 * Metastore partition name whitelist related properties.
                 *
                 * @author tgianos
                 * @since 1.1.0
                 */
                @Data
                public static class Whitelist {
                    @NonNull
                    private String pattern = "";
                }
            }
        }
    }

    /**
     * Iceberg related properties.
     *
     * @author zhenl
     * @since 1.2.0
     */
    @Data
    public static class Iceberg {
        @NonNull
        private IcebergCacheProperties cache = new IcebergCacheProperties();
        private boolean enabled;
        private int fetchSizeInTableSummary = 100;
        /* each retry needs a s3 access, default to 0 as no retry */
        private int refreshFromMetadataLocationRetryNumber;
        /* loading metadata consumes memory, cap to 500m as default */
        private long maxMetadataFileSizeBytes = 500 * 1024 * 1024; //500m
        /*iceberg://<db-name.table-name>/<partition>/snapshot_time=<dateCreated> */
        private String partitionUriScheme = "iceberg";
    }

    /**
     * Iceberg cache related properties.
     *
     * @author amajumdar
     * @since 1.3.0
     */
    @Data
    public static class IcebergCacheProperties {
        private boolean enabled;
    }

    /**
     * CommonView related properties.
     *
     * @author zhenl
     * @since 1.3.0
     */
    @Data
    public static class CommonView {
        private boolean enabled;
    }
}
