/*
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
 */

package com.netflix.metacat.connector.druid;

/**
 * Druid Config Constants.
 *
 * @author zhenl jtuglu
 * @since 1.2.0
 */
public final class DruidConfigConstants {
    /**
     * DRUID_ROUTER_URI .
     */
    public static final String DRUID_ROUTER_URI = "druid.uri";

    //Http client
    /**
     * HTTP_TIMEOUT.
     */
    public static final String HTTP_TIMEOUT = "http.timeout";

    /**
     * POOL_SIZE.
     */
    public static final String POOL_SIZE = "pool.size";

    /**
     * DRUID_DB.
     */
    public static final String DRUID_DB = "default";

    /**
     * druid name.
     */
    public static final String NAME = "name";

    /**
     * druid properties.
     */
    public static final String PROPERTIES = "properties";

    /**
     * druid created.
     */
    public static final String CREATED = "created";

    /**
     * druid dimensions.
     */
    public static final String DIMENSIONS = "dimensions";

    /**
     * druid metrics.
     */
    public static final String METRICS = "metrics";

    /**
     * druid segments.
     */
    public static final String SEGMENTS = "segments";


    /**
     * size.
     */
    public static final String SIZE = "size";

    //Segment related information
    /**
     * dataSource.
     */
    public static final String DATA_SOURCE = "dataSource";

    /**
     * interval.
     */
    public static final String INTERVAL = "interval";

    /**
     * loadSpec.
     */
    public static final String LOADSPEC = "loadSpec";

    /**
     * bucket.
     */
    public static final String LOADSPEC_BUCKET = "bucket";

    /**
     * key.
     */
    public static final String LOADSPEC_KEY = "key";

    /**
     * loadspec type.
     */
    public static final String LOADSPEC_TYPE = "type";

    /**
     * identifier.
     */
    public static final String IDENTIFIER = "identifier";

    /**
     * default value if empty.
     */
    public static final String DEFAULT_VAULE = "NONE";


    private DruidConfigConstants() {
    }
}
