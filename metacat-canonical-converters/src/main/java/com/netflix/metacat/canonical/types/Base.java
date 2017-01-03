/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.canonical.types;

/**
 Canonical base type class.
 */
public enum Base {

    /** Numeric Types.
     * small int 2-byte signed integer from -32,768 to 32,767.*/
    SMALLINT("smallint", false, false),
    /** tinyint 1-byte signed integer, from -128 to 127. */
    TINYINT("tinyint", false, false),
    /** int 4-byte signed integer, from -2,147,483,648 to 2,147,483,647. */
    INT("int", false, false),
    /** bigint 8-byte signed integer, from -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807. */
    BIGINT("bigint", false, false),
    /** float 4-byte single precision floating point number. */
    FLOAT("float", false, false),
    /** double 8-byte double precision floating point number. */
    DOUBLE("double", false, false),
    /** decimal type user definable precision and scale. */
    DECIMAL("decimal", true, false),
    /** char fixed-length <= 255. */
    CHAR("char", true, false),
    /** varchar created with a length specifier (between 1 and 65355). */
    VARCHAR("varchar", true, false),
    /** string type. */
    STRING("string", false, false),
    /** json json string. */
    JSON("json", false, false),

    /** boolean type. */
    BOOLEAN("boolean", false, false),
    /** varbinary type. */
    VARBINARY("varbinary", false, false),

    /** date year/month/day in the form YYYY-­MM-­DD. */
    DATE("date", false, false),
    /** time traditional UNIX timestamp with optional nanosecond precision. */
    TIME("time", false, false),
    /** time with time zone. */
    TIME_WITH_TIME_ZONE("time with time zone", false, false),
    /** timestamp type. */
    TIMESTAMP("timestamp", false, false),
    /** timestamp with time zone type. */
    TIMESTAMP_WITH_TIME_ZONE("timestamp with time zone", false, false),
    /** Year to month intervals, format: SY-M
     * S: optional sign (+/-)
     * Y: year count
     * M: month count
     * example INTERVAL '1-2' YEAR TO MONTH.
     **/
    INTERVAL_YEAR_TO_MONTH("interval year to month", false, false),
    /** Day to second intervals, format: SD H:M:S.nnnnnn
     *  S: optional sign (+/-)
     *  D: day countH: hours
     *  M: minutes
     *  S: seconds
     *  nnnnnn: optional nanotime
     *  example INTERVAL '1 2:3:4.000005' DAY.
     * */
    INTERVAL_DAY_TO_SECOND("interval day to second", false, false),

    /** unknown type. */
    UNKNOWN("unknown", false, false),
    /** array type. */
    ARRAY("array", false, true),
    /** row type. */
    ROW("row", false, true),
    /** map type. */
    MAP("map", false, true);

    private final String type;
    private boolean isParametricType;
    private boolean isComplexType;
    Base(final String type, final boolean isParametricType, final boolean isComplexType) {
        this.type = type;
        this.isParametricType = isParametricType;
        this.isComplexType = isComplexType;
    }

    /* signature of the type. */
    public String getBaseTypeDisplayName() {
        return type;
    }

    /* Type has literal parameters. Ex. char(10). */
    public boolean isParametricType() {
        return isParametricType;
    }

    /* Type is a complex type. */
    public boolean isComplexType() {
        return isComplexType;
    }

    /**
     * Return name of the base type.
     * @param name name
     * @return Base type
     */
    public Base fromName(final String name) {
        try {
            return Base.valueOf(name);
        } catch (Exception e) {
            return UNKNOWN;
        }
    }
}
