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

package com.netflix.metacat.common.type;

/**
 * Canonical base type class.
 * @author zhenl
 */
public enum TypeEnum {

    /** Numeric Types.
     * small int 2-byte signed integer from -32,768 to 32,767.*/
    SMALLINT("smallint", false),
    /** tinyint 1-byte signed integer, from -128 to 127. */
    TINYINT("tinyint", false),
    /** int 4-byte signed integer, from -2,147,483,648 to 2,147,483,647. */
    INT("int", false),
    /** bigint 8-byte signed integer, from -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807. */
    BIGINT("bigint", false),
    /** float 4-byte single precision floating point number. */
    FLOAT("float", false),
    /** double 8-byte double precision floating point number. */
    DOUBLE("double", false),
    /** decimal type user definable precision and scale. */
    DECIMAL("decimal", true),
    /** char fixed length less than or equals to 255. */
    CHAR("char", true),
    /** varchar created with a length specifier (between 1 and 65355). */
    VARCHAR("varchar", true),
    /** string type. */
    STRING("string", false),
    /** json json string. */
    JSON("json", false),

    /** boolean type. */
    BOOLEAN("boolean", false),
    /** varbinary type. */
    VARBINARY("varbinary", true),

    /** date year/month/day in the form YYYY-­MM-­DD. */
    DATE("date", false),
    /** time traditional UNIX timestamp with optional nanosecond precision. */
    TIME("time", false),
    /** time with time zone. */
    TIME_WITH_TIME_ZONE("time with time zone", false),
    /** timestamp type. */
    TIMESTAMP("timestamp", false),
    /** timestamp with time zone type. */
    TIMESTAMP_WITH_TIME_ZONE("timestamp with time zone", false),
    /** Year to month intervals, format: SY-M
     * S: optional sign (+/-)
     * Y: year count
     * M: month count
     * example INTERVAL '1-2' YEAR TO MONTH.
     **/
    INTERVAL_YEAR_TO_MONTH("interval year to month", false),
    /** Day to second intervals, format: SD H:M:S.nnnnnn
     *  S: optional sign (+/-)
     *  D: day countH: hours
     *  M: minutes
     *  S: seconds
     *  nnnnnn: optional nanotime
     *  example INTERVAL '1 2:3:4.000005' DAY.
     * */
    INTERVAL_DAY_TO_SECOND("interval day to second", false),

    /** unknown type. */
    UNKNOWN("unknown", false),
    /** array type. */
    ARRAY("array", true),
    /** row type. */
    ROW("row", true),
    /** map type. */
    MAP("map", true);

    private final String type;
    private boolean isParametricType;

    TypeEnum(final String type, final boolean isParametricType) {
        this.type = type;
        this.isParametricType = isParametricType;
    }

    /* signature of the type. */
    public String getBaseTypeDisplayName() {
        return type;
    }

    /* Type has literal parameters. Ex. char(10). */
    public boolean isParametricType() {
        return isParametricType;
    }

    /**
     * Return name of the base type.
     * @param name name
     * @return TypeEnum type
     */
    public TypeEnum fromName(final String name) {
        try {
            return TypeEnum.valueOf(name);
        } catch (Exception e) {
            return UNKNOWN;
        }
    }
}
