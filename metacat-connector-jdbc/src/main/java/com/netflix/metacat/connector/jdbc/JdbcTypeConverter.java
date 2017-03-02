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
package com.netflix.metacat.connector.jdbc;

import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter;
import com.netflix.metacat.common.type.BaseType;
import com.netflix.metacat.common.type.CharType;
import com.netflix.metacat.common.type.DecimalType;
import com.netflix.metacat.common.type.Type;
import com.netflix.metacat.common.type.VarbinaryType;
import com.netflix.metacat.common.type.VarcharType;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Type converter utilities for JDBC connectors.
 *
 * @author tgianos
 * @since 0.1.52
 */
@Slf4j
public abstract class JdbcTypeConverter implements ConnectorTypeConverter {

    private static final Pattern TYPE_PATTERN = Pattern.compile(
        "^\\s*?"
            + "(\\w+(?:\\s(?:precision|varying))?)" // group 0
            + "\\s*?"
            + "(?:\\(\\s*?(\\d+)(?:\\s*?,\\s*?(\\d+))?\\s*?\\))?" // group 1 and 2
            + "\\s*?"
            + "(\\[\\](?:\\[\\])?)?" // group 3
            + "(?:\\s*?(\\w+(?:\\s\\w+)*))?$" // group 4
    );

    protected String[] splitType(final String type) {
        final Matcher matcher = TYPE_PATTERN.matcher(type);
        final int numGroups = matcher.groupCount();
        if (matcher.find()) {
            final String[] split = new String[numGroups];
            for (int i = 0; i < numGroups; i++) {
                split[i] = matcher.group(i + 1);
            }
            return split;
        } else {
            throw new IllegalArgumentException("Unable to parse " + type);
        }
    }

    protected Type toMetacatBitType(@Nonnull final String[] bit) {
        // No size parameter
        if (bit[1] == null || Integer.parseInt(bit[1]) == 1) {
            return BaseType.BOOLEAN;
        } else {
            final int bytes = (int) Math.ceil(Double.parseDouble(bit[1]) / 8.0);
            return VarbinaryType.createVarbinaryType(bytes);
        }


    }

    protected DecimalType toMetacatDecimalType(@Nonnull final String[] splitType) {
        if (splitType[1] == null && splitType[2] == null) {
            return DecimalType.createDecimalType();
        } else if (splitType[1] != null) {
            final int precision = Integer.parseInt(splitType[1]);
            if (splitType[2] == null) {
                return DecimalType.createDecimalType(precision);
            } else {
                return DecimalType.createDecimalType(precision, Integer.parseInt(splitType[2]));
            }
        } else {
            throw new IllegalArgumentException("Illegal definition of a decimal type: " + Arrays.toString(splitType));
        }
    }

    protected Type toMetacatCharType(@Nonnull final String[] splitType) {
        if (splitType[1] == null) {
            throw new IllegalArgumentException("Must have size for char type");
        }

        final int size = Integer.parseInt(splitType[1]);
        // Check if we're dealing with binary or not
        if (splitType[4] != null) {
            if (!splitType[4].equals("binary")) {
                throw new IllegalArgumentException(
                    "Unrecognized extra field in char type: " + splitType[4] + ". Expected 'binary'."
                );
            }
            return VarbinaryType.createVarbinaryType(size);
        } else {
            return CharType.createCharType(size);
        }
    }

    protected Type toMetacatVarcharType(@Nonnull final String[] splitType) {
        if (splitType[1] == null) {
            throw new IllegalArgumentException("Must have size for varchar type");
        }

        final int size = Integer.parseInt(splitType[1]);
        // Check if we're dealing with binary or not
        if (splitType[4] != null) {
            if (!splitType[4].equals("binary")) {
                throw new IllegalArgumentException(
                    "Unrecognized extra field in varchar type: " + splitType[4] + ". Expected 'binary'."
                );
            }
            return VarbinaryType.createVarbinaryType(size);
        } else {
            return VarcharType.createVarcharType(size);
        }
    }

    protected VarbinaryType toMetacatVarbinaryType(@Nonnull final String[] splitType) {
        if (!splitType[0].equals("varbinary") && !splitType[0].equals("binary")) {
            // Blob
            return VarbinaryType.createVarbinaryType(Integer.MAX_VALUE);
        }
        if (splitType[1] == null) {
            throw new IllegalArgumentException("Must have size for varbinary type");
        }

        return VarbinaryType.createVarbinaryType(Integer.parseInt(splitType[1]));
    }

    protected Type toMetacatTimeType(@Nonnull final String[] splitType) {
        if (splitType[4] != null && splitType[4].equals("with time zone")) {
            return BaseType.TIME_WITH_TIME_ZONE;
        } else {
            return BaseType.TIME;
        }
    }

    protected Type toMetacatTimestampType(@Nonnull final String[] splitType) {
        if (splitType[4] != null && splitType[4].equals("with time zone")) {
            return BaseType.TIMESTAMP_WITH_TIME_ZONE;
        } else {
            return BaseType.TIMESTAMP;
        }
    }
}
