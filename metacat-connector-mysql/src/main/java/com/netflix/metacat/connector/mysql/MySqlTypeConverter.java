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
package com.netflix.metacat.connector.mysql;

import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter;
import com.netflix.metacat.common.type.BaseType;
import com.netflix.metacat.common.type.CharType;
import com.netflix.metacat.common.type.DecimalType;
import com.netflix.metacat.common.type.Type;
import com.netflix.metacat.common.type.VarbinaryType;
import com.netflix.metacat.common.type.VarcharType;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Type converter for MySQL.
 *
 * @author tgianos
 * @since 0.1.52
 */
@Slf4j
public class MySqlTypeConverter implements ConnectorTypeConverter {

    private static final Pattern TYPE_PATTERN = Pattern.compile(
        "^\\s*?(\\w+(?: precision)?)\\s*?(?:\\(\\s*?(\\d+)(?:\\s*?,\\s*?(\\d+))?\\s*?\\))?(?:\\s*?(\\w+))?$"
    );

    /**
     * {@inheritDoc}
     */
    @Override
    public Type toMetacatType(@Nonnull @NonNull final String type) {
        // see: https://dev.mysql.com/doc/connector-j/6.0/en/connector-j-reference-type-conversions.html
        final String lowerType = type.toLowerCase();

        // Split up the possible type: TYPE[(size, magnitude)] EXTRA
        final String[] splitType = this.splitType(lowerType);
        switch (splitType[0]) {
            case "bit":
                return this.toMetacatBitType(splitType);
            case "tinyint":
                // TODO: MySQL generally treats this as boolean should we? Not according to spreadsheet currently
                return BaseType.TINYINT;
            case "bool":
            case "boolean":
                return BaseType.BOOLEAN;
            case "smallint":
                return BaseType.SMALLINT;
            case "mediumint":
            case "int":
            case "integer":
                return BaseType.INT;
            case "bigint":
                return BaseType.BIGINT;
            case "float":  // TODO: MySQL precision is lost
                return BaseType.FLOAT;
            case "double":
            case "double precision":
                return BaseType.DOUBLE;
            case "decimal":
            case "dec":
                return this.toMetacatDecimalType(splitType);
            case "date":
                return BaseType.DATE;
            case "datetime":
            case "time":
            case "timestamp":
                return BaseType.TIME;
            case "char":
                return this.toMetacatCharType(splitType);
            case "varchar":
                return this.toMetacatVarcharType(splitType);
            case "binary":
            case "tinyblob":
            case "blob":
            case "mediumblob":
            case "longblob":
            case "varbinary":
                return this.toMetacatVarbinaryType(splitType);
            case "tinytext":
            case "text":
            case "mediumtext":
            case "longtext":
                return BaseType.STRING;
            case "json":
                return BaseType.JSON;
            case "year":
            case "enum":
            case "set":
                // TODO: What do we do with these?
                throw new UnsupportedOperationException("Encountered " + splitType[0] + " type. Ignoring");
            default:
                throw new IllegalArgumentException("Unhandled or unknown sql type" + splitType[0]);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String fromMetacatType(@Nonnull @NonNull final Type type) {
        return null;
    }

    String[] splitType(final String type) {
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

    private Type toMetacatBitType(@Nonnull final String[] bit) {
        // No size parameter
        if (bit[1] == null || Integer.parseInt(bit[1]) == 1) {
            return BaseType.BOOLEAN;
        } else {
            final int bytes = (int) Math.ceil(Double.parseDouble(bit[1]) / 8.0);
            return VarbinaryType.createVarbinaryType(bytes);
        }
    }

    private DecimalType toMetacatDecimalType(@Nonnull final String[] splitType) {
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

    private Type toMetacatCharType(@Nonnull final String[] splitType) {
        if (splitType[1] == null) {
            throw new IllegalArgumentException("Must have size for char type");
        }

        final int size = Integer.parseInt(splitType[1]);
        // Check if we're dealing with binary or not
        if (splitType[3] != null) {
            if (!splitType[3].equals("binary")) {
                throw new IllegalArgumentException(
                    "Unrecognized extra field in char type: " + splitType[3] + ". Expected 'binary'."
                );
            }
            return VarbinaryType.createVarbinaryType(size);
        } else {
            return CharType.createCharType(size);
        }
    }

    private Type toMetacatVarcharType(@Nonnull final String[] splitType) {
        if (splitType[1] == null) {
            throw new IllegalArgumentException("Must have size for varchar type");
        }

        final int size = Integer.parseInt(splitType[1]);
        // Check if we're dealing with binary or not
        if (splitType[3] != null) {
            if (!splitType[3].equals("binary")) {
                throw new IllegalArgumentException(
                    "Unrecognized extra field in varchar type: " + splitType[3] + ". Expected 'binary'."
                );
            }
            return VarbinaryType.createVarbinaryType(size);
        } else {
            return VarcharType.createVarcharType(size);
        }
    }

    private VarbinaryType toMetacatVarbinaryType(@Nonnull final String[] splitType) {
        if (!splitType[0].equals("varbinary") && !splitType[0].equals("binary")) {
            // Blob
            return VarbinaryType.createVarbinaryType(Integer.MAX_VALUE);
        }
        if (splitType[1] == null) {
            throw new IllegalArgumentException("Must have size for varbinary type");
        }

        return VarbinaryType.createVarbinaryType(Integer.parseInt(splitType[1]));
    }
}
