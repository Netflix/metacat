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

import com.netflix.metacat.common.type.BaseType;
import com.netflix.metacat.common.type.CharType;
import com.netflix.metacat.common.type.DecimalType;
import com.netflix.metacat.common.type.Type;
import com.netflix.metacat.common.type.VarbinaryType;
import com.netflix.metacat.common.type.VarcharType;
import com.netflix.metacat.connector.jdbc.JdbcTypeConverter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

/**
 * Type converter for MySQL.
 *
 * @author tgianos
 * @since 1.0.0
 */
@Slf4j
public class MySqlTypeConverter extends JdbcTypeConverter {

    static final int MAX_BYTE_LENGTH = 65_535;
    private static final int MIN_BYTE_LENGTH = 0;

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
                return this.toMetacatTimeType(splitType);
            case "timestamp":
                return this.toMetacatTimestampType(splitType);
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
                throw new UnsupportedOperationException("Encountered " + splitType[0] + " type. Ignoring");
            default:
                throw new IllegalArgumentException("Unhandled or unknown sql type " + splitType[0]);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String fromMetacatType(@Nonnull @NonNull final Type type) {
        switch (type.getTypeSignature().getBase()) {
            case ARRAY:
                throw new UnsupportedOperationException("MySQL doesn't support array types");
            case BIGINT:
                return "BIGINT";
            case BOOLEAN:
                return "BOOLEAN";
            case CHAR:
                if (!(type instanceof CharType)) {
                    throw new IllegalArgumentException("Expected char type but was " + type.getClass().getName());
                }
                final CharType charType = (CharType) type;
                final int charLength = charType.getLength();
                if (charLength < MIN_BYTE_LENGTH) {
                    throw new IllegalArgumentException("CHAR type must have a length > 0");
                }
                // NOTE: Note that for MySQL the max column size is 65,535 bytes so technically you can have a table
                //       of a single char column of this length but that's it. Hard to handle that in here when
                //       just doing the conversions. It would have to be handled by higher level logic that had the
                //       entire picture.
                if (charLength <= MAX_BYTE_LENGTH) {
                    return "CHAR(" + charLength + ")";
                } else {
                    return "TEXT";
                }
            case DATE:
                return "DATE";
            case DECIMAL:
                if (!(type instanceof DecimalType)) {
                    throw new IllegalArgumentException("Expected decimal type but was " + type.getClass().getName());
                }
                final DecimalType decimalType = (DecimalType) type;
                return "DECIMAL(" + decimalType.getPrecision() + ", " + decimalType.getScale() + ")";
            case DOUBLE:
                return "DOUBLE";
            case FLOAT:
                return "FLOAT(24)";
            case INT:
                return "INT";
            case INTERVAL_DAY_TO_SECOND:
                throw new UnsupportedOperationException("MySQL doesn't support interval types");
            case INTERVAL_YEAR_TO_MONTH:
                throw new UnsupportedOperationException("MySQL doesn't support interval types");
            case JSON:
                return "JSON";
            case MAP:
                throw new UnsupportedOperationException("MySQL doesn't support map types");
            case ROW:
                throw new UnsupportedOperationException("MySQL doesn't support row types");
            case SMALLINT:
                return "SMALLINT";
            case STRING:
                return "TEXT";
            case TIME:
            case TIME_WITH_TIME_ZONE:
                return "TIME";
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
                return "TIMESTAMP";
            case TINYINT:
                return "TINYINT";
            case UNKNOWN:
                throw new IllegalArgumentException("Can't map an unknown type");
            case VARBINARY:
                if (!(type instanceof VarbinaryType)) {
                    throw new IllegalArgumentException("Expected varbinary type but was " + type.getClass().getName());
                }
                final VarbinaryType varbinaryType = (VarbinaryType) type;
                final int binaryLength = varbinaryType.getLength();
                if (binaryLength < MIN_BYTE_LENGTH) {
                    throw new IllegalArgumentException("VARBINARY type must have a length > 0");
                }
                // NOTE: Note that for MySQL the max column size is 65,535 bytes so technically you can have a table
                //       of a single varbinary column of this length but that's it. Hard to handle that in here when
                //       just doing the conversions. It would have to be handled by higher level logic that had the
                //       entire picture.
                if (binaryLength <= MAX_BYTE_LENGTH) {
                    return "VARBINARY(" + binaryLength + ")";
                } else {
                    return "BLOB";
                }
            case VARCHAR:
                if (!(type instanceof VarcharType)) {
                    throw new IllegalArgumentException("Expected varchar type but was " + type.getClass().getName());
                }
                final VarcharType varcharType = (VarcharType) type;
                final int varCharLength = varcharType.getLength();
                if (varCharLength < MIN_BYTE_LENGTH) {
                    throw new IllegalArgumentException("VARCHAR type must have a length > 0");
                }
                // NOTE: Note that for MySQL the max column size is 65,535 bytes so technically you can have a table
                //       of a single varchar column of this length but that's it. Hard to handle that in here when
                //       just doing the conversions. It would have to be handled by higher level logic that had the
                //       entire picture.
                if (varCharLength <= MAX_BYTE_LENGTH) {
                    return "VARCHAR(" + varCharLength + ")";
                } else {
                    return "TEXT";
                }
            default:
                throw new IllegalArgumentException("Unknown type " + type.getTypeSignature().getBase());
        }
    }
}
