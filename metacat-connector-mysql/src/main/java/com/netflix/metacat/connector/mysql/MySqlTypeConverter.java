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
import com.netflix.metacat.common.type.Type;
import com.netflix.metacat.connector.jdbc.JdbcTypeConverter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

/**
 * Type converter for MySQL.
 *
 * @author tgianos
 * @since 0.1.52
 */
@Slf4j
public class MySqlTypeConverter extends JdbcTypeConverter {

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
}
