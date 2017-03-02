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
package com.netflix.metacat.connector.mysql

import com.google.common.collect.Lists
import com.netflix.metacat.common.type.*
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Specifications for the MySqlTypeConverter.
 *
 * @author tgianos
 * @since 0.1.52
 */
class MySqlTypeConverterSpec extends Specification {

    def converter = new MySqlTypeConverter()

    @Unroll
    "Can convert MySQL string: #type to Metacat Type: #signature"() {

        expect:
        this.converter.toMetacatType(type).getTypeSignature().toString() == signature

        where:
        type                    | signature
        "bit (1)"               | BaseType.BOOLEAN.getTypeSignature().toString()
        "bit"                   | BaseType.BOOLEAN.getTypeSignature().toString()
        "BIT(16)"               | VarbinaryType.createVarbinaryType(2).getTypeSignature().toString()
        "TINYINT"               | BaseType.TINYINT.getTypeSignature().toString()
        "bool"                  | BaseType.BOOLEAN.getTypeSignature().toString()
        "BOOLEAN"               | BaseType.BOOLEAN.getTypeSignature().toString()
        "sMaLLInt"              | BaseType.SMALLINT.getTypeSignature().toString()
        "MEDIUMINT"             | BaseType.INT.getTypeSignature().toString()
        "INT"                   | BaseType.INT.getTypeSignature().toString()
        "integer"               | BaseType.INT.getTypeSignature().toString()
        "bigint"                | BaseType.BIGINT.getTypeSignature().toString()
        "float(53)"             | BaseType.FLOAT.getTypeSignature().toString()
        "DOUBLE"                | BaseType.DOUBLE.getTypeSignature().toString()
        "double PRECISION (15)" | BaseType.DOUBLE.getTypeSignature().toString()
        "DEC (15, 14)"          | DecimalType.createDecimalType(15, 14).getTypeSignature().toString()
        "DEcimal ( 13 , 12 )"   | DecimalType.createDecimalType(13, 12).getTypeSignature().toString()
        "Dec ( 13 )"            | DecimalType.createDecimalType(13).getTypeSignature().toString()
        "decimal"               | DecimalType.createDecimalType().getTypeSignature().toString()
        "date"                  | BaseType.DATE.getTypeSignature().toString()
        "datetime"              | BaseType.TIME.getTypeSignature().toString()
        "time"                  | BaseType.TIME.getTypeSignature().toString()
        "timeStamp"             | BaseType.TIMESTAMP.getTypeSignature().toString()
        "char(42)"              | CharType.createCharType(42).getTypeSignature().toString()
        "char (52) binary"      | VarbinaryType.createVarbinaryType(52).getTypeSignature().toString()
        "varchar (255)"         | VarcharType.createVarcharType(255).getTypeSignature().toString()
        "varchar(255) BINARY"   | VarbinaryType.createVarbinaryType(255).getTypeSignature().toString()
        "binary(165)"           | VarbinaryType.createVarbinaryType(165).getTypeSignature().toString()
        "BLOB"                  | VarbinaryType.createVarbinaryType(Integer.MAX_VALUE).getTypeSignature().toString()
        "mediumBlob"            | VarbinaryType.createVarbinaryType(Integer.MAX_VALUE).getTypeSignature().toString()
        "longblob"              | VarbinaryType.createVarbinaryType(Integer.MAX_VALUE).getTypeSignature().toString()
        "varbinary (234)"       | VarbinaryType.createVarbinaryType(234).getTypeSignature().toString()
        "tinytext garbage"      | BaseType.STRING.getTypeSignature().toString()
        "text"                  | BaseType.STRING.getTypeSignature().toString()
        "mediumtext"            | BaseType.STRING.getTypeSignature().toString()
        "longtext"              | BaseType.STRING.getTypeSignature().toString()
        "json"                  | BaseType.JSON.getTypeSignature().toString()
    }

    @Unroll
    "Can't process unsupported type #type"() {

        when:
        this.converter.toMetacatType(type)

        then:
        thrown UnsupportedOperationException

        where:
        type   | _
        "year" | _
        "enum" | _
        "set"  | _
    }

    @Unroll
    "Can convert type #type to MySQL string #sql"() {

        expect:
        this.converter.fromMetacatType(type) == sql

        where:
        type                                                                      | sql
        BaseType.BIGINT                                                           | "BIGINT"
        BaseType.BOOLEAN                                                          | "BOOLEAN"
        CharType.createCharType(42)                                               | "CHAR(42)"
        CharType.createCharType(MySqlTypeConverter.MAX_BYTE_LENGTH)               |
            "CHAR(" + MySqlTypeConverter.MAX_BYTE_LENGTH + ")"
        CharType.createCharType(MySqlTypeConverter.MAX_BYTE_LENGTH + 1)           | "TEXT"
        BaseType.DATE                                                             | "DATE"
        DecimalType.createDecimalType(15, 14)                                     | "DECIMAL(15, 14)"
        DecimalType.createDecimalType(13, 12)                                     | "DECIMAL(13, 12)"
        DecimalType.createDecimalType(13)                                         | "DECIMAL(13, 0)"
        DecimalType.createDecimalType()                                           | "DECIMAL(10, 0)"
        BaseType.DOUBLE                                                           | "DOUBLE"
        BaseType.FLOAT                                                            | "FLOAT(24)"
        BaseType.INT                                                              | "INT"
        BaseType.JSON                                                             | "JSON"
        BaseType.SMALLINT                                                         | "SMALLINT"
        BaseType.STRING                                                           | "TEXT"
        BaseType.TIME                                                             | "TIME"
        BaseType.TIME_WITH_TIME_ZONE                                              | "TIME"
        BaseType.TIMESTAMP                                                        | "TIMESTAMP"
        BaseType.TIMESTAMP_WITH_TIME_ZONE                                         | "TIMESTAMP"
        BaseType.TINYINT                                                          | "TINYINT"
        VarbinaryType.createVarbinaryType(2)                                      | "VARBINARY(2)"
        VarbinaryType.createVarbinaryType(52)                                     | "VARBINARY(52)"
        VarbinaryType.createVarbinaryType(MySqlTypeConverter.MAX_BYTE_LENGTH)     |
            "VARBINARY(" + MySqlTypeConverter.MAX_BYTE_LENGTH + ")"
        VarbinaryType.createVarbinaryType(MySqlTypeConverter.MAX_BYTE_LENGTH + 1) | "BLOB"
        VarcharType.createVarcharType(255)                                        | "VARCHAR(255)"
        VarcharType.createVarcharType(MySqlTypeConverter.MAX_BYTE_LENGTH)         |
            "VARCHAR(" + MySqlTypeConverter.MAX_BYTE_LENGTH + ")"
        VarcharType.createVarcharType(MySqlTypeConverter.MAX_BYTE_LENGTH + 1)     | "TEXT"
    }

    @Unroll
    "Can't convert type #type back to SQL"() {
        when:
        this.converter.fromMetacatType(type)

        then:
        thrown exception

        where:
        type                                       | exception
        new ArrayType(BaseType.DOUBLE)             | UnsupportedOperationException
        BaseType.INTERVAL_DAY_TO_SECOND            | UnsupportedOperationException
        BaseType.INTERVAL_YEAR_TO_MONTH            | UnsupportedOperationException
        new MapType(BaseType.STRING, BaseType.INT) | UnsupportedOperationException
        new RowType(
            Lists.asList(BaseType.STRING),
            Lists.asList(UUID.randomUUID().toString())
        )                                          | UnsupportedOperationException
        BaseType.UNKNOWN                           | IllegalArgumentException
//        new CharType(MySqlTypeConverter.MIN_BYTE_LENGTH - 1)                      | IllegalArgumentException
//        VarbinaryType.createVarbinaryType(MySqlTypeConverter.MIN_BYTE_LENGTH - 1) | IllegalArgumentException
//        VarcharType.createVarcharType(MySqlTypeConverter.MIN_BYTE_LENGTH - 1)     | IllegalArgumentException
    }

    @Unroll
    "Can Split Type: #type into base: #base, array: #array, size: #size, magnitude: #magnitude and extra: #extra"() {
        expect:
        def split = this.converter.splitType(type)
        split.length == 5
        split[0] == base
        split[1] == size
        split[2] == magnitude
        split[3] == array
        split[4] == extra

        where:
        type                       | base        | array  | size | magnitude | extra
        "int(32) unsigned"         | "int"       | null   | "32" | null      | "unsigned"
        "  int (32) unsigned"      | "int"       | null   | "32" | null      | "unsigned"
        "int ( 32)"                | "int"       | null   | "32" | null      | null
        "int(32   )"               | "int"       | null   | "32" | null      | null
        "decimal (32, 32)"         | "decimal"   | null   | "32" | "32"      | null
        "decimal (32 , 32) signed" | "decimal"   | null   | "32" | "32"      | "signed"
        "int unsigned"             | "int"       | null   | null | null      | "unsigned"
        "int"                      | "int"       | null   | null | null      | null
        "timestamp with time zone" | "timestamp" | null   | null | null      | "with time zone"
        "integer []"               | "integer"   | "[]"   | null | null      | null
        "int[][]"                  | "int"       | "[][]" | null | null      | null
        "char (4) [][]"            | "char"      | "[][]" | "4"  | null      | null
        "char(4)[]"                | "char"      | "[]"   | "4"  | null      | null
    }

    def "Can't split up a bad input"() {
        when:
        this.converter.splitType("()")

        then:
        thrown IllegalArgumentException
    }
}
