/*
 *
 * Copyright 2018 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */
package com.netflix.metacat.connector.snowflake

import com.google.common.collect.Lists
import com.netflix.metacat.common.type.*
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Specifications for the SnowflakeTypeConverter.
 *
 * @author amajumdar
 * @since 1.2.0
 */
class SnowflakeTypeConverterSpec extends Specification {

    def converter = new SnowflakeTypeConverter()

    @Unroll
    "Can convert Snowflake string: #type to Metacat Type: #metacatType"() {

        expect:
        this.converter.toMetacatType(type) == metacatType

        where:
        type                           | metacatType
        "SMALLINT"                     | BaseType.SMALLINT
        "int"                          | BaseType.INT
        "INteger"                      | BaseType.INT
        " BIGINt"                      | BaseType.BIGINT
        "decimal (15)"                 | DecimalType.createDecimalType(15)
        "DECIMAL"                      | DecimalType.createDecimalType()
        "DEciMal(15, 14)"              | DecimalType.createDecimalType(15, 14)
        "numeric"                      | DecimalType.createDecimalType()
        "REAL"                         | BaseType.FLOAT
        "float4"                       | BaseType.FLOAT
        "double PRECISION (15)"        | BaseType.DOUBLE
        "double PRECISION (15, 3)"     | BaseType.DOUBLE
        "double precision"             | BaseType.DOUBLE
        "FLOAT8"                       | BaseType.DOUBLE
        "float"                        | BaseType.DOUBLE
        "varchar(53)"                  | VarcharType.createVarcharType(53)
        "text"                         | VarcharType.createVarcharType(SnowflakeTypeConverter.DEFAULT_CHARACTER_LENGTH)
        "character (3)"                | CharType.createCharType(3)
        "char(18)"                     | CharType.createCharType(18)
        "timestamp (7) with time zone" | BaseType.TIMESTAMP_WITH_TIME_ZONE
        "TIMESTAMP(16)"                | BaseType.TIMESTAMP
        "date"                         | BaseType.DATE
        "BOOLEAN"                      | BaseType.BOOLEAN
        "boolean"                      | BaseType.BOOLEAN
        "bit"                          | BaseType.UNKNOWN
        "bit varying"                  | BaseType.UNKNOWN
        "bytea"                        | BaseType.UNKNOWN
        "row"                          | BaseType.UNKNOWN
        "map"                          | BaseType.UNKNOWN
        "time"                         | BaseType.UNKNOWN
        "time with time zone"          | BaseType.UNKNOWN
        "timez"                        | BaseType.UNKNOWN
        "json"                         | BaseType.UNKNOWN
        "bigserial"                    | BaseType.UNKNOWN
        "serial8"                      | BaseType.UNKNOWN
        "money"                        | BaseType.UNKNOWN
        "interval"                     | BaseType.UNKNOWN
        "enum"                         | BaseType.UNKNOWN
        "point"                        | BaseType.UNKNOWN
        "line"                         | BaseType.UNKNOWN
        "lseg"                         | BaseType.UNKNOWN
        "box"                          | BaseType.UNKNOWN
        "path"                         | BaseType.UNKNOWN
        "polygon"                      | BaseType.UNKNOWN
        "circle"                       | BaseType.UNKNOWN
        "cidr"                         | BaseType.UNKNOWN
        "inet"                         | BaseType.UNKNOWN
        "macaddr"                      | BaseType.UNKNOWN
        "tsvector"                     | BaseType.UNKNOWN
        "tsquery"                      | BaseType.UNKNOWN
        "uuid"                         | BaseType.UNKNOWN
        "xml"                          | BaseType.UNKNOWN
        "int4range"                    | BaseType.UNKNOWN
        "int8range"                    | BaseType.UNKNOWN
        "numrange"                     | BaseType.UNKNOWN
        "tsrange"                      | BaseType.UNKNOWN
        "tstzrange"                    | BaseType.UNKNOWN
        "daterange"                    | BaseType.UNKNOWN
        "regproc"                      | BaseType.UNKNOWN
        "regprocedure"                 | BaseType.UNKNOWN
        "regoper"                      | BaseType.UNKNOWN
        "regoperator"                  | BaseType.UNKNOWN
        "regclass"                     | BaseType.UNKNOWN
        "regtype"                      | BaseType.UNKNOWN
        "regrole"                      | BaseType.UNKNOWN
        "regnamespace"                 | BaseType.UNKNOWN
        "regconfig"                    | BaseType.UNKNOWN
        "regdictionary"                | BaseType.UNKNOWN
        "pg_lsn"                       | BaseType.UNKNOWN
        "jsonb"                        | BaseType.UNKNOWN
        "txid_snapshot"                | BaseType.UNKNOWN
    }

    @Unroll
    "Can convert type #type to Snowflake string #sql"() {

        expect:
        this.converter.fromMetacatType(type) == sql

        where:
        type                                  | sql
        BaseType.BIGINT                       | "NUMBER(38)"
        BaseType.BOOLEAN                      | "BOOLEAN"
        CharType.createCharType(42)           | "CHAR(42)"
        BaseType.DATE                         | "DATE"
        DecimalType.createDecimalType(15, 14) | "DECIMAL(15, 14)"
        DecimalType.createDecimalType(13, 12) | "DECIMAL(13, 12)"
        DecimalType.createDecimalType(13)     | "DECIMAL(13, 0)"
        DecimalType.createDecimalType()       | "DECIMAL(10, 0)"
        BaseType.DOUBLE                       | "DOUBLE PRECISION"
        BaseType.FLOAT                        | "DOUBLE PRECISION"
        BaseType.INT                          | "INT"
        BaseType.SMALLINT                     | "SMALLINT"
        BaseType.TIMESTAMP                    | "TIMESTAMP"
        BaseType.TIMESTAMP_WITH_TIME_ZONE     | "TIMESTAMPTZ"
        BaseType.TINYINT                      | "SMALLINT"
        VarcharType.createVarcharType(255)    | "VARCHAR(255)"
    }

    @Unroll
    "Can't convert type #type back to SQL"() {
        when:
        this.converter.fromMetacatType(type)

        then:
        thrown exception

        where:
        type                                       | exception
        BaseType.INTERVAL_DAY_TO_SECOND            | UnsupportedOperationException
        BaseType.INTERVAL_YEAR_TO_MONTH            | UnsupportedOperationException
        new MapType(BaseType.STRING, BaseType.INT) | UnsupportedOperationException
        RowType.createRowType(
            Lists.asList(BaseType.STRING),
            Lists.asList(UUID.randomUUID().toString())
        )                                          | UnsupportedOperationException
        BaseType.UNKNOWN                           | IllegalArgumentException
        new ArrayType(BaseType.DOUBLE)             | UnsupportedOperationException
        BaseType.JSON                              | UnsupportedOperationException
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
