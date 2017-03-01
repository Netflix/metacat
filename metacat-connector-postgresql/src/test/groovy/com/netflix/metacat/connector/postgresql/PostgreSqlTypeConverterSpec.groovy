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
package com.netflix.metacat.connector.postgresql

import com.netflix.metacat.common.type.*
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Specifications for the PostgresSqlTypeConverter.
 *
 * @author tgianos
 * @since 0.1.52
 */
class PostgreSqlTypeConverterSpec extends Specification {

    def converter = new PostgreSqlTypeConverter()

    @Unroll
    "Can convert PostgreSQL string: #type to Metacat Type: #signature"() {

        expect:
        this.converter.toMetacatType(type).getTypeSignature().toString() == signature

        where:
        type                           | signature
        "SMALLINT"                     | BaseType.SMALLINT.getTypeSignature().toString()
        "int"                          | BaseType.INT.getTypeSignature().toString()
        " BIGINt"                      | BaseType.BIGINT.getTypeSignature().toString()
        "decimal (15)"                 | DecimalType.createDecimalType(15).getTypeSignature().toString()
        "DECIMAL"                      | DecimalType.createDecimalType().getTypeSignature().toString()
        "DEciMal(15, 14)"              | DecimalType.createDecimalType(15, 14).getTypeSignature().toString()
        "numeric"                      | DecimalType.createDecimalType().getTypeSignature().toString()
        "REAL"                         | BaseType.FLOAT.getTypeSignature().toString()
        "double PRECISION (15)"        | BaseType.DOUBLE.getTypeSignature().toString()
        "double PRECISION (15, 3)"     | BaseType.DOUBLE.getTypeSignature().toString()
        "double precision"             | BaseType.DOUBLE.getTypeSignature().toString()
        "character varying (17)"       | VarcharType.createVarcharType(17).getTypeSignature().toString()
        "varchar(53)"                  | VarcharType.createVarcharType(53).getTypeSignature().toString()
        "character (3)"                | CharType.createCharType(3).getTypeSignature().toString()
        "char(18)"                     | CharType.createCharType(18).getTypeSignature().toString()
        "TEXT"                         | BaseType.STRING.getTypeSignature().toString()
        "bytea"                        | VarbinaryType
            .createVarbinaryType(Integer.MAX_VALUE)
            .getTypeSignature()
            .toString()
        "timestamp (7) with time zone" | BaseType.TIMESTAMP_WITH_TIME_ZONE.getTypeSignature().toString()
        "TIMESTAMP(16)"                | BaseType.TIMESTAMP.getTypeSignature().toString()
        "date"                         | BaseType.DATE.getTypeSignature().toString()
        "TIME WITH TIME ZONE"          | BaseType.TIME_WITH_TIME_ZONE.getTypeSignature().toString()
        "time"                         | BaseType.TIME.getTypeSignature().toString()
        "BOOLEAN"                      | BaseType.BOOLEAN.getTypeSignature().toString()
        "bit(2)"                       | VarbinaryType.createVarbinaryType(1).getTypeSignature().toString()
        "bit varying (9)"              | VarbinaryType.createVarbinaryType(2).getTypeSignature().toString()
        "JSON"                         | BaseType.JSON.getTypeSignature().toString()
        "int []"                       | new ArrayType(BaseType.INT).getTypeSignature().toString()
        "double precision[]"           | new ArrayType(BaseType.DOUBLE).getTypeSignature().toString()
        "text ARRAY"                   | new ArrayType(BaseType.STRING).getTypeSignature().toString()
        "JSON[][]"                     | new ArrayType(new ArrayType(BaseType.JSON)).getTypeSignature().toString()
    }

    @Unroll
    "Can't process unsupported type #type"() {

        when:
        this.converter.toMetacatType(type)

        then:
        thrown UnsupportedOperationException

        where:
        type            | _
        "smallserial"   | _
        "serial"        | _
        "bigserial"     | _
        "money"         | _
        "interval"      | _
        "enum"          | _
        "point"         | _
        "line"          | _
        "lseg"          | _
        "box"           | _
        "path"          | _
        "polygon"       | _
        "circle"        | _
        "cidr"          | _
        "inet"          | _
        "macaddr"       | _
        "tsvector"      | _
        "tsquery"       | _
        "uuid"          | _
        "xml"           | _
        "int4range"     | _
        "int8range"     | _
        "numrange"      | _
        "tsrange"       | _
        "tstzrange"     | _
        "daterange"     | _
        "oid"           | _
        "regproc"       | _
        "regprocedure"  | _
        "regoper"       | _
        "regoperator"   | _
        "regclass"      | _
        "regtype"       | _
        "regrole"       | _
        "regnamespace"  | _
        "regconfig"     | _
        "regdictionary" | _
        "pg_lsn"        | _
    }

    @Unroll
    "Can Split Type: #type into base: #base, array: #array, size: #size, magnitude: #magnitude and extra: #extra"() {
        expect:
        def split = this.converter.splitType(type)
        split.length == 5
        split[0] == base
        split[1] == array
        split[2] == size
        split[3] == magnitude
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
    }

    def "Can't split up a bad input"() {
        when:
        this.converter.splitType("()")

        then:
        thrown IllegalArgumentException
    }
}
