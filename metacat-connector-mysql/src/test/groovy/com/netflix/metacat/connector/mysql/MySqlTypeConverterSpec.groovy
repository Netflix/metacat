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
    "Can convert to metacat type from string"() {

        expect:
        this.converter.toMetacatType(type).getTypeSignature().toString() == signature

        where:
        type                     | signature
        "bit (1)"                | BaseType.BOOLEAN.getTypeSignature().toString()
        "bit"                    | BaseType.BOOLEAN.getTypeSignature().toString()
        "BIT(16)"                | VarbinaryType.createVarbinaryType(2).getTypeSignature().toString()
        "TINYINT"                | BaseType.TINYINT.getTypeSignature().toString()
        "bool"                   | BaseType.BOOLEAN.getTypeSignature().toString()
        "BOOLEAN"                | BaseType.BOOLEAN.getTypeSignature().toString()
        "sMaLLInt"               | BaseType.SMALLINT.getTypeSignature().toString()
        "MEDIUMINT"              | BaseType.INT.getTypeSignature().toString()
        "INT"                    | BaseType.INT.getTypeSignature().toString()
        "integer"                | BaseType.INT.getTypeSignature().toString()
        "bigint"                 | BaseType.BIGINT.getTypeSignature().toString()
        "float(53)"              | BaseType.FLOAT.getTypeSignature().toString()
        "DOUBLE"                 | BaseType.DOUBLE.getTypeSignature().toString()
//        "double PRECISION (15) " | BaseType.DOUBLE.getTypeSignature().toString()
        "DEC (15, 14)"           | DecimalType.createDecimalType(15, 14).getTypeSignature().toString()
        "DEcimal ( 13 , 12 )"    | DecimalType.createDecimalType(13, 12).getTypeSignature().toString()
        "Dec ( 13 )"             | DecimalType.createDecimalType(13).getTypeSignature().toString()
        "decimal"                | DecimalType.createDecimalType().getTypeSignature().toString()
        "date"                   | BaseType.DATE.getTypeSignature().toString()
        "datetime"               | BaseType.TIME.getTypeSignature().toString()
        "time"                   | BaseType.TIME.getTypeSignature().toString()
        "timeStamp"              | BaseType.TIME.getTypeSignature().toString()
        "char(42)"               | CharType.createCharType(42).getTypeSignature().toString()
        "char (52) binary"       | VarbinaryType.createVarbinaryType(52).getTypeSignature().toString()
        "varchar (255)"          | VarcharType.createVarcharType(255).getTypeSignature().toString()
        "varchar(255) BINARY"    | VarbinaryType.createVarbinaryType(255).getTypeSignature().toString()
        "binary(165)"            | VarbinaryType.createVarbinaryType(165).getTypeSignature().toString()
        "BLOB"                   | VarbinaryType.createVarbinaryType(Integer.MAX_VALUE).getTypeSignature().toString()
        "mediumBlob"             | VarbinaryType.createVarbinaryType(Integer.MAX_VALUE).getTypeSignature().toString()
        "longblob"               | VarbinaryType.createVarbinaryType(Integer.MAX_VALUE).getTypeSignature().toString()
        "varbinary (234)"        | VarbinaryType.createVarbinaryType(234).getTypeSignature().toString()
        "tinytext garbage"       | BaseType.STRING.getTypeSignature().toString()
        "text"                   | BaseType.STRING.getTypeSignature().toString()
        "mediumtext"             | BaseType.STRING.getTypeSignature().toString()
        "longtext"               | BaseType.STRING.getTypeSignature().toString()
        "json"                   | BaseType.JSON.getTypeSignature().toString()
    }

    @Unroll
    "Can Split Types"() {
        expect:
        def split = this.converter.splitType(type)
        split.length == 4
        split[0] == base
        split[1] == size
        split[2] == magnitude
        split[3] == extra

        where:
        type                       | base      | size | magnitude | extra
        "int(32) unsigned"         | "int"     | "32" | null      | "unsigned"
        "  int (32) unsigned"      | "int"     | "32" | null      | "unsigned"
        "int ( 32)"                | "int"     | "32" | null      | null
        "int(32   )"               | "int"     | "32" | null      | null
        "decimal (32, 32)"         | "decimal" | "32" | "32"      | null
        "decimal (32 , 32) signed" | "decimal" | "32" | "32"      | "signed"
        "int unsigned"             | "int"     | null | null      | "unsigned"
        "int"                      | "int"     | null | null      | null
    }

    def "Can't split up a bad input"() {
        when:
        this.converter.splitType("()")

        then:
        thrown IllegalArgumentException
    }
}
