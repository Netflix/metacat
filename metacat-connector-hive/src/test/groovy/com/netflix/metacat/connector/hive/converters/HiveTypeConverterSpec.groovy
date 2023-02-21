/*
 *  Copyright 2016 Netflix, Inc.
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *         http://www.apache.org/licenses/LICENSE-2.0
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.netflix.metacat.connector.hive.converters

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Unit test for hive and canonical type converter.
 * @author zhenl
 * @since 1.0.0
 */
class HiveTypeConverterSpec extends Specification {
    @Shared
    HiveTypeConverter converter = new HiveTypeConverter()

    @Unroll
    def 'can convert "#typeString" to a presto type and back'(String typeString) {
        expect:
        def metacatType = converter.toMetacatType(typeString)
        def hiveType = converter.fromMetacatType(metacatType)
        def metacatTypeFromHiveType = converter.toMetacatType(hiveType)
        metacatTypeFromHiveType ==  metacatType
        where:
        typeString << [
            'tinyint',
            'smallint',
            'int',
            'bigint',
            'float',
            'double',
            'decimal',
            'decimal(4,2)',
            'array<decimal(4,2)>',
            'timestamp',
            'date',
            'string',
            'varchar(10)',
            'char(10)',
            'boolean',
            'binary',
            'array<bigint>',
            'array<boolean>',
            'array<double>',
            'array<bigint>',
            'array<string>',
            'array<map<bigint,bigint>>',
            'array<map<bigint,string>>',
            'array<map<string,bigint>>',
            'array<map<string,string>>',
            'array<struct<field1:bigint,field2:bigint>>',
            'array<struct<field1:bigint,field2:string>>',
            'array<struct<field1:string,field2:bigint>>',
            'array<struct<field1:string,field2:string>>',
            'map<boolean,boolean>',
            'map<boolean,string>',
            'map<bigint,bigint>',
            'map<string,double>',
            'map<string,bigint>',
            'map<string,string>',
            'map<string,struct<field1:array<bigint>>>',
            'struct<field1:bigint,field2:bigint,field3:bigint>',
            'struct<field1:bigint,field2:string,field3:double>',
            'struct<field1:bigint,field2:string,field3:string>',
            'struct<field1:string,field2:bigint,field3:bigint>',
            'struct<field1:string,field2:string,field3:bigint>',
            'struct<field1:string,field2:string,field3:string>',

            "decimal",
            "decimal( 38)",
            "decimal( 38, 9)",
            "decimal(38, 9)",
            "decimal(38,  9)",
            "decimal(38,9)",
            "decimal(38,9 )",
            "decimal (38,9 )",
            "decimal (38)",
            "decimal(38 )",
            "array<decimal>",
            "array<decimal(38, 9)>",
            "array<decimal(38,  9)>",
            "array<decimal(38,9)>",
            "array<decimal(38,9 )>",
            "array<decimal (38,9 )>",
            "array<decimal (38)>",
            "array<decimal(38 )>",
            "struct<field1:string,field2:decimal,field3:bigint>",
            "struct<field1:string,field2:decimal(38, 9),field3:bigint>",
            "struct<field1:string,field2:decimal( 38, 9),field3:bigint>",
            "struct<field1:string,field2:decimal(38,  9),field3:bigint>",
            "struct<field1:string,field2:decimal(38,9),field3:bigint>",
            "struct<field1:string,field2:decimal(38,9 ),field3:bigint>",
            "struct<field1:string,field2:decimal (38,9 ),field3:bigint>",
            "struct<field1:string,field2:decimal (38),field3:bigint>",
            "struct<field1:string,field2:decimal(38 ),field3:bigint>",
        ]
    }

    @Unroll
    def "handles sanitization of decimal declarations with spaces"() {
        when:
        def sanitizedType = HiveTypeConverter.sanitizeType(input)

        then:
        sanitizedType == expected

        where:
        input                                                         || expected
        "tinyint"                                                     || "tinyint"
        "smallint"                                                    || "smallint"
        "int"                                                         || "int"
        "bigint"                                                      || "bigint"
        "float"                                                       || "float"
        "double"                                                      || "double"
        "decimal"                                                     || "decimal"
        "decimal(4,2)"                                                || "decimal(4,2)"
        "array<decimal(4,2)>"                                         || "array<decimal(4,2)>"
        "timestamp"                                                   || "timestamp"
        "date"                                                        || "date"
        "string"                                                      || "string"
        "varchar(10)"                                                 || "varchar(10)"
        "char(10)"                                                    || "char(10)"
        "boolean"                                                     || "boolean"
        "binary"                                                      || "binary"
        "array<bigint>"                                               || "array<bigint>"
        "array<boolean>"                                              || "array<boolean>"
        "array<double>"                                               || "array<double>"
        "array<bigint>"                                               || "array<bigint>"
        "array<string>"                                               || "array<string>"
        "array<map<bigint,bigint>>"                                   || "array<map<bigint,bigint>>"
        "array<map<bigint,string>>"                                   || "array<map<bigint,string>>"
        "array<map<string,bigint>>"                                   || "array<map<string,bigint>>"
        "array<map<string,string>>"                                   || "array<map<string,string>>"
        "array<struct<field1:bigint,field2:bigint>>"                  || "array<struct<field1:bigint,field2:bigint>>"
        "array<struct<field1:bigint,field2:string>>"                  || "array<struct<field1:bigint,field2:string>>"
        "array<struct<field1:string,field2:bigint>>"                  || "array<struct<field1:string,field2:bigint>>"
        "array<struct<field1:string,field2:string>>"                  || "array<struct<field1:string,field2:string>>"
        "map<boolean,boolean>"                                        || "map<boolean,boolean>"
        "map<boolean,string>"                                         || "map<boolean,string>"
        "map<bigint,bigint>"                                          || "map<bigint,bigint>"
        "map<string,double>"                                          || "map<string,double>"
        "map<string,bigint>"                                          || "map<string,bigint>"
        "map<string,string>"                                          || "map<string,string>"
        "map<string,struct<field1:array<bigint>>>"                    || "map<string,struct<field1:array<bigint>>>"
        "struct<field1:bigint,field2:bigint,field3:bigint>"           || "struct<field1:bigint,field2:bigint,field3:bigint>"
        "struct<field1:bigint,field2:string,field3:double>"           || "struct<field1:bigint,field2:string,field3:double>"
        "struct<field1:bigint,field2:string,field3:string>"           || "struct<field1:bigint,field2:string,field3:string>"
        "struct<field1:string,field2:bigint,field3:bigint>"           || "struct<field1:string,field2:bigint,field3:bigint>"
        "struct<field1:string,field2:string,field3:bigint>"           || "struct<field1:string,field2:string,field3:bigint>"
        "struct<field1:string,field2:string,field3:string>"           || "struct<field1:string,field2:string,field3:string>"

        // problem types starts here; we expect the spaces in the decimal declarations to be removed
        "decimal"                                                     || "decimal"
        "decimal( 38)"                                                || "decimal(38)"
        "decimal( 38, 9)"                                             || "decimal(38,9)"
        "decimal(38, 9)"                                              || "decimal(38,9)"
        "decimal(38,  9)"                                             || "decimal(38,9)"
        "decimal(38,9)"                                               || "decimal(38,9)"
        "decimal(38,9 )"                                              || "decimal(38,9)"
        "decimal (38,9 )"                                             || "decimal(38,9)"
        "decimal (38)"                                                || "decimal(38)"
        "decimal(38 )"                                                || "decimal(38)"
        "array<decimal>"                                              || "array<decimal>"
        "array<decimal(38, 9)>"                                       || "array<decimal(38,9)>"
        "array<decimal(38,  9)>"                                      || "array<decimal(38,9)>"
        "array<decimal(38,9)>"                                        || "array<decimal(38,9)>"
        "array<decimal(38,9 )>"                                       || "array<decimal(38,9)>"
        "array<decimal (38,9 )>"                                      || "array<decimal(38,9)>"
        "array<decimal (38)>"                                         || "array<decimal(38)>"
        "array<decimal(38 )>"                                         || "array<decimal(38)>"
        "struct<field1:string,field2:decimal,field3:bigint>"          || "struct<field1:string,field2:decimal,field3:bigint>"
        "struct<field1:string,field2:decimal(38, 9),field3:bigint>"   || "struct<field1:string,field2:decimal(38,9),field3:bigint>"
        "struct<field1:string,field2:decimal( 38, 9),field3:bigint>"  || "struct<field1:string,field2:decimal(38,9),field3:bigint>"
        "struct<field1:string,field2:decimal( 38 , 9),field3:bigint>" || "struct<field1:string,field2:decimal(38,9),field3:bigint>"
        "struct<field1:string,field2:decimal(38,  9),field3:bigint>"  || "struct<field1:string,field2:decimal(38,9),field3:bigint>"
        "struct<field1:string,field2:decimal(38,9),field3:bigint>"    || "struct<field1:string,field2:decimal(38,9),field3:bigint>"
        "struct<field1:string,field2:decimal(38,9 ),field3:bigint>"   || "struct<field1:string,field2:decimal(38,9),field3:bigint>"
        "struct<field1:string,field2:decimal (38,9 ),field3:bigint>"  || "struct<field1:string,field2:decimal(38,9),field3:bigint>"
        "struct<field1:string,field2:decimal (38),field3:bigint>"     || "struct<field1:string,field2:decimal(38),field3:bigint>"
        "struct<field1:string,field2:decimal(38 ),field3:bigint>"     || "struct<field1:string,field2:decimal(38),field3:bigint>"
    }

    @Unroll
    def 'fail to convert "#typeString" to a presto type and back'(String typeString) {
        when:
        converter.toMetacatType(typeString)
        then:
        thrown(Exception)
        where:
        typeString << [
            'list<string>',
            'struct<s: string>'
        ]
    }

}
