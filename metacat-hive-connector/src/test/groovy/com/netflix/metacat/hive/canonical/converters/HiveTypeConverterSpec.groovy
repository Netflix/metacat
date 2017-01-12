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

package com.netflix.metacat.hive.canonical.converters;

import com.netflix.metacat.canonical.type.converters.CanonicalHiveTypeConverter;
import com.netflix.metacat.canonical.types.TypeManager;
import com.netflix.metacat.canonical.types.TypeRegistry;
import spock.lang.Shared;
import spock.lang.Specification;
import spock.lang.Unroll;

/**
 * Unit test for hive and canonical type converter.
 */
class HiveTypeConverterSpec extends Specification {
    @Shared
    CanonicalHiveTypeConverter converter = new CanonicalHiveTypeConverter()
    @Shared
    TypeManager typeManager = new TypeRegistry()

    @Unroll
    def 'can convert "#typeString" to a presto type and back'(String typeString) {
        expect:
        def canonicalType = converter.dataTypeToCanonicalType(typeString, typeManager)
        def hiveType = converter.canonicalTypeToDataType( canonicalType)
        def canonicalTypeFromHiveType = converter.dataTypeToCanonicalType(hiveType, typeManager)
        canonicalTypeFromHiveType ==  canonicalType
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
            'struct<field1:string,field2:string,field3:string>'
        ]
    }

}
