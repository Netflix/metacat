package com.netflix.metacat.converters

import com.facebook.presto.spi.type.TypeManager
import com.facebook.presto.type.TypeRegistry
import com.netflix.metacat.converters.impl.HiveTypeConverter
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class HiveTypeConverterSpec extends Specification {
    @Shared
    HiveTypeConverter converter = new HiveTypeConverter()
    @Shared
    TypeManager typeManager = new TypeRegistry()

    @Unroll
    def 'can convert "#typeString" to a presto type and back'(String typeString) {
        expect:
        def prestoType = converter.toType(typeString, typeManager)
        def hiveType = converter.fromType(prestoType)
        prestoType == converter.toType(hiveType, typeManager)
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
                'struct<field1:string,field2:string,field3:string>',
        ]
    }
}
