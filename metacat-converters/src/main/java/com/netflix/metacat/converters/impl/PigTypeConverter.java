package com.netflix.metacat.converters.impl;

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.FloatType;
import com.facebook.presto.type.IntType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.facebook.presto.type.UnknownType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.metacat.converters.TypeConverter;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.newplan.logical.Util;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

import static java.lang.String.format;

/**
 * Created by amajumdar on 4/27/15.
 */
public class PigTypeConverter implements TypeConverter{
    private static final Logger log = LoggerFactory.getLogger(PigTypeConverter.class);
    private static final String NAME_ARRAY_ELEMENT = "array_element";
    public Type toType(String pigType, TypeManager typeManager){
        try {
            LogicalSchema schema = Utils.parseSchema(pigType);
            LogicalSchema.LogicalFieldSchema field = schema.getField(0);
            return toPrestoType( field);
        } catch (Exception e) {
            log.warn("Pig Parsing failed for signature {}", pigType, e);
            throw new IllegalArgumentException(format("Bad type signature: '%s'", pigType));
        }
    }

    public String fromType(Type prestoType){
        Schema schema = new Schema(Util.translateFieldSchema(fromPrestoTypeToPigSchema(null, prestoType)));
        StringBuilder result = new StringBuilder();
        try {
            Schema.stringifySchema( result, schema, DataType.GENERIC_WRITABLECOMPARABLE, Integer.MIN_VALUE);
        } catch (FrontendException e) {
            throw new IllegalArgumentException(format("Bad presto type: '%s'", prestoType));
        }
        return result.toString();
    }

    private LogicalSchema.LogicalFieldSchema fromPrestoTypeToPigSchema(String alias, Type prestoType){
        if( VarbinaryType.VARBINARY.equals(prestoType)){
            return new LogicalSchema.LogicalFieldSchema(alias, null, DataType.BYTEARRAY);
        } else if ( BooleanType.BOOLEAN.equals( prestoType)) {
            return new LogicalSchema.LogicalFieldSchema(alias, null, DataType.BOOLEAN);
        } else if( IntType.INT.equals(prestoType)){
            return new LogicalSchema.LogicalFieldSchema(alias, null, DataType.INTEGER);
        } else if( BigintType.BIGINT.equals(prestoType)){
            return new LogicalSchema.LogicalFieldSchema(alias, null, DataType.LONG);
        } else if( FloatType.FLOAT.equals(prestoType)){
            return new LogicalSchema.LogicalFieldSchema(alias, null, DataType.FLOAT);
        } else if( DoubleType.DOUBLE.equals(prestoType)){
            return new LogicalSchema.LogicalFieldSchema(alias, null, DataType.DOUBLE);
        } else if( TimestampType.TIMESTAMP.equals(prestoType) || DateType.DATE.equals(prestoType)){
            return new LogicalSchema.LogicalFieldSchema(alias, null, DataType.DATETIME);
        } else if( VarcharType.VARCHAR.equals(prestoType)){
            return new LogicalSchema.LogicalFieldSchema(alias, null, DataType.CHARARRAY);
        } else if( UnknownType.UNKNOWN.equals(prestoType)){
            return new LogicalSchema.LogicalFieldSchema(alias, null, DataType.UNKNOWN);
        } else if( prestoType instanceof MapType){
            MapType mapType = (MapType) prestoType;
            LogicalSchema schema = null;
            //
            // map<unknown type> in Presto is map[] in PIG
            //
            if( mapType.getValueType() != null && !UnknownType.UNKNOWN.equals(mapType.getValueType())){
                schema = new LogicalSchema();
                schema.addField(fromPrestoTypeToPigSchema(null, mapType.getValueType()));
            }
            return new LogicalSchema.LogicalFieldSchema(alias, schema, DataType.MAP);
        } else if( prestoType instanceof ArrayType){
            ArrayType arrayType = (ArrayType) prestoType;
            LogicalSchema schema = new LogicalSchema();
            Type elementType = arrayType.getElementType();
            if( elementType != null){
                if( !(elementType instanceof RowType)){
                    elementType = new RowType(Lists.newArrayList(elementType), Optional.of(ImmutableList.of(NAME_ARRAY_ELEMENT)));
                }
                schema.addField(fromPrestoTypeToPigSchema(null, elementType));
            }
            return new LogicalSchema.LogicalFieldSchema(alias, schema, DataType.BAG);

        } else if( prestoType instanceof RowType){
            RowType rowType = (RowType) prestoType;
            LogicalSchema schema = new LogicalSchema();
            rowType.getFields().stream().forEach(
                    rowField -> schema.addField(fromPrestoTypeToPigSchema(rowField.getName().isPresent()?rowField.getName().get():null, rowField.getType())));
            return new LogicalSchema.LogicalFieldSchema(alias, schema, DataType.TUPLE);
        }
        throw new IllegalArgumentException(format("Bad presto type: '%s'", prestoType));
    }

    private Type toPrestoType(LogicalSchema.LogicalFieldSchema field){
        switch(field.type){
        case DataType.BOOLEAN:
            return BooleanType.BOOLEAN;
        case DataType.BYTE:
        case DataType.BYTEARRAY:
            return VarbinaryType.VARBINARY;
        case DataType.INTEGER:
            return IntType.INT;
        case DataType.LONG:
        case DataType.BIGINTEGER:
            return BigintType.BIGINT;
        case DataType.FLOAT:
            return FloatType.FLOAT;
        case DataType.DOUBLE:
        case DataType.BIGDECIMAL:
            return DoubleType.DOUBLE;
        case DataType.DATETIME:
            return TimestampType.TIMESTAMP;
        case DataType.CHARARRAY:
        case DataType.BIGCHARARRAY:
            return VarcharType.VARCHAR;
        case DataType.MAP:
            return toPrestoMapType(field);
        case DataType.BAG:
            return toPrestoArrayType(field);
        case DataType.TUPLE:
            return toPrestoRowType( field);
        case DataType.UNKNOWN:
            return UnknownType.UNKNOWN;
        }
        throw new IllegalArgumentException(format("Bad type signature: '%s'", field.toString()));
    }

    private Type toPrestoRowType(LogicalSchema.LogicalFieldSchema field) {
        List<Type> fieldTypes = Lists.newArrayList();
        List<String> fieldNames = Lists.newArrayList();
        field.schema.getFields().stream().forEach(logicalFieldSchema -> {
            fieldTypes.add(toPrestoType( logicalFieldSchema));
            fieldNames.add(logicalFieldSchema.alias);
        });
        return new RowType(fieldTypes, Optional.of(fieldNames));
    }

    private Type toPrestoArrayType(LogicalSchema.LogicalFieldSchema field) {
        LogicalSchema.LogicalFieldSchema subField = field.schema.getField(0);
        Type elementType = null;
        if( subField.type == DataType.TUPLE
                && subField.schema.getFields() != null
                && !subField.schema.getFields().isEmpty()
                && NAME_ARRAY_ELEMENT.equals(subField.schema.getFields().get(0).alias)){
            elementType = toPrestoType(subField.schema.getFields().get(0));
        } else {
            elementType = toPrestoType(subField);
        }
        return new ArrayType(elementType);
    }

    private Type toPrestoMapType(LogicalSchema.LogicalFieldSchema field) {
        Type key = VarcharType.VARCHAR;
        Type value = UnknownType.UNKNOWN;
        if( field.schema != null){
            List<LogicalSchema.LogicalFieldSchema> fields = field.schema.getFields();
            if(fields.size() > 0){
                value = toPrestoType(fields.get(0));
            }
        }
        return new MapType(key, value);
    }
}
