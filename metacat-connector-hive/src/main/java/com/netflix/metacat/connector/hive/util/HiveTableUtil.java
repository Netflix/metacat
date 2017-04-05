package com.netflix.metacat.connector.hive.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * HiveTableUtil.
 *
 * @author zhenl
 * @since 1.0.0
 */
@SuppressWarnings("deprecation")
public final class HiveTableUtil {
    private HiveTableUtil() {

    }

    /**
     * getTableStructFields.
     *
     * @param table table
     * @return all struct field refs
     */
    public static List<? extends StructField> getTableStructFields(final Table table) {
        final Properties schema = MetaStoreUtils.getTableMetadata(table);
        final String name = schema.getProperty(serdeConstants.SERIALIZATION_LIB);
        if (name == null) {
            return Collections.emptyList();
        }
        final Deserializer deserializer = createDeserializer(getDeserializerClass(name));

        try {
            deserializer.initialize(new Configuration(false), schema);
        } catch (SerDeException e) {
            throw new RuntimeException("error initializing deserializer: " + deserializer.getClass().getName());
        }
        try {
            final ObjectInspector inspector = deserializer.getObjectInspector();
            Preconditions.checkArgument(inspector.getCategory() == ObjectInspector.Category.STRUCT,
                    "expected STRUCT: %s", inspector.getCategory());
            return ((StructObjectInspector) inspector).getAllStructFieldRefs();
        } catch (SerDeException e) {
            throw Throwables.propagate(e);
        }
    }

    private static Class<? extends Deserializer> getDeserializerClass(final String name) {
        // CDH uses different names for Parquet
        if ("parquet.hive.serde.ParquetHiveSerDe".equals(name)) {
            return ParquetHiveSerDe.class;
        }

        try {
            return Class.forName(name, true, JavaUtils.getClassLoader()).asSubclass(Deserializer.class);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("deserializer does not exist: " + name);
        } catch (ClassCastException e) {
            throw new RuntimeException("invalid deserializer class: " + name);
        }
    }

    private static Deserializer createDeserializer(final Class<? extends Deserializer> clazz) {
        try {
            return clazz.getConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("error creating deserializer: " + clazz.getName(), e);
        }
    }

}
