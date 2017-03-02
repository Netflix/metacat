package com.netflix.metacat.connector.hive;

import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

import javax.annotation.Nonnull;

/**
 * Hive storage format.
 *
 * @author zhenl
 */
public enum HiveStorageFormat {
    /**
     * Optimized Row Columnar.
     */
    ORC(OrcSerde.class.getName(),
            OrcInputFormat.class.getName(),
            OrcOutputFormat.class.getName()),
    /**
     * PARQUET.
     */
    PARQUET(ParquetHiveSerDe.class.getName(),
            MapredParquetInputFormat.class.getName(),
            MapredParquetOutputFormat.class.getName()),

    /**
     * RCBINARY.
     */
    RCBINARY(LazyBinaryColumnarSerDe.class.getName(),
            RCFileInputFormat.class.getName(),
            RCFileOutputFormat.class.getName()),

    /**
     * RCTEXT.
     */
    RCTEXT(ColumnarSerDe.class.getName(),
            RCFileInputFormat.class.getName(),
            RCFileOutputFormat.class.getName()),

    /**
     * SEQUENCEFILE.
     */
    SEQUENCEFILE(LazySimpleSerDe.class.getName(),
            SequenceFileInputFormat.class.getName(),
            HiveSequenceFileOutputFormat.class.getName()),

    /**
     * TEXTFILE.
     */
    TEXTFILE(LazySimpleSerDe.class.getName(),
            TextInputFormat.class.getName(),
            HiveIgnoreKeyTextOutputFormat.class.getName());

    private final String serde;
    private final String inputFormat;
    private final String outputFormat;

    HiveStorageFormat(@Nonnull final String serde,
                      @Nonnull final String inputFormat,
                      @Nonnull final String outputFormat) {
        this.serde = serde;
        this.inputFormat = inputFormat;
        this.outputFormat = outputFormat;
    }

    public String getSerDe() {
        return serde;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public String getOutputFormat() {
        return outputFormat;
    }
}
