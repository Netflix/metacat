package com.netflix.metacat.common.server.properties;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;
import javax.annotation.Nullable;

/**
 * Partition Column service properties.
 *
 * @author gabeiglio
 */
@Data
@Slf4j
public class PartitionColumnProperties {
    private static final String OMIT_VOID_TRANSFORM_PARTITION_FIELDS_PROPERTY_NAME =
        "metacat.partitionColumnProperties.omitVoidTransformPartitionFields";
    private boolean omitVoidPartitionEnabled;

    /**
     * Constructor.
     *
     * @param env Spring environment
     */
    public PartitionColumnProperties(@Nullable final Environment env) {
        if (env != null) {
            setOmitVoidTransformPartitionFields(
                env.getProperty(OMIT_VOID_TRANSFORM_PARTITION_FIELDS_PROPERTY_NAME,
                    Boolean.class, true)
            );
        }
    }

    /**
     * set omitVoidPartitionEnabled based on boolean config.
     *
     * @param  configBool configBool
     */
    public void setOmitVoidTransformPartitionFields(final boolean configBool) {
        this.omitVoidPartitionEnabled = configBool;
    }

}
