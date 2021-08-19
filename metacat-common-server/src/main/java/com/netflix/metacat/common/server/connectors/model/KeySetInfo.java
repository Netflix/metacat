package com.netflix.metacat.common.server.connectors.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * KeySet Info.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class KeySetInfo implements Serializable {
    private static final long serialVersionUID = 3659843901964058788L;

    private static final String PARTITION_KEY_DEFAULT_NAME = "partition";
    private static final String PRIMARY_KEY_DEFAULT_NAME = "primary";
    private static final String SORT_KEY_DEFAULT_NAME = "sort";
    private static final String INDEX_KEY_DEFAULT_NAME = "index";

    private List<KeyInfo> partition;
    private List<KeyInfo> primary;
    private List<KeyInfo> sort;
    private List<KeyInfo> index;

    /**
     * builds a keyset from fieldInfo list.
     *
     * @param fields list of fieldInfo
     * @return keyset
     */
    public static KeySetInfo buildKeySet(final List<FieldInfo> fields) {
        return buildKeySet(fields, null);
    }

    /**
     * builds a keyset from fieldInfo list and primary key list.
     *
     * @param fields list of fieldInfo
     * @param primary list of primary keys
     * @return keyset
     */
    public static KeySetInfo buildKeySet(final List<FieldInfo> fields, final List<KeyInfo> primary) {
        if (fields == null) {
            return null;
        } else if (fields.isEmpty()) {
            return new KeySetInfo();
        }

        final List<String> partitionKeys = new LinkedList<>();
        final List<String> sortKeys = new LinkedList<>();
        final List<String> indexKeys = new LinkedList<>();
        for (FieldInfo field : fields) {
            if (field.isPartitionKey()) {
                partitionKeys.add(field.getName());
            }
            if (field.getIsSortKey() != null && field.getIsSortKey()) {
                sortKeys.add(field.getName());
            }
            if (field.getIsIndexKey() != null && field.getIsIndexKey()) {
                indexKeys.add(field.getName());
            }
        }

        final KeySetInfo keySetInfo = new KeySetInfo();
        keySetInfo.partition = partitionKeys.isEmpty() ? Collections.emptyList()
            : Arrays.asList(
                KeyInfo.builder().name(PARTITION_KEY_DEFAULT_NAME).fields(partitionKeys).build());
        keySetInfo.sort = sortKeys.isEmpty() ? Collections.emptyList()
            : Arrays.asList(KeyInfo.builder().name(SORT_KEY_DEFAULT_NAME).fields(sortKeys).build());
        keySetInfo.index = indexKeys.isEmpty() ? Collections.emptyList()
            : Arrays.asList(KeyInfo.builder().name(INDEX_KEY_DEFAULT_NAME).fields(indexKeys).build());
        keySetInfo.primary = (primary == null || primary.isEmpty()) ? Collections.emptyList()
            : primary;

        return keySetInfo;
    }
}
