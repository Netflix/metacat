package com.netflix.metacat.common.server.connectors.model;

import com.netflix.metacat.common.dto.FieldDto;
import com.netflix.metacat.common.dto.KeyDto;
import com.netflix.metacat.common.dto.KeySetDto;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class KeySetInfo implements Serializable {

    private static final String partitionKey_defaultName = "partition";
    private static final String primaryKey_defaultName = "primary";
    private static final String sortKey_defaultName = "sort";
    private static final String indexKey_defaultName = "index";

    public List<KeyInfo> partition;
    public List<KeyInfo> primary;
    public List<KeyInfo> sort;
    public List<KeyInfo> index;

    public static KeySetInfo buildKeySet(List<FieldInfo> fields) {
        return buildKeySet(fields, null);
    }

    public static KeySetInfo buildKeySet(List<FieldInfo> fields, List<KeyInfo> primary)
    {
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
            if (field.getIsSortKey()) {
                sortKeys.add(field.getName());
            }
            if (field.getIsIndexKey()){
                indexKeys.add(field.getName());
            }
        }

        final KeySetInfo keySetInfo = new KeySetInfo();
        keySetInfo.setDefaultPartitionKeys(partitionKeys);
        keySetInfo.setDefaultSortKeys(sortKeys);
        keySetInfo.setDefaultIndexKeys(indexKeys);
        keySetInfo.primary = primary;

        return keySetInfo;
    }

    private void setDefaultPartitionKeys(List<String> fieldNames) {
        this.partition = Arrays.asList(KeyInfo.builder().name(partitionKey_defaultName).fields(fieldNames).build());
    }

    private void setDefaultSortKeys(List<String> fieldNames) {
        this.sort = Arrays.asList(KeyInfo.builder().name(sortKey_defaultName).fields(fieldNames).build());
    }

    private void setDefaultIndexKeys(List<String> fieldNames) {
        this.index = Arrays.asList(KeyInfo.builder().name(indexKey_defaultName).fields(fieldNames).build());
    }

}
