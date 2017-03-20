package com.netflix.metacat.connector.hive.util;

import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * PartitionDetail.
 * @author zhenl
 * @since 1.0.0
 */
@Data
@AllArgsConstructor
public class PartitionDetail {
    private Long id;
    private Long sdId;
    private Long serdeId;
    private PartitionInfo partitionInfo;
}
