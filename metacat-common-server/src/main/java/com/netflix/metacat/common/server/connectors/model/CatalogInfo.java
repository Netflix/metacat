package com.netflix.metacat.common.server.connectors.model;

import com.netflix.metacat.common.QualifiedName;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Connector catalog information.
 *
 * @author rveeramacheneni
 * @since 1.3.0
 */
@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public final class CatalogInfo extends BaseInfo {
    private ClusterInfo clusterInfo;

    /**
     * Constructor.
     * @param name qualified name of the catalog
     * @param auditInfo audit info
     * @param metadata metadata properties
     * @param clusterInfo cluster information
     */
    @Builder
    private CatalogInfo(
        final QualifiedName name,
        final AuditInfo auditInfo,
        final Map<String, String> metadata,
        final ClusterInfo clusterInfo
    ) {
        super(name, auditInfo, metadata);
        this.clusterInfo = clusterInfo;
    }
}
