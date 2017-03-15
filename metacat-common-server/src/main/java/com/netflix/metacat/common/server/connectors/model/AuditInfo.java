package com.netflix.metacat.common.server.connectors.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * Audit information.
 * @since 1.0.0
 */
@SuppressWarnings("unused")
@Data
@EqualsAndHashCode(callSuper = false)
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class AuditInfo {
    /* Created By */
    private String createdBy;
    /* Created date */
    private Date createdDate;
    /* Last modified by */
    private String lastModifiedBy;
    /* Last modified date */
    private Date lastModifiedDate;
}
