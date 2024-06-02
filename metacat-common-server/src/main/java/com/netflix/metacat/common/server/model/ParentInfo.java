package com.netflix.metacat.common.server.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ParentInfo.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ParentInfo {
    private String name;
    private String relationType;
}
