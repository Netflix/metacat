package com.netflix.metacat.common.server.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ChildInfo.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ChildInfo {
    private String name;
    private String relationType;
}
