package com.netflix.metacat.common.server.properties;

import lombok.Data;

/**
 * Parent Child Relationship service properties.
 *
 * @author yingjianw
 */
@Data
public class ParentChildRelationshipProperties {
    private boolean parentChildCreateEnabled = true;
    private boolean parentChildGetEnabled = true;
    private boolean parentChildRenameEnabled = true;
    private boolean parentChildDropEnabled = true;
}
