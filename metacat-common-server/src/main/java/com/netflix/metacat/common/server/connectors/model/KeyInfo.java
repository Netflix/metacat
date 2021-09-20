package com.netflix.metacat.common.server.connectors.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * Key Info.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class KeyInfo implements Serializable {
    private static final long serialVersionUID = 7254898853779135216L;

    private String name;
    private List<String> fields;
}
