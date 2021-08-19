package com.netflix.metacat.common.server.connectors.model;

import com.netflix.metacat.common.dto.BaseDto;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Key Info.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class KeyInfo extends BaseDto {
    private static final long serialVersionUID = 7254898853779135216L;

    private String name;
    private List<String> fields;
}
