package com.netflix.metacat.common.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * ChildInfo dto information.
 */
@SuppressWarnings("unused")
@Data
@EqualsAndHashCode(callSuper = false)
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ChildInfoDto extends BaseDto {
    private static final long serialVersionUID = 9121109874202088789L;
    /* Name of the child */
    @Schema(description = "name of the child")
    private String name;
    /* Type of the relation */
    @Schema(description = "type of the relation")
    private String relationType;
    /* uuid of the table */
    @Schema(description = "uuid of the table")
    private String uuid;
}
