package com.netflix.metacat.common.dto;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * ParentInfo dto information.
 */
@SuppressWarnings("unused")
@Data
@EqualsAndHashCode(callSuper = false)
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ParentInfoDto extends BaseDto {
    private static final long serialVersionUID = 8121239864203088788L;
    /* Name of the parent */
    @ApiModelProperty(value = "name of the child")
    private String name;
    /* Type of the relation */
    @ApiModelProperty(value = "type of the relation")
    private String relationType;
    /* uuid of the table */
    @ApiModelProperty(value = "uuid of the table")
    private String uuid;
}
