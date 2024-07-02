package com.netflix.metacat.common.dto.notifications;

import com.netflix.metacat.common.dto.BaseDto;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * ChildInfo information.
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
    @ApiModelProperty(value = "name of the child")
    private String name;
    /* Type of the relation */
    @ApiModelProperty(value = "type of the relation")
    private String relationType;
    /* uuid of the table */
    @ApiModelProperty(value = "uuid of the table")
    private String uuid;
}
