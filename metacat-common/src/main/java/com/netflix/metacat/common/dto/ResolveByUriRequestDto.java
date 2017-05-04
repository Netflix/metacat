package com.netflix.metacat.common.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * ResolveByUriRequestDto.
 *
 * @author zhenl
 * @since 1.0.0
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class ResolveByUriRequestDto extends BaseDto {
    private static final long serialVersionUID = -2649784382533439526L;
    private String uri;
}
