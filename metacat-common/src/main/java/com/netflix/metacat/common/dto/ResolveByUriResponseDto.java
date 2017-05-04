package com.netflix.metacat.common.dto;

import com.netflix.metacat.common.QualifiedName;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * ResolveByUriResponseDto.
 *
 * @author zhenl
 * @since 1.0.0
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class ResolveByUriResponseDto extends BaseDto {
    private static final long serialVersionUID = -4505346090786555046L;
    private List<QualifiedName> tables;
    private List<QualifiedName> partitions;
}
