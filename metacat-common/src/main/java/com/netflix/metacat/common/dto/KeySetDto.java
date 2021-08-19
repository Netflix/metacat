package com.netflix.metacat.common.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KeySetDto extends BaseDto {
    public List<KeyDto> partition;
    public List<KeyDto> primary;
    public List<KeyDto> sort;
    public List<KeyDto> index;
}
