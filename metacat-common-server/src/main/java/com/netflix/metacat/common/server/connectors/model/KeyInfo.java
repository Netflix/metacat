package com.netflix.metacat.common.server.connectors.model;

import com.netflix.metacat.common.dto.BaseDto;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KeyInfo extends BaseDto {
    public String name;
    public List<String> fields;
}
