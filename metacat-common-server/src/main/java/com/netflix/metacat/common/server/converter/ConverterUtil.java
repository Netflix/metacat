/*
 *
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.metacat.common.server.converter;

import com.google.common.collect.Maps;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.dto.AuditDto;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.FieldDto;
import com.netflix.metacat.common.dto.GetPartitionsRequestDto;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveRequestDto;
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.dto.StorageDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.model.AuditInfo;
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo;
import com.netflix.metacat.common.server.connectors.model.FieldInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest;
import com.netflix.metacat.common.server.connectors.model.PartitionsSaveRequest;
import com.netflix.metacat.common.server.connectors.model.PartitionsSaveResponse;
import com.netflix.metacat.common.server.connectors.model.StorageInfo;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import org.dozer.CustomConverter;
import org.dozer.DozerBeanMapper;
import org.dozer.Mapper;
import org.dozer.loader.api.BeanMappingBuilder;
import org.dozer.loader.api.FieldsMappingOptions;

import javax.inject.Inject;
import java.util.Map;

/**
 * Mapper from Dto to Connector Info.
 * @author amajumdar
 */
public class ConverterUtil {
    private final Mapper mapper;

    /**
     * Constructor.
     * @param dozerTypeConverter custom dozer converter for types
     */
    @Inject
    public ConverterUtil(final DozerTypeConverter dozerTypeConverter) {
        final DozerBeanMapper dozerBeanMapper = new DozerBeanMapper();
        final BeanMappingBuilder builder = new BeanMappingBuilder() {
            @Override
            protected void configure() {
                mapping(FieldDto.class, FieldInfo.class)
                    .fields("type", "type", FieldsMappingOptions.customConverterId("typeConverter"))
                    .fields("partition_key", "partitionKey", FieldsMappingOptions.copyByReference())
                    .fields("source_type", "sourceType", FieldsMappingOptions.copyByReference());
                mapping(TableDto.class, TableInfo.class)
                    .fields("name", "name", FieldsMappingOptions.copyByReference());
                mapping(DatabaseDto.class, DatabaseInfo.class)
                    .fields("name", "name", FieldsMappingOptions.copyByReference());
                mapping(PartitionDto.class, PartitionInfo.class)
                    .fields("name", "name", FieldsMappingOptions.copyByReference());
                mapping(AuditDto.class, AuditInfo.class);
                mapping(StorageDto.class, StorageInfo.class);
            }
        };
        dozerBeanMapper.addMapping(builder);
        final Map<String, CustomConverter> customConverterMap = Maps.newHashMap();
        customConverterMap.put("typeConverter", dozerTypeConverter);
        dozerBeanMapper.setCustomConvertersWithId(customConverterMap);
        mapper = dozerBeanMapper;
    }

    /**
     * Converts from DatabaseInfo to DatabaseDto.
     * @param databaseInfo connector table info
     * @return database dto
     */
    public DatabaseDto toDatabaseDto(final DatabaseInfo databaseInfo) {
        return mapper.map(databaseInfo, DatabaseDto.class);
    }

    /**
     * Converts from TableDto to TableInfo.
     * @param databaseDto database dto
     * @return connector database info
     */
    public DatabaseInfo fromDatabaseDto(final DatabaseDto databaseDto) {
        return mapper.map(databaseDto, DatabaseInfo.class);
    }

    /**
     * Converts from TableInfo to TableDto.
     * @param tableInfo connector table info
     * @return table dto
     */
    public TableDto toTableDto(final TableInfo tableInfo) {
        return mapper.map(tableInfo, TableDto.class);
    }

    /**
     * Converts from TableDto to TableInfo.
     * @param tableDto table dto
     * @return connector table info
     */
    public TableInfo fromTableDto(final TableDto tableDto) {
        return mapper.map(tableDto, TableInfo.class);
    }

    /**
     * Converts from PartitionInfo to PartitionDto.
     * @param partitionInfo connector partition info
     * @return partition dto
     */
    public PartitionDto toPartitionDto(final PartitionInfo partitionInfo) {
        return mapper.map(partitionInfo, PartitionDto.class);
    }

    /**
     * Converts from PartitionDto to PartitionInfo.
     * @param partitionDto partition dto
     * @return connector partition info
     */
    public PartitionInfo fromPartitionDto(final PartitionDto partitionDto) {
        return mapper.map(partitionDto, PartitionInfo.class);
    }

    /**
     * Creates the connector context.
     * @param metacatRequestContext request context
     * @return connector context
     */
    public ConnectorContext toConnectorContext(final MetacatRequestContext metacatRequestContext) {
        return mapper.map(metacatRequestContext, ConnectorContext.class);
    }

    /**
     * Creates the partition list connector request.
     * @param partitionsRequestDto request containing the filter and other properties used for listing
     * @param pageable pageable info
     * @param sort sort info
     * @return connector request
     */
    public PartitionListRequest toPartitionListRequest(final GetPartitionsRequestDto partitionsRequestDto,
        final Pageable pageable, final Sort sort) {
        final PartitionListRequest result = mapper.map(partitionsRequestDto, PartitionListRequest.class);
        result.setPageable(pageable);
        result.setSort(sort);
        return result;
    }

    /**
     * Creates the partition list connector request.
     * @param partitionsRequestDto request containing the save request information
     * @return connector request
     */
    public PartitionsSaveRequest toPartitionsSaveRequest(final PartitionsSaveRequestDto partitionsRequestDto) {
        return mapper.map(partitionsRequestDto, PartitionsSaveRequest.class);
    }

    /**
     * Creates the partition list connector request.
     * @param partitionsSaveResponse response on saving partitions
     * @return response dto
     */
    public PartitionsSaveResponseDto toPartitionsSaveResponseDto(final PartitionsSaveResponse partitionsSaveResponse) {
        return mapper.map(partitionsSaveResponse, PartitionsSaveResponseDto.class);
    }


}
