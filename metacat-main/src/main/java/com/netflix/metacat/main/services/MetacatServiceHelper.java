/*
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
 */
package com.netflix.metacat.main.services;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.BaseDto;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveRequestDto;
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.server.events.MetacatDeleteDatabasePreEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteMViewPostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteMViewPreEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePartitionPreEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePreEvent;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionPreEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateDatabasePostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateDatabasePreEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateMViewPostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateMViewPreEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePreEvent;

import java.util.List;

/**
 * Generic Service helper.
 *
 * @author amajumdar
 */
public class MetacatServiceHelper {
    /**
     * Defines the table type.
     */
    public static final String PARAM_TABLE_TYPE = "table_type";
    /**
     * Iceberg table type.
     */
    public static final String ICEBERG_TABLE_TYPE = "ICEBERG";
    private final DatabaseService databaseService;
    private final TableService tableService;
    private final PartitionService partitionService;
    private final MViewService mViewService;
    private final MetacatEventBus eventBus;

    /**
     * Constructor.
     *
     * @param databaseService  database service
     * @param tableService     table service
     * @param partitionService partition service
     * @param mViewService     mview service
     * @param eventBus         event bus
     */
    public MetacatServiceHelper(
        final DatabaseService databaseService,
        final TableService tableService,
        final PartitionService partitionService,
        final MViewService mViewService,
        final MetacatEventBus eventBus
    ) {
        this.databaseService = databaseService;
        this.tableService = tableService;
        this.partitionService = partitionService;
        this.mViewService = mViewService;
        this.eventBus = eventBus;
    }

    /**
     * Get the relevant service for the given qualified name.
     *
     * @param name name
     * @return service
     */
    public MetacatService getService(final QualifiedName name) {
        final MetacatService result;
        if (name.isPartitionDefinition()) {
            result = partitionService;
        } else if (name.isViewDefinition()) {
            result = mViewService;
        } else if (name.isTableDefinition()) {
            result = tableService;
        } else if (name.isDatabaseDefinition()) {
            result = databaseService;
        } else {
            throw new IllegalArgumentException(String.format("Invalid name %s", name));
        }
        return result;
    }

    /**
     * Calls the right method of the event bus for the given qualified name.
     *
     * @param name                  name
     * @param metacatRequestContext context
     * @param dto                   dto
     */
    public void postPreUpdateEvent(
        final QualifiedName name,
        final MetacatRequestContext metacatRequestContext,
        final BaseDto dto
    ) {
        if (name.isPartitionDefinition()) {
            final PartitionsSaveRequestDto partitionsSaveRequestDto = new PartitionsSaveRequestDto();
            if (dto != null) {
                partitionsSaveRequestDto.setPartitions(ImmutableList.of((PartitionDto) dto));
            }
            this.eventBus.post(
                new MetacatSaveTablePartitionPreEvent(name, metacatRequestContext, this, partitionsSaveRequestDto)
            );
        } else if (name.isViewDefinition()) {
            this.eventBus.post(
                new MetacatUpdateMViewPreEvent(name, metacatRequestContext, this, (TableDto) dto)
            );
        } else if (name.isTableDefinition()) {
            this.eventBus.post(
                new MetacatUpdateTablePreEvent(name, metacatRequestContext, this, (TableDto) dto, (TableDto) dto)
            );
        } else if (name.isDatabaseDefinition()) {
            eventBus.post(new MetacatUpdateDatabasePreEvent(name, metacatRequestContext, this));
        } else {
            throw new IllegalArgumentException(String.format("Invalid name %s", name));
        }
    }

    /**
     * Calls the right method of the event bus for the given qualified name.
     *
     * @param name                  name
     * @param metacatRequestContext context
     * @param oldDTo                dto
     * @param currentDto            dto
     */
    public void postPostUpdateEvent(
        final QualifiedName name,
        final MetacatRequestContext metacatRequestContext,
        final BaseDto oldDTo,
        final BaseDto currentDto
    ) {
        if (name.isPartitionDefinition()) {
            final List<PartitionDto> dtos = Lists.newArrayList();
            if (currentDto != null) {
                dtos.add((PartitionDto) currentDto);
            }
            // This request neither added nor updated partitions
            final PartitionsSaveResponseDto partitionsSaveResponseDto = new PartitionsSaveResponseDto();
            this.eventBus.post(
                new MetacatSaveTablePartitionPostEvent(
                    name,
                    metacatRequestContext,
                    this,
                    dtos,
                    partitionsSaveResponseDto
                )
            );
        } else if (name.isViewDefinition()) {
            final MetacatUpdateMViewPostEvent event = new MetacatUpdateMViewPostEvent(
                name,
                metacatRequestContext,
                this,
                (TableDto) currentDto
            );
            this.eventBus.post(event);
        } else if (name.isTableDefinition()) {
            final MetacatUpdateTablePostEvent event = new MetacatUpdateTablePostEvent(
                name,
                metacatRequestContext,
                this,
                (TableDto) oldDTo,
                (TableDto) currentDto
            );
            this.eventBus.post(event);
        } else if (name.isDatabaseDefinition()) {
            this.eventBus.post(new MetacatUpdateDatabasePostEvent(name, metacatRequestContext, this));
        } else {
            throw new IllegalArgumentException(String.format("Invalid name %s", name));
        }
    }



    /**
     * Calls the right method of the event bus for the given qualified name.
     *
     * @param name                  name
     * @param metacatRequestContext context
     */
    public void postPreDeleteEvent(
        final QualifiedName name,
        final MetacatRequestContext metacatRequestContext
    ) {
        if (name.isPartitionDefinition()) {
            final PartitionsSaveRequestDto partitionsSaveRequestDto = new PartitionsSaveRequestDto();
            partitionsSaveRequestDto.setPartitionIdsForDeletes(Lists.newArrayList(name.getPartitionName()));
            this.eventBus.post(
                new MetacatDeleteTablePartitionPreEvent(name, metacatRequestContext, this, partitionsSaveRequestDto)
            );
        } else if (name.isViewDefinition()) {
            this.eventBus.post(
                new MetacatDeleteMViewPreEvent(name, metacatRequestContext, this)
            );
        } else if (name.isTableDefinition()) {
            this.eventBus.post(new MetacatDeleteTablePreEvent(name, metacatRequestContext, this));
        } else if (name.isDatabaseDefinition()) {
            final DatabaseDto dto = new DatabaseDto();
            dto.setName(name);
            eventBus.post(new MetacatDeleteDatabasePreEvent(name, metacatRequestContext, this, dto));
        } else {
            throw new IllegalArgumentException(String.format("Invalid name %s", name));
        }
    }

    /**
     * Calls the right method of the event bus for the given qualified name.
     *
     * @param name                  name
     * @param metacatRequestContext context
     */
    public void postPostDeleteEvent(
        final QualifiedName name,
        final MetacatRequestContext metacatRequestContext
    ) {
        if (name.isPartitionDefinition()) {
            this.eventBus.post(
                new MetacatDeleteTablePartitionPostEvent(
                    name,
                    metacatRequestContext,
                    this,
                    Lists.newArrayList(PartitionDto.builder().name(name).build())
                )
            );
        } else if (name.isViewDefinition()) {
            final TableDto dto = new TableDto();
            dto.setName(name);
            this.eventBus.post(new MetacatDeleteMViewPostEvent(name, metacatRequestContext, this, dto));
        } else if (name.isTableDefinition()) {
            final TableDto dto = new TableDto();
            dto.setName(name);
            this.eventBus.post(new MetacatDeleteTablePostEvent(name, metacatRequestContext, this, dto,
                false));
        } else if (name.isDatabaseDefinition()) {
            this.eventBus.post(new MetacatUpdateDatabasePostEvent(name, metacatRequestContext, this));
        } else {
            throw new IllegalArgumentException(String.format("Invalid name %s", name));
        }
    }

    /**
     * check if the table is an Iceberg Table.
     *
     * @param tableDto table dto
     * @return true for iceberg table
     */
    public static boolean isIcebergTable(final TableDto tableDto) {
        return tableDto.getMetadata() != null
            && tableDto.getMetadata().containsKey(PARAM_TABLE_TYPE)
            && ICEBERG_TABLE_TYPE
            .equalsIgnoreCase(tableDto.getMetadata().get(PARAM_TABLE_TYPE));
    }
}
