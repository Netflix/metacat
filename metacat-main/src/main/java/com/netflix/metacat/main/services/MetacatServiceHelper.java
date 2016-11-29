/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.main.services;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.BaseDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveRequestDto;
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionPreEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateDatabasePostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateDatabasePreEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePreEvent;

import java.util.List;

/**
 * @author amajumdar
 */
public class MetacatServiceHelper {
    private final DatabaseService databaseService;
    private final TableService tableService;
    private final PartitionService partitionService;
    private final MetacatEventBus eventBus;

    @Inject
    public MetacatServiceHelper(
            DatabaseService databaseService,
            TableService tableService,
            PartitionService partitionService,
            MetacatEventBus eventBus
    ) {
        this.databaseService = databaseService;
        this.tableService = tableService;
        this.partitionService = partitionService;
        this.eventBus = eventBus;
    }

    public MetacatService getService(QualifiedName name) {
        MetacatService result;
        if (name.isPartitionDefinition()) {
            result = partitionService;
        } else if (name.isTableDefinition()) {
            result = tableService;
        } else if (name.isDatabaseDefinition()) {
            result = databaseService;
        } else {
            throw new IllegalArgumentException(String.format("Invalid name %s", name));
        }
        return result;
    }

    public void postPreUpdateEvent(QualifiedName name, MetacatRequestContext metacatRequestContext, BaseDto dto) {
        if (name.isPartitionDefinition()) {
            PartitionsSaveRequestDto partitionsSaveRequestDto = new PartitionsSaveRequestDto();
            if (dto != null) {
                partitionsSaveRequestDto.setPartitions(ImmutableList.of((PartitionDto) dto));
            }
            eventBus.postSync(new MetacatSaveTablePartitionPreEvent(name, metacatRequestContext, partitionsSaveRequestDto));
        } else if (name.isTableDefinition()) {
            eventBus.postSync(new MetacatUpdateTablePreEvent(name, metacatRequestContext, (TableDto) dto, (TableDto) dto));
        } else if (name.isDatabaseDefinition()) {
            eventBus.postSync(new MetacatUpdateDatabasePreEvent(name, metacatRequestContext));
        } else {
            throw new IllegalArgumentException(String.format("Invalid name %s", name));
        }
    }

    public void postPostUpdateEvent(
            final QualifiedName name,
            final MetacatRequestContext metacatRequestContext,
            final BaseDto oldDTo,
            final BaseDto currentDto
    ) {
        if (name.isPartitionDefinition()) {
            List<PartitionDto> dtos = Lists.newArrayList();
            if (currentDto != null) {
                dtos.add((PartitionDto) currentDto);
            }
            // This request neither added nor updated partitions
            PartitionsSaveResponseDto partitionsSaveResponseDto = new PartitionsSaveResponseDto();
            eventBus.postAsync(
                    new MetacatSaveTablePartitionPostEvent(
                            name,
                            metacatRequestContext,
                            dtos,
                            partitionsSaveResponseDto
                    )
            );
        } else if (name.isTableDefinition()) {
            MetacatUpdateTablePostEvent event = new MetacatUpdateTablePostEvent(
                    name,
                    metacatRequestContext,
                    (TableDto) oldDTo,
                    (TableDto) currentDto
            );
            eventBus.postAsync(event);
        } else if (name.isDatabaseDefinition()) {
            eventBus.postAsync(new MetacatUpdateDatabasePostEvent(name, metacatRequestContext));
        } else {
            throw new IllegalArgumentException(String.format("Invalid name %s", name));
        }
    }
}
