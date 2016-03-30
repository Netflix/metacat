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

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.metacat.common.MetacatContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.BaseDto;
import com.netflix.metacat.common.dto.PartitionDto;
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
 * Created by amajumdar on 3/29/16.
 */
public class MetacatServiceHelper {
    private final DatabaseService databaseService;
    private final TableService tableService;
    private final PartitionService partitionService;
    private final MetacatEventBus eventBus;

    @Inject
    public MetacatServiceHelper(DatabaseService databaseService,
            TableService tableService, PartitionService partitionService,
            MetacatEventBus eventBus) {
        this.databaseService = databaseService;
        this.tableService = tableService;
        this.partitionService = partitionService;
        this.eventBus = eventBus;
    }

    public MetacatService getService(QualifiedName name){
        MetacatService result = null;
        if( name.isPartitionDefinition()){
            result = partitionService;
        } else if( name.isTableDefinition()){
            result = tableService;
        } else if( name.isDatabaseDefinition()){
            result = databaseService;
        }  else {
            throw new IllegalArgumentException(String.format("Invalid name %s", name));
        }
        return result;
    }

    public void postPreUpdateEvent(QualifiedName name, BaseDto dto, MetacatContext metacatContext) {
        if( name.isPartitionDefinition()){
            List<PartitionDto> dtos = Lists.newArrayList();
            if( dto != null) {
                dtos.add((PartitionDto) dto);
            }
            eventBus.post(new MetacatSaveTablePartitionPreEvent(name, dtos, metacatContext));
        } else if( name.isTableDefinition()){
            eventBus.post(new MetacatUpdateTablePreEvent(name, (TableDto) dto, metacatContext));
        } else if( name.isDatabaseDefinition()){
            eventBus.post(new MetacatUpdateDatabasePreEvent(name, metacatContext));
        }  else {
            throw new IllegalArgumentException(String.format("Invalid name %s", name));
        }
    }

    public void postPostUpdateEvent(QualifiedName name, BaseDto dto, MetacatContext metacatContext) {
        if( name.isPartitionDefinition()){
            List<PartitionDto> dtos = Lists.newArrayList();
            if( dto != null) {
                dtos.add((PartitionDto) dto);
            }
            eventBus.post(new MetacatSaveTablePartitionPostEvent(name, dtos, metacatContext));
        } else if( name.isTableDefinition()){
            MetacatUpdateTablePostEvent event = new MetacatUpdateTablePostEvent(name, metacatContext);
            if( dto != null){
                event = new MetacatUpdateTablePostEvent((TableDto)dto, metacatContext);
            }
            eventBus.post( event);
        } else if( name.isDatabaseDefinition()){
            eventBus.post(new MetacatUpdateDatabasePostEvent(name, metacatContext));
        }  else {
            throw new IllegalArgumentException(String.format("Invalid name %s", name));
        }
    }
}
