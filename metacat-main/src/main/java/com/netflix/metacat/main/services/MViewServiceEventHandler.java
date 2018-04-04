
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

import com.netflix.metacat.common.NameDateDto;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.events.AsyncListener;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatRenameTablePostEvent;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Event handler for view changes based on table changes.
 *
 * @author amajumdar
 */
@Slf4j
@Component
@AsyncListener
public class MViewServiceEventHandler {
    private final Config config;
    private final MViewService mViewService;
    private final UserMetadataService userMetadataService;

    /**
     * Default constructor.
     * @param config               server configurations
     * @param mViewService         view service
     * @param userMetadataService  user metadata service
     */
    @Autowired
    public MViewServiceEventHandler(final Config config,
                                    final MViewService mViewService,
                                    final UserMetadataService userMetadataService) {
        this.config = config;
        this.mViewService = mViewService;
        this.userMetadataService = userMetadataService;
    }

    /**
     * Subscriber.
     *
     * @param event event
     */
    @EventListener
    public void metacatDeleteTablePostEventHandler(final MetacatDeleteTablePostEvent event) {
        if (config.canCascadeViewsMetadataOnTableDelete() && !event.isMView()) {
            final QualifiedName name = event.getTable().getName();
            try {
                // delete views associated with this table
                final List<NameDateDto> viewNames = mViewService.list(name);
                viewNames.forEach(viewName -> mViewService.deleteAndReturn(viewName.getName()));
            } catch (Exception e) {
                log.warn("Failed cleaning mviews after deleting table {}", name);
            }
            // delete table partitions metadata
            try {
                final List<QualifiedName> names = userMetadataService.getDescendantDefinitionNames(name);
                if (names != null && !names.isEmpty()) {
                    userMetadataService.deleteDefinitionMetadata(names);
                }
            } catch (Exception e) {
                log.warn("Failed cleaning partition definition metadata after deleting table {}", name);
            }
        }
    }

    /**
     * Subscriber.
     *
     * @param event event
     */
    @EventListener
    public void metacatRenameTablePostEventHandler(final MetacatRenameTablePostEvent event) {
        if (!event.isMView()) {
            final QualifiedName oldName = event.getOldTable().getName();
            final QualifiedName newName = event.getCurrentTable().getName();
            final List<NameDateDto> views = mViewService.list(oldName);
            if (views != null && !views.isEmpty()) {
                views.forEach(view -> {
                    final QualifiedName newViewName = QualifiedName
                        .ofView(oldName.getCatalogName(), oldName.getDatabaseName(), newName.getTableName(),
                            view.getName().getViewName());
                    mViewService.rename(view.getName(), newViewName);
                });
            }
        }
    }
}
