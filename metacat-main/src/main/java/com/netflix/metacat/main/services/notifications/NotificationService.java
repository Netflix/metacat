/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.metacat.main.services.notifications;

import com.google.common.eventbus.Subscribe;
import com.netflix.metacat.common.server.events.MetacatCreateTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatRenameTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePostEvent;

import javax.annotation.Nonnull;

/**
 * Interface for services which will provide external notifications based on internal events. The structure and
 * destinations of the notifications are left up to the implementation.
 *
 * @author tgianos
 * @since 0.1.47
 */
public interface NotificationService {

    /**
     * Publish information about a partition being added.
     *
     * @param event The event passed within the JVM after a partition has been successfully added
     */
    @Subscribe
    void notifyOfPartitionAddition(@Nonnull final MetacatSaveTablePartitionPostEvent event);

    /**
     * Publish information about a partition being deleted.
     *
     * @param event The event passed within the JVM after a partition has been successfully deleted
     */
    @Subscribe
    void notifyOfPartitionDeletion(@Nonnull final MetacatDeleteTablePartitionPostEvent event);

    /**
     * Publish information about a table being created.
     *
     * @param event The event passed within the JVM after a table has been successfully created
     */
    @Subscribe
    void notifyOfTableCreation(@Nonnull final MetacatCreateTablePostEvent event);

    /**
     * Publish information about a table being deleted.
     *
     * @param event The event passed within the JVM after a table has been successfully deleted
     */
    @Subscribe
    void notifyOfTableDeletion(@Nonnull final MetacatDeleteTablePostEvent event);

    /**
     * Publish information about a table being renamed.
     *
     * @param event The event passed within the JVM after a table has been successfully renamed
     */
    @Subscribe
    void notifyOfTableRename(@Nonnull final MetacatRenameTablePostEvent event);

    /**
     * Publish information about a table being updated.
     *
     * @param event The event passed within the JVM after a table has been successfully updated
     */
    @Subscribe
    void notifyOfTableUpdate(@Nonnull final MetacatUpdateTablePostEvent event);
}
