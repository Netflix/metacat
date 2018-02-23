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

import com.netflix.metacat.common.server.events.MetacatCreateTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatRenameTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionMetadataOnlyPostEvent;
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePostEvent;

/**
 * Interface for services which will provide external notifications based on internal events. The structure and
 * destinations of the notifications are left up to the implementation.
 *
 * @author tgianos
 * @since 0.1.47
 */
public interface NotificationService {

    /**
     * Publish information about partitions being added.
     *
     * @param event The event passed within the JVM after a partition has been successfully added
     */
    void notifyOfPartitionAddition(MetacatSaveTablePartitionPostEvent event);

    /**
     * Publish information about partition metadata save only.
     *
     * @param event The event passed within the JVM after a partition has been successfully added
     */
    void notifyOfPartitionMetdataDataSaveOnly(MetacatSaveTablePartitionMetadataOnlyPostEvent event);

    /**
     * Publish information about partitions being deleted.
     *
     * @param event The event passed within the JVM after a partition has been successfully deleted
     */
    void notifyOfPartitionDeletion(MetacatDeleteTablePartitionPostEvent event);

    /**
     * Publish information about a table being created.
     *
     * @param event The event passed within the JVM after a table has been successfully created
     */
    void notifyOfTableCreation(MetacatCreateTablePostEvent event);

    /**
     * Publish information about a table being deleted.
     *
     * @param event The event passed within the JVM after a table has been successfully deleted
     */
    void notifyOfTableDeletion(MetacatDeleteTablePostEvent event);

    /**
     * Publish information about a table being renamed.
     *
     * @param event The event passed within the JVM after a table has been successfully renamed
     */
    void notifyOfTableRename(MetacatRenameTablePostEvent event);

    /**
     * Publish information about a table being updated.
     *
     * @param event The event passed within the JVM after a table has been successfully updated
     */
    void notifyOfTableUpdate(MetacatUpdateTablePostEvent event);
}
