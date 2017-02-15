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
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePostEvent;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

/**
 * This is a default implementation of the NotificationService interface. It doesn't really do anything other than
 * log the event that would have generated some sort of external notification in a real instance. This class exists
 * primarily to handle returns from providers when the "plugin" isn't enabled instead of returning null which is
 * prohibited by the Provider interface definition.
 *
 * @author tgianos
 * @since 0.1.47
 */
@Slf4j
public class DefaultNotificationServiceImpl implements NotificationService {

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfPartitionAddition(@Nonnull
    final MetacatSaveTablePartitionPostEvent event) {
        log.debug(event.toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfPartitionDeletion(@Nonnull final MetacatDeleteTablePartitionPostEvent event) {
        log.debug(event.toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfTableCreation(@Nonnull final MetacatCreateTablePostEvent event) {
        log.debug(event.toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfTableDeletion(@Nonnull final MetacatDeleteTablePostEvent event) {
        log.debug(event.toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfTableRename(@Nonnull final MetacatRenameTablePostEvent event) {
        log.debug(event.toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfTableUpdate(@Nonnull final MetacatUpdateTablePostEvent event) {
        log.debug(event.toString());
    }
}
