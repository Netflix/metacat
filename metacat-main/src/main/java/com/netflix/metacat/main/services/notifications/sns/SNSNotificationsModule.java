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
package com.netflix.metacat.main.services.notifications.sns;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.netflix.metacat.main.services.notifications.NotificationService;
import lombok.extern.slf4j.Slf4j;

/**
 * Module for configuring notifications to AWS SNS.
 *
 * @author tgianos
 * @since 0.1.47
 */
@Slf4j
public class SNSNotificationsModule extends AbstractModule {

    /**
     * {@inheritDoc}
     */
    @Override
    protected void configure() {
        final Multibinder<NotificationService> notificationServices = Multibinder.newSetBinder(
            this.binder(),
            NotificationService.class
        );
        notificationServices.addBinding().toProvider(SNSNotificationServiceImplProvider.class).in(Scopes.SINGLETON);
    }
}
