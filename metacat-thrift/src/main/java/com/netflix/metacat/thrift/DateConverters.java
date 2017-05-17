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
package com.netflix.metacat.thrift;

import com.netflix.metacat.common.server.properties.Config;
import lombok.NonNull;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Date;

/**
 * Date converter.
 *
 * @author amajumdar
 * @since 1.0.0
 */
public class DateConverters {
    private final Config config;

    /**
     * Constructor.
     *
     * @param config The application config.
     */
    public DateConverters(@NonNull final Config config) {
        this.config = config;
    }

    /**
     * Converts date to epoch.
     *
     * @param d date
     * @return epoch time
     */
    public Long fromDateToLong(@Nullable final Date d) {
        if (d == null) {
            return null;
        }

        final Instant instant = d.toInstant();
        return config.isEpochInSeconds() ? instant.getEpochSecond() : instant.toEpochMilli();
    }

    /**
     * Converts epoch to date.
     *
     * @param l epoch time
     * @return date
     */
    public Date fromLongToDate(@Nullable final Long l) {
        if (l == null) {
            return null;
        }

        final Instant instant = config.isEpochInSeconds() ? Instant.ofEpochSecond(l) : Instant.ofEpochMilli(l);
        return Date.from(instant);
    }
}
