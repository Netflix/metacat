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

package com.netflix.metacat.thrift;

import com.netflix.metacat.common.server.Config;

import javax.inject.Inject;
import java.time.Instant;
import java.util.Date;

/**
 * Date converter.
 */
public class DateConverters {
    private static Config config;

    /**
     * Sets the config.
     * @param config config
     */
    @Inject
    public static void setConfig(final Config config) {
        DateConverters.config = config;
    }

    /**
     * Converts date to epoch.
     * @param d date
     * @return epoch time
     */
    public Long fromDateToLong(final Date d) {
        if (d == null) {
            return null;
        }

        final Instant instant = d.toInstant();
        return config.isEpochInSeconds() ? instant.getEpochSecond() : instant.toEpochMilli();
    }

    /**
     * Converts epoch to date.
     * @param l epoch time
     * @return date
     */
    public Date fromLongToDate(final Long l) {
        if (l == null) {
            return null;
        }

        final Instant instant = config.isEpochInSeconds() ? Instant.ofEpochSecond(l) : Instant.ofEpochMilli(l);
        return Date.from(instant);
    }
}
