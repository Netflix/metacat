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

package com.netflix.metacat.canonical.common.spi.util;

import java.util.Objects;
import java.util.TimeZone;

/**
 * time zone index.
 * @author zhenl
 */
public final class TimeZoneIndex {
    private static final TimeZone[] TIME_ZONES;

    private TimeZoneIndex() {
    }

    static {
        TIME_ZONES = new TimeZone[TimeZoneKey.MAX_TIME_ZONE_KEY + 1];
        for (TimeZoneKey timeZoneKey : TimeZoneKey.getTimeZoneKeys()) {
            final String zoneId = timeZoneKey.getId();
            final TimeZone timeZone;
            // zone class is totally broken...
            if (zoneId.charAt(0) == '-' || zoneId.charAt(0) == '+') {
                timeZone = TimeZone.getTimeZone("GMT" + zoneId);
            } else {
                timeZone = TimeZone.getTimeZone(zoneId);
            }
            TIME_ZONES[timeZoneKey.getKey()] = timeZone;
        }
    }

    /**
     * get time zone for key.
     *
     * @param timeZoneKey time zone key.
     * @return timezone.
     */
    public static TimeZone getTimeZoneForKey(final TimeZoneKey timeZoneKey) {
        Objects.requireNonNull(timeZoneKey, "timeZoneKey is null");
        return (TimeZone) TIME_ZONES[timeZoneKey.getKey()].clone();
    }
}
