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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.netflix.metacat.canonical.common.exception.TimeZoneNotSupportedException;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

/**
 * Time zone key.
 * @author zhenl
 */
@EqualsAndHashCode
@ToString
@SuppressWarnings("checkstyle:javadocmethod")
public final class TimeZoneKey {
    /**
     * UTC_KEY.
     */
    public static final TimeZoneKey UTC_KEY = new TimeZoneKey("UTC", (short) 0);
    /**
     * MAX_TIME_ZONE_KEY.
     */
    public static final short MAX_TIME_ZONE_KEY;
    private static final Map<String, TimeZoneKey> ZONE_ID_TO_KEY;
    private static final Set<TimeZoneKey> ZONE_KEYS;

    private static final TimeZoneKey[] TIME_ZONE_KEYS;

    private static final short OFFSET_TIME_ZONE_MIN = -14 * 60;
    private static final short OFFSET_TIME_ZONE_MAX = 14 * 60;
    private static final TimeZoneKey[] OFFSET_TIME_ZONE_KEYS =
        new TimeZoneKey[OFFSET_TIME_ZONE_MAX - OFFSET_TIME_ZONE_MIN + 1];

    private final String id;

    private final short key;

    TimeZoneKey(final String id, final short key) {
        this.id = Objects.requireNonNull(id, "id is null");
        if (key < 0) {
            throw new IllegalArgumentException("key is negative");
        }
        this.key = key;
    }

    static {
        try (InputStream in = TimeZoneIndex.class.getResourceAsStream("zone-index.properties")) {
            // load zone file
            final Properties data = new Properties() {
                @Override
                public synchronized Object put(final Object keyName, final Object value) {
                    final Object existingEntry = super.put(keyName, value);
                    if (existingEntry != null) {
                        throw new AssertionError("Zone file has duplicate entries for " + keyName);
                    }
                    return null;
                }
            };
            data.load(in);

            if (data.containsKey("0")) {
                throw new AssertionError("Zone file should not contain a mapping for key 0");
            }

            final Map<String, TimeZoneKey> zoneIdToKey = new TreeMap<>();
            zoneIdToKey.put(UTC_KEY.getId().toLowerCase(Locale.ENGLISH), UTC_KEY);

            short maxZoneKey = 0;
            for (Entry<Object, Object> entry : data.entrySet()) {
                final short zoneKey = Short.valueOf(((String) entry.getKey()).trim());
                final String zoneId = ((String) entry.getValue()).trim();

                maxZoneKey = (short) Math.max(maxZoneKey, zoneKey);
                zoneIdToKey.put(zoneId.toLowerCase(Locale.ENGLISH), new TimeZoneKey(zoneId, zoneKey));
            }

            MAX_TIME_ZONE_KEY = maxZoneKey;
            ZONE_ID_TO_KEY = Collections.unmodifiableMap(new LinkedHashMap<>(zoneIdToKey));
            ZONE_KEYS = Collections.unmodifiableSet(new LinkedHashSet<>(zoneIdToKey.values()));

            TIME_ZONE_KEYS = new TimeZoneKey[maxZoneKey + 1];
            for (TimeZoneKey timeZoneKey : zoneIdToKey.values()) {
                TIME_ZONE_KEYS[timeZoneKey.getKey()] = timeZoneKey;
            }

            for (short offset = OFFSET_TIME_ZONE_MIN; offset <= OFFSET_TIME_ZONE_MAX; offset++) {
                if (offset == 0) {
                    continue;
                }
                final String zoneId = zoneIdForOffset(offset);
                final TimeZoneKey zoneKey = ZONE_ID_TO_KEY.get(zoneId);
                OFFSET_TIME_ZONE_KEYS[offset - OFFSET_TIME_ZONE_MIN] = zoneKey;
            }
        } catch (IOException e) {
            throw new AssertionError("Error loading time zone index file", e);
        }
    }

    public static Set<TimeZoneKey> getTimeZoneKeys() {
        return ZONE_KEYS;
    }

    @JsonCreator
    public static TimeZoneKey getTimeZoneKey(final short timeZoneKey) {
        Preconditions.checkArgument(timeZoneKey < TIME_ZONE_KEYS.length && TIME_ZONE_KEYS[timeZoneKey] != null,
            "Invalid time zone key %d", timeZoneKey);
        return TIME_ZONE_KEYS[timeZoneKey];
    }

    public static TimeZoneKey getTimeZoneKey(final String zoneId) {
        Objects.requireNonNull(zoneId, "Zone id is null");
        Preconditions.checkArgument(!zoneId.isEmpty(), "Zone id is an empty string");

        TimeZoneKey zoneKey = ZONE_ID_TO_KEY.get(zoneId.toLowerCase(Locale.ENGLISH));
        if (zoneKey == null) {
            zoneKey = ZONE_ID_TO_KEY.get(normalizeZoneId(zoneId));
        }
        if (zoneKey == null) {
            throw new TimeZoneNotSupportedException(zoneId);
        }
        return zoneKey;
    }

    public static TimeZoneKey getTimeZoneKeyForOffset(final long offsetMinutes) {
        if (offsetMinutes == 0) {
            return UTC_KEY;
        }

        Preconditions.checkArgument(offsetMinutes >= OFFSET_TIME_ZONE_MIN && offsetMinutes <= OFFSET_TIME_ZONE_MAX,
            "Invalid offset minutes %s", offsetMinutes);
        final TimeZoneKey timeZoneKey = OFFSET_TIME_ZONE_KEYS[((int) offsetMinutes) - OFFSET_TIME_ZONE_MIN];
        if (timeZoneKey == null) {
            throw new TimeZoneNotSupportedException(zoneIdForOffset(offsetMinutes));
        }
        return timeZoneKey;
    }


    public String getId() {
        return id;
    }

    @JsonValue
    public short getKey() {
        return key;
    }

    public static boolean isUtcZoneId(final String zoneId) {
        return normalizeZoneId(zoneId).equals("utc");
    }

    @SuppressWarnings("PMD")
    private static String normalizeZoneId(final String originalZoneId) {
        String zoneId = originalZoneId.toLowerCase(Locale.ENGLISH);

        if (zoneId.startsWith("etc/")) {
            zoneId = zoneId.substring(4);
        }

        if (isUtcEquivalentName(zoneId)) {
            return "utc";
        }

        //
        // Normalize fixed offset time zones.
        //

        // In some zones systems, these will start with UTC, GMT or UT.
        int length = zoneId.length();
        if (length > 3 && (zoneId.startsWith("utc") || zoneId.startsWith("gmt"))) {
            zoneId = zoneId.substring(3);
            length = zoneId.length();
        } else if (length > 2 && zoneId.startsWith("ut")) {
            zoneId = zoneId.substring(2);
            length = zoneId.length();
        }

        // (+/-)00:00 is UTC
        if ("+00:00".equals(zoneId) || "-00:00".equals(zoneId)) {
            return "utc";
        }

        // if zoneId matches XXX:XX, it is likely +HH:mm, so just return it
        // since only offset time zones will contain a `:` character
        if (length == 6 && zoneId.charAt(3) == ':') {
            return zoneId;
        }

        //
        // Rewrite (+/-)H[H] to (+/-)HH:00
        //
        if (length != 2 && length != 3) {
            return originalZoneId;
        }

        // zone must start with a plus or minus sign
        final char signChar = zoneId.charAt(0);
        if (signChar != '+' && signChar != '-') {
            return originalZoneId;
        }

        // extract the tens and ones characters for the hour
        final char hourTens;
        final char hourOnes;
        if (length == 2) {
            hourTens = '0';
            hourOnes = zoneId.charAt(1);
        } else {
            hourTens = zoneId.charAt(1);
            hourOnes = zoneId.charAt(2);
        }

        // do we have a valid hours offset time zone?
        if (!Character.isDigit(hourTens) || !Character.isDigit(hourOnes)) {
            return originalZoneId;
        }

        // is this offset 0 (e.g., UTC)?
        if (hourTens == '0' && hourOnes == '0') {
            return "utc";
        }
        final String end = ":00";
        return "" + signChar + hourTens + hourOnes + end;
    }

    private static boolean isUtcEquivalentName(final String zoneId) {
        return zoneId.equals("utc")
            || zoneId.equals("z")
            || zoneId.equals("ut")
            || zoneId.equals("uct")
            || zoneId.equals("ut")
            || zoneId.equals("gmt")
            || zoneId.equals("gmt0")
            || zoneId.equals("greenwich")
            || zoneId.equals("universal")
            || zoneId.equals("zulu");
    }

    private static String zoneIdForOffset(final long offset) {
        return String.format("%s%02d:%02d", offset < 0 ? "-" : "+", Math.abs(offset / 60), Math.abs(offset % 60));
    }

}
