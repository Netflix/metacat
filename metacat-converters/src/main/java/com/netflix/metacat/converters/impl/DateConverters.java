package com.netflix.metacat.converters.impl;

import com.netflix.metacat.common.server.Config;

import javax.inject.Inject;
import java.time.Instant;
import java.util.Date;

public class DateConverters {
    private static Config config;

    @Inject
    public static void setConfig(Config config) {
        DateConverters.config = config;
    }

    public Long fromDateToLong(Date d) {
        if (d == null) {
            return null;
        }

        Instant instant = d.toInstant();
        return config.isEpochInSeconds() ? instant.getEpochSecond() : instant.toEpochMilli();
    }

    public Date fromLongToDate(Long l) {
        if (l == null) {
            return null;
        }

        Instant instant = config.isEpochInSeconds() ? Instant.ofEpochSecond(l) : Instant.ofEpochMilli(l);
        return Date.from(instant);
    }
}
