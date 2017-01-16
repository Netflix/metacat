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

package com.netflix.metacat.canonical.common.exception;

import lombok.Getter;

/**
 * Time zone not supported class.
 * @author zhenl
 */
public class TimeZoneNotSupportedException
    extends RuntimeException {
    @Getter private final String zoneId;

    /**
     * Constructor.
     * @param zoneId zoneid.
     */
    public TimeZoneNotSupportedException(final String zoneId) {
        super("Time zone " + zoneId + " is not supported");
        this.zoneId = zoneId;
    }
}
