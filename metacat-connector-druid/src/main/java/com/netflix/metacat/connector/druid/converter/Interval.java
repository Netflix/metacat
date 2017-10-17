/*
 *  Copyright 2017 Netflix, Inc.
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
 */

package com.netflix.metacat.connector.druid.converter;

import lombok.Data;

import java.time.Instant;

/**
 * Interval.
 *
 * @author zhenl
 * @since 1.2.0
 */
@Data
public class Interval implements Comparable<Interval> {
    private final Instant start;
    private final Instant end;

    @Override
    public int compareTo(final Interval interval) {
        return this.getStart().compareTo(interval.getStart());
    }
}
