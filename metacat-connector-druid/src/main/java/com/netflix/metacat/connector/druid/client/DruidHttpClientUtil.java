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

package com.netflix.metacat.connector.druid.client;

/**
 * DruidHttpClientUtil.
 *
 * @author zhenl
 * @since 1.2.0
 */
public final class DruidHttpClientUtil {
    private DruidHttpClientUtil() {
    }

    /**
     * get Latest Segment.
     *
     * @param input segments strings
     * @return lastest segment id
     */
    public static String getLatestSegment(final String input) {
        final String[] segments = input.substring(1, input.length() - 1).split(",");
        String current = segments[0].trim().replace("\"", "");
        for (int i = 1; i < segments.length; i++) {
            final String next = segments[i].trim().replace("\"", "");
            if (current.compareTo(next) <= 0) {
                current = next;
            }
        }
        return current;
    }

}
