/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.netflix.metacat

import com.netflix.metacat.common.json.MetacatJsonLocator


class TestUtilities {
    final static MetacatJsonLocator metacatJson = new MetacatJsonLocator()

    public static boolean dateCloseEnough(Date a, Date b, int acceptableDifferenceInSeconds) {
        def diff = Math.abs(a.time - b.time)

        return diff <= (acceptableDifferenceInSeconds * 1000)
    }

    public static boolean epochCloseEnough(int a, int b, int acceptableDifferenceInSeconds) {
        def diff = Math.abs(a - b)

        return diff <= acceptableDifferenceInSeconds
    }

    public static boolean epochCloseEnough(int a, Date b, int acceptableDifferenceInSeconds) {
        return epochCloseEnough(a, Math.floorDiv(b.time, 1000) as int, acceptableDifferenceInSeconds)
    }

    public static String toJsonString(final Object object)
            throws Exception {
        try {
            metacatJson.toJsonString(object)
        } catch (Exception e) {
            throw new Exception(e.getMessage(), e);
        }
    }
}
