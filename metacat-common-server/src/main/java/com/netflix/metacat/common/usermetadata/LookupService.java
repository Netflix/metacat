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

package com.netflix.metacat.common.usermetadata;

import com.netflix.metacat.common.model.Lookup;

import java.util.Set;

/**
 * Lookup service API.
 * @author amajumdar
 */
public interface LookupService {
    /**
     * Returns the lookup for the given <code>name</code>.
     * @param name lookup name
     * @return lookup
     */
    Lookup get(String name);

    /**
     * Returns the value of the lookup name.
     * @param name lookup name
     * @return scalar lookup value
     */
    String getValue(String name);

    /**
     * Returns the list of values of the lookup name.
     * @param name lookup name
     * @return list of lookup values
     */
    Set<String> getValues(String name);

    /**
     * Returns the list of values of the lookup name.
     * @param lookupId lookup id
     * @return list of lookup values
     */
    Set<String> getValues(Long lookupId);

    /**
     * Saves the lookup value.
     * @param name lookup name
     * @param values multiple values
     * @return updated lookup
     */
    Lookup setValues(String name, Set<String> values);

    /**
     * Saves the lookup value.
     * @param name lookup name
     * @param values multiple values
     * @return updated lookup
     */
    Lookup addValues(String name, Set<String> values);

    /**
     * Saves the lookup value.
     * @param name lookup name
     * @param value lookup value
     * @return updated lookup
     */
    Lookup setValue(String name, String value);
}
