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

package com.netflix.metacat.connector.hive.client.embedded;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;

import java.util.Set;

/**
 * HivePrivilege.
 *
 * @author zhenl
 * @since 1.0.0
 */
public enum HivePrivilege {
    /**SELECT.*/
    SELECT,
    /**INSERT.*/
    INSERT,
    /**UPDATE.*/
    UPDATE,
    /**DELETE.*/
    DELETE,
    /**OWNERSHIP.*/
    OWNERSHIP,
    /**GRANT.*/
    GRANT;

    /**
     * parsePrivilege.
     * @param userGrant userGrant
     * @return set of hive privilege
     */
    public static Set<HivePrivilege> parsePrivilege(final PrivilegeGrantInfo userGrant) {
        final String name = userGrant.getPrivilege().toUpperCase(java.util.Locale.ENGLISH);
        switch (name) {
            case "ALL":
                return ImmutableSet.copyOf(values());
            case "SELECT":
                return ImmutableSet.of(SELECT);
            case "INSERT":
                return ImmutableSet.of(INSERT);
            case "UPDATE":
                return ImmutableSet.of(UPDATE);
            case "DELETE":
                return ImmutableSet.of(DELETE);
            default:
                return ImmutableSet.of();
        }
    }
}
