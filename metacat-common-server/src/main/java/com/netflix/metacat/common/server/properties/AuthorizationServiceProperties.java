/*
 *  Copyright 2018 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.netflix.metacat.common.server.properties;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.exception.MetacatException;
import lombok.Data;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Properties related to authorization.
 *
 * @author zhenl
 * @since 1.2.0
 */
@Data
public class AuthorizationServiceProperties {
    private boolean enabled;
    @NonNull
    private CreateAcl createAcl = new CreateAcl();
    @NonNull
    private DeleteAcl deleteAcl = new DeleteAcl();

    /**
     * Create Acl properties.
     *
     * @author zhenl
     * @since 1.2.0
     */
    @Data
    public static class CreateAcl {
        private String createAclStr;
        private Map<QualifiedName, Set<String>> createAclMap;

        /**
         * Get the create acl map.
         *
         * @return The create acl map
         */
        public Map<QualifiedName, Set<String>> getCreateAclMap() {
             if (createAclMap == null) {
                createAclMap = createAclStr == null ? new HashMap<>()
                    : getAclMap(createAclStr);
            }
            return createAclMap;
        }
    }

    /**
     * Delete Acl properties.
     *
     * @author zhenl
     * @since 1.2.0
     */
    @Data
    public static class DeleteAcl {
        private String deleteAclStr;
        private Map<QualifiedName, Set<String>> deleteAclMap;

        /**
         * Get the delete acl map.
         *
         * @return The delete acl map
         */
        public Map<QualifiedName, Set<String>> getDeleteAclMap() {
            if (deleteAclMap == null) {
                deleteAclMap = deleteAclStr == null ? new HashMap<>()
                    : getAclMap(deleteAclStr);
            }
            return deleteAclMap;
        }
    }

    /**
     * Parse the configuration to get operation control. The control is at userName level
     * and the controlled operations include create, delete, and rename for table.
     * The format is below.
     * db1:user1,user2|db2:user1,user2
     *
     * @param aclConfig the config strings for dbs
     * @return acl config
     */
    @VisibleForTesting
    private static Map<QualifiedName, Set<String>> getAclMap(final String aclConfig) {
        final Map<QualifiedName, Set<String>> aclMap = new HashMap<>();
        try {
            for (String aclstr : StringUtils.split(aclConfig, "|")) {
                for (QualifiedName key
                    : PropertyUtils.delimitedStringsToQualifiedNamesList(
                    aclstr.substring(0, aclstr.indexOf(":")), ',')) {
                    aclMap.put(key,
                        new HashSet<>(Arrays.asList(
                            StringUtils.split(aclstr.substring(aclstr.indexOf(":") + 1), ","))));
                }

            }

        } catch (Exception e) {
            throw new MetacatException("metacat acl property parsing error" + e.getMessage());
        }
        return aclMap;
    }
}
