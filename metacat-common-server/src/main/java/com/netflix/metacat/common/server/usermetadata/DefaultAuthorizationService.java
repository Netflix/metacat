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

package com.netflix.metacat.common.server.usermetadata;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.exception.MetacatUnAuthorizedException;
import com.netflix.metacat.common.server.properties.Config;

import java.util.Map;
import java.util.Set;

/**
 * Config based authorization Service implementation.
 *
 * @author zhenl
 * @since 1.2.0
 */
public class DefaultAuthorizationService implements AuthorizationService {
    //catalog+database name as the acl control key, and userNames as the value
    private final Map<QualifiedName, Set<String>> createACL;
    private final Map<QualifiedName, Set<String>> deleteACL;

    /**
     * Constructor.
     *
     * @param config metacat config
     */
    public DefaultAuthorizationService(
        final Config config
    ) {
        this.createACL = config.getMetacatCreateAcl();
        this.deleteACL = config.getMetacatDeleteAcl();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void checkPermission(final String userName,
                                final QualifiedName name,
                                final MetacatOperation op) {
        switch (op) {
            case CREATE:
                checkPermit(createACL, userName, name, op);
                break;
            case RENAME:
            case DELETE:
                checkPermit(deleteACL, userName, name, op);
                break;
            default:

        }
    }

    private void checkPermit(final Map<QualifiedName, Set<String>> accessACL,
                             final String userName,
                             final QualifiedName name,
                             final MetacatOperation op) {
        //for the names without database
        if (!name.isCatalogDefinition() || !name.isDatabaseDefinition()) {
            return;
        }
        final Set<String> users =
            accessACL.get(QualifiedName.ofDatabase(name.getCatalogName(), name.getDatabaseName()));
        if ((users != null) && !users.isEmpty() && !users.contains(userName)) {
            throw new MetacatUnAuthorizedException(String.format("%s is not permitted for %s %s",
                userName, op.name(), name
            ));
        }
    }

}
