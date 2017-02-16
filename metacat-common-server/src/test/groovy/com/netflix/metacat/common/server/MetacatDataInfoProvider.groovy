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

package com.netflix.metacat.common.server

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.TableDto
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo

/**
 * Created by zhenl on 2/8/17.
 */
class MetacatDataInfoProvider {

    private static testdatabaseInfos = [DatabaseInfo.builder().name(QualifiedName.ofDatabase("testhive", "test1")).build(),
                                        DatabaseInfo.builder().name(QualifiedName.ofDatabase("testhive", "test2")).build()]

    private static databaseInfos = [DatabaseInfo.builder().name(QualifiedName.ofDatabase("testhive", "test1")).build(),
                                    DatabaseInfo.builder().name(QualifiedName.ofDatabase("testhive", "test2")).build(),
                                    DatabaseInfo.builder().name(QualifiedName.ofDatabase("testhive", "dev1")).build()]

    private static databaseNames = [
        QualifiedName.ofDatabase("testhive", "test1"),
        QualifiedName.ofDatabase("testhive", "test2"),
        QualifiedName.ofDatabase("testhive", "dev1"),
        QualifiedName.ofDatabase("testhive", "dev2")
    ]

    private static testdatabaseNames = [
        QualifiedName.ofDatabase("testhive", "test1"),
        QualifiedName.ofDatabase("testhive", "test2"),
    ]

    def static List<DatabaseInfo> getAllTestDatabaseInfo(){
        return testdatabaseInfos;
    }

    def static List<DatabaseInfo> getAllDatabaseNames(){
        return databaseNames;
    }

    def static List<DatabaseInfo> getAllTestDatabaseName(){
        return testdatabaseNames;
    }
}
