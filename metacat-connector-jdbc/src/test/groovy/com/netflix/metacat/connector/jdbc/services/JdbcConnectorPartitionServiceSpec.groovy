/*
 *
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
 *
 */
package com.netflix.metacat.connector.jdbc.services

import com.google.common.collect.Lists
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext
import com.netflix.metacat.common.server.connectors.model.PartitionInfo
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest
import com.netflix.metacat.common.server.connectors.model.PartitionsSaveRequest
import com.netflix.metacat.common.server.connectors.model.TableInfo
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Specifications for the JdbcConnectorPartition implementation.
 *
 * @author tgianos
 * @since 1.0.0
 */
class JdbcConnectorPartitionServiceSpec extends Specification {

    @Shared
        service = new JdbcConnectorPartitionService()
    @Shared
        context = Mock(ConnectorRequestContext)
    @Shared
        name = QualifiedName.ofTable("catalog", "database", "table")

    @Unroll
    "Can't call unsupported method #methodName"() {
        when:
        method.call()

        then:
        thrown exception

        where:
        method | methodName          | exception
        (
            {
                this.service.getPartitions(
                    this.context,
                    this.name,
                    Mock(PartitionListRequest),
                    Mock(TableInfo)
                )
            }
        )      | "getPartitions"     | UnsupportedOperationException
        (
            {
                this.service.savePartitions(
                    this.context,
                    this.name,
                    Mock(PartitionsSaveRequest)
                )
            }
        )      | "savePartitions"    | UnsupportedOperationException
        (
            {
                this.service.deletePartitions(
                    this.context,
                    this.name,
                    Lists.newArrayList(),
                    Mock(TableInfo)
                )
            }
        )      | "deletePartitions"  | UnsupportedOperationException
        (
            {
                this.service.getPartitionCount(this.context, this.name, Mock(TableInfo))
            }
        )      | "getPartitionCount" | UnsupportedOperationException
        (
            {
                this.service.getPartitionNames(
                    this.context,
                    Lists.newArrayList(),
                    false
                )
            }
        )      | "getPartitionNames" | UnsupportedOperationException
        (
            {
                this.service.getPartitionKeys(
                    this.context,
                    this.name,
                    Mock(PartitionListRequest),
                    Mock(TableInfo)
                )
            }
        )      | "getPartitionKeys"  | UnsupportedOperationException
        (
            {
                this.service.getPartitionUris(
                    this.context,
                    this.name,
                    Mock(PartitionListRequest),
                    Mock(TableInfo)
                )
            }
        )      | "getPartitionUris"  | UnsupportedOperationException
        (
            {
                this.service.create(
                    this.context,
                    PartitionInfo.builder().name(this.name).build()
                )
            }
        )      | "create"            | UnsupportedOperationException
        (
            {
                this.service.update(
                    this.context,
                    PartitionInfo.builder().name(this.name).build()
                )
            }
        )      | "update"            | UnsupportedOperationException
        (
            {
                this.service.delete(
                    this.context,
                    this.name
                )
            }
        )      | "delete"            | UnsupportedOperationException
        (
            {
                this.service.get(
                    this.context,
                    this.name
                )
            }
        )      | "get"               | UnsupportedOperationException
        (
            {
                this.service.exists(
                    this.context,
                    this.name
                )
            }
        )      | "exists"            | UnsupportedOperationException
        (
            {
                this.service.list(
                    this.context,
                    this.name,
                    null,
                    null,
                    null
                )
            }
        )      | "list"              | UnsupportedOperationException
        (
            {
                this.service.listNames(
                    this.context,
                    this.name,
                    null,
                    null,
                    null
                )
            }
        )      | "listNames"         | UnsupportedOperationException
        (
            {
                this.service.rename(
                    this.context,
                    this.name,
                    this.name
                )
            }
        )      | "rename"            | UnsupportedOperationException
    }
}
