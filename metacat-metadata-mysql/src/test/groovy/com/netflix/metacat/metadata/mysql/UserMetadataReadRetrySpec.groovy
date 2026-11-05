/*
 *       Copyright 2026 Netflix, Inc.
 *          Licensed under the Apache License, Version 2.0 (the "License");
 *          you may not use this file except in compliance with the License.
 *          You may obtain a copy of the License at
 *              http://www.apache.org/licenses/LICENSE-2.0
 *          Unless required by applicable law or agreed to in writing, software
 *          distributed under the License is distributed on an "AS IS" BASIS,
 *          WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *          See the License for the specific language governing permissions and
 *          limitations under the License.
 */

package com.netflix.metacat.metadata.mysql

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.TableDto
import com.netflix.metacat.common.server.usermetadata.UserMetadataService
import com.netflix.metacat.common.server.usermetadata.UserMetadataServiceException
import org.springframework.retry.backoff.NoBackOffPolicy
import org.springframework.transaction.TransactionSystemException
import org.springframework.transaction.support.TransactionSynchronizationManager
import spock.lang.Specification

import java.sql.SQLNonTransientConnectionException
import java.sql.SQLRecoverableException
import java.sql.SQLSyntaxErrorException

class UserMetadataReadRetrySpec extends Specification {
    def name = QualifiedName.ofTable('prodhive', 'db', 't')
    def metadata = JsonNodeFactory.instance.objectNode()
    def delegate = Mock(UserMetadataService)
    def service = UserMetadataReadRetry.wrap(delegate, new NoBackOffPolicy())

    def 'read is retried on a broken connection'() {
        when:
        def result = service.getDefinitionMetadata(name)

        then:
        1 * delegate.getDefinitionMetadata(name) >> {
            throw new UserMetadataServiceException('Failed to get data', new SQLRecoverableException('Socket is closed'))
        }
        1 * delegate.getDefinitionMetadata(name) >> {
            // the read failed and so did rolling back its transaction
            throw new TransactionSystemException('JDBC rollback failed',
                new SQLNonTransientConnectionException('Communications link failure during rollback()'))
        }
        1 * delegate.getDefinitionMetadata(name) >> Optional.of(metadata)
        result.get() == metadata
    }

    def 'read gives up after the last attempt'() {
        when:
        service.getDefinitionMetadata(name)

        then:
        UserMetadataReadRetry.MAX_ATTEMPTS * delegate.getDefinitionMetadata(name) >> {
            throw new UserMetadataServiceException('Failed to get data', new SQLRecoverableException('Socket is closed'))
        }
        thrown(UserMetadataServiceException)
    }

    def 'read is not retried on other failures'() {
        when:
        service.getDefinitionMetadata(name)

        then:
        1 * delegate.getDefinitionMetadata(name) >> {
            throw new UserMetadataServiceException('Failed to get data', new SQLSyntaxErrorException('bad sql'))
        }
        thrown(UserMetadataServiceException)
    }

    def 'write is not retried'() {
        given:
        def table = new TableDto(name: name)

        when:
        service.saveMetadata('user', table, true)

        then:
        1 * delegate.saveMetadata('user', table, true) >> {
            throw new UserMetadataServiceException('Failed to save', new SQLRecoverableException('Socket is closed'))
        }
        thrown(UserMetadataServiceException)
    }

    def 'read in a caller transaction is not retried'() {
        given:
        TransactionSynchronizationManager.setActualTransactionActive(true)

        when:
        service.getDefinitionMetadata(name)

        then:
        1 * delegate.getDefinitionMetadata(name) >> {
            throw new UserMetadataServiceException('Failed to get data', new SQLRecoverableException('Socket is closed'))
        }
        thrown(UserMetadataServiceException)

        cleanup:
        TransactionSynchronizationManager.setActualTransactionActive(false)
    }
}
