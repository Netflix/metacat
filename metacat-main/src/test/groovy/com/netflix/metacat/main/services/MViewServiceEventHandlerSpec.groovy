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

package com.netflix.metacat.main.services

import com.netflix.metacat.common.NameDateDto
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.TableDto
import com.netflix.metacat.common.server.events.MetacatDeleteTablePostEvent
import com.netflix.metacat.common.server.events.MetacatRenameTablePostEvent
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.common.server.usermetadata.UserMetadataService
import com.netflix.metacat.common.server.util.MetacatContextManager
import spock.lang.Specification

/**
 * @author amajumdar
 */
class MViewServiceEventHandlerSpec extends Specification {
    def config = Mock(Config)
    def mViewService = Mock(MViewService)
    def userMetadataService = Mock(UserMetadataService)
    def context = MetacatContextManager.getContext()
    def handler = new MViewServiceEventHandler(config, mViewService, userMetadataService)
    def name = QualifiedName.ofTable('a','b','c')
    def viewName = QualifiedName.ofView('a','b','c','d')
    def dto = new TableDto(name:name)
    def deleteEvent = new MetacatDeleteTablePostEvent(name, context, context, dto, false)
    def renameEvent = new MetacatRenameTablePostEvent(name, context, context, dto, dto, false)
    def testMetacatDeleteTablePostEventHandler() {
        when:
        handler.metacatDeleteTablePostEventHandler(deleteEvent)
        then:
        1 * config.canCascadeViewsMetadataOnTableDelete() >> false
        0 * mViewService.list(name)
        when:
        handler.metacatDeleteTablePostEventHandler(deleteEvent)
        then:
        1 * config.canCascadeViewsMetadataOnTableDelete() >> true
        1 * mViewService.list(name) >> []
        when:
        handler.metacatDeleteTablePostEventHandler(deleteEvent)
        then:
        1 * config.canCascadeViewsMetadataOnTableDelete() >> true
        1 * mViewService.list(name) >> [new NameDateDto(name:viewName)]
        1 * mViewService.deleteAndReturn(viewName)
        1 * userMetadataService.getDescendantDefinitionNames(name) >> [name]
        1 * userMetadataService.deleteDefinitionMetadatas(_)
    }

    def testMetacatRenameTablePostEventHandler() {
        when:
        handler.metacatRenameTablePostEventHandler(renameEvent)
        then:
        1 * mViewService.list(name) >> []
        0 * mViewService.rename(_,_)
        when:
        handler.metacatRenameTablePostEventHandler(renameEvent)
        then:
        1 * mViewService.list(name) >> [new NameDateDto(name:viewName)]
        1 * mViewService.rename(viewName, viewName)
    }
}
