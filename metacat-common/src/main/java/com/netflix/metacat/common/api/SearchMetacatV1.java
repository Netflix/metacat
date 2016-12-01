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

package com.netflix.metacat.common.api;

import com.netflix.metacat.common.dto.TableDto;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.List;

/**
 * Search APIs for metacat that queries the search store.
 * @author amajumdar
 */
@Path("mds/v1/search")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface SearchMetacatV1 {
    /**
     * Searches the list of tables for the given search string.
     * @param searchString search string
     * @return list of tables
     */
    @GET
    @Path("table")
    @Consumes(MediaType.APPLICATION_JSON)
    List<TableDto> searchTables(
        @QueryParam("q")
            String searchString
    );
}
