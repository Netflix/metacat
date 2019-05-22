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
package com.netflix.metacat.common.server.connectors.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Cluster information.
 *
 * @author amajumdar
 * @since 1.3.0
 */
@SuppressWarnings("unused")
@Data
@EqualsAndHashCode(callSuper = false)
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ClusterInfo implements Serializable {
    private static final long serialVersionUID = -3119788564952124498L;
    /** Name of the cluster. */
    private String name;
    /** Type of the cluster. */
    private String type;
    /** Name of the account under which the cluster was created. Ex: "abc_test" */
    private String account;
    /** Id of Account under which the cluster was created. Ex: "abc_test" */
    private String accountId;
    /** Environment under which the cluster exists. Ex: "prod", "test" */
    private String env;
    /** Region in which the cluster exists. Ex: "us-east-1" */
    private String region;
}
