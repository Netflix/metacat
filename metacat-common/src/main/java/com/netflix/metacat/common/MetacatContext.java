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

package com.netflix.metacat.common;

/**
 * Created by amajumdar on 8/3/15.
 */
public class MetacatContext {
    public static final String HEADER_KEY_USER_NAME = "X-Netflix.user.name";
    public static final String HEADER_KEY_CLIENT_APP_NAME = "X-Netflix.client.app.name";
    public static final String HEADER_KEY_JOB_ID = "X-Netflix.job.id";
    public static final String HEADER_KEY_DATA_TYPE_CONTEXT = "X-Netflix.data.type.context";
    private final String userName;
    private final String clientAppName;
    private final String clientId;
    private final String jobId;
    private final String dataTypeContext;
    public enum DATA_TYPE_CONTEXTS {hive, pig, presto}
    public MetacatContext(String userName, String clientAppName, String clientId, String jobId, String dataTypeContext) {
        this.userName = userName;
        this.clientAppName = clientAppName;
        this.clientId = clientId;
        this.jobId = jobId;
        this.dataTypeContext = dataTypeContext;
    }

    public String getUserName() {
        return userName;
    }

    public String getClientAppName() {
        return clientAppName;
    }

    public String getJobId() {
        return jobId;
    }

    public String getClientId() {
        return clientId;
    }

    public String getDataTypeContext() {
        return dataTypeContext;
    }

    @Override
    public String toString() {
        return "MetacatContext{" + "userName='" + userName + '\'' + ", clientAppName='" + clientAppName + '\''
                + ", clientId='" + clientId + '\'' + ", jobId='" + jobId + '\'' + ", dataTypeContext='"
                + dataTypeContext + '\'' + '}';
    }
}
