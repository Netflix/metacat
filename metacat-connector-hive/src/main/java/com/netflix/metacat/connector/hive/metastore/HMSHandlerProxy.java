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
package com.netflix.metacat.connector.hive.metastore;

import com.netflix.metacat.connector.hive.util.HiveConfigConstants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Deadline;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.TimeUnit;

/**
 * HMSHandlerProxy.
 *
 * @author zhenl
 * @since 1.0.0
 */
public final class HMSHandlerProxy implements InvocationHandler {

    private MetacatHMSHandler metacatHMSHandler;
    private HiveConf hiveConf;
    private long timeout = 600000; //600s


    private HMSHandlerProxy(final HiveConf hiveConf) throws MetaException {
        metacatHMSHandler =
                new MetacatHMSHandler(HiveConfigConstants.HIVE_HMSHANDLER_NAME, hiveConf, false);
        metacatHMSHandler.init();
        this.hiveConf = hiveConf;
        timeout = HiveConf.getTimeVar(hiveConf,
                HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    /**
     * getProxy.
     *
     * @param hiveConf hive configuration
     * @return MetacatHMSHandler
     * @throws Exception Exception
     */
    public static IMetacatHMSHandler getProxy(final HiveConf hiveConf)
            throws Exception {

        final HMSHandlerProxy handler = new HMSHandlerProxy(hiveConf);
        return (IMetacatHMSHandler) Proxy.newProxyInstance(
                HMSHandlerProxy.class.getClassLoader(),
                new Class[]{IMetacatHMSHandler.class}, handler);
    }

    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
        Deadline.registerIfNot(timeout);
        try {
            Deadline.startTimer(method.getName());
            final Object object = method.invoke(metacatHMSHandler, args);
            Deadline.stopTimer();
            return object;

        } catch (Exception e) {
            throw e.getCause();
        }
    }
}
