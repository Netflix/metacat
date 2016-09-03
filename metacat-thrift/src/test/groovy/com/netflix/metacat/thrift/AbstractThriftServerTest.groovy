/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.netflix.metacat.thrift

import com.amazonaws.util.Throwables
import com.google.common.base.Stopwatch
import com.netflix.metacat.common.server.Config
import org.apache.thrift.TException
import org.apache.thrift.TProcessor
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.server.TServerEventHandler
import org.apache.thrift.transport.TTransportException
import spock.lang.Specification
import spock.lang.Stepwise

import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit

@Stepwise
class AbstractThriftServerTest extends Specification {
    private static final int THRIFT_PORT = {
        def rand = new Random()
        int port = 0;
        for (; port < 1024; port = rand.nextInt(65536));
        return port
    }.call()
    Config config = Mock(Config)

    class TestThriftServer extends AbstractThriftServer {
        final Closure<Boolean> processClosure

        protected TestThriftServer(Config config, Closure<Boolean> processClosure) {
            super(config, THRIFT_PORT, "test-thrift-server-port-${THRIFT_PORT}-%d")
            this.processClosure = processClosure
        }

        @Override
        TProcessor getProcessor() {
            return new TProcessor() {
                @Override
                boolean process(TProtocol tProtocol, TProtocol out) throws TException {
                    return processClosure.call(tProtocol, out)
                }
            }
        }

        @Override
        boolean hasServerEventHandler() {
            return false
        }

        @Override
        TServerEventHandler getServerEventHandler() {
            return null
        }

        @Override
        String getServerName() {
            return 'test thrift server'
        }
    }

    def 'calling start does not block this thread'() {
        given:
        int serverConnections = 0
        int clientConnections = 0
        config.thriftServerSocketClientTimeoutInSeconds >> 5
        def server = new TestThriftServer(config, { tProtocol, out ->
            serverConnections++
            Thread.sleep(10)
            false
        })

        when:
        def stopwatch = Stopwatch.createStarted()
        server.start()

        then:
        notThrown(Throwable)
        stopwatch.elapsed(TimeUnit.SECONDS) < 5

        when:
        (0..<10).each {
            def socket = new Socket('localhost', THRIFT_PORT)
            socket.withStreams { input, output ->
                clientConnections++
                Thread.sleep(10)
                output << 'test'
            }
        }

        then:
        notThrown(Throwable)
        serverConnections == clientConnections
        serverConnections == 10
        stopwatch.elapsed(TimeUnit.SECONDS) < 5

        when:
        server.stop()

        then:
        notThrown(Throwable)
        stopwatch.elapsed(TimeUnit.SECONDS) < 5
    }

    def 'throwing an exception does not stop the server from serving'() {
        given:
        int serverConnections = 0
        int clientConnections = 0
        config.thriftServerSocketClientTimeoutInSeconds >> 5
        def server = new TestThriftServer(config, { tProtocol, out ->
            serverConnections++
            Thread.sleep(10)
            switch (exceptionType) {
                case 'runtime': throw new RuntimeException()
                case 'rejected': throw new RejectedExecutionException()
                case 'throwable': throw new Throwable()
                case 'transport': throw new TTransportException()
                case 'error': throw new Error()
                default: throw new IllegalArgumentException()
            }
            //noinspection GroovyUnreachableStatement
            return false
        })

        when:
        def stopwatch = Stopwatch.createStarted()
        server.start()

        then:
        notThrown(Throwable)
        stopwatch.elapsed(TimeUnit.SECONDS) < 5

        when:
        (0..<10).each {
            def socket = new Socket('localhost', THRIFT_PORT)
            socket.withStreams { input, output ->
                clientConnections++
                Thread.sleep(10)
                output << 'test'
            }
        }

        then:
        notThrown(Throwable)
        serverConnections == clientConnections
        serverConnections == 10
        stopwatch.elapsed(TimeUnit.SECONDS) < 5

        when:
        server.stop()

        then:
        notThrown(Throwable)
        stopwatch.elapsed(TimeUnit.SECONDS) < 5

        where:
        exceptionType << ['runtime', 'rejected', 'throwable', 'transport', 'error', 'default']
    }

    def 'the server will close a connection after reads exceed the timeout'() {
        given:
        boolean readTimeout = false
        config.thriftServerSocketClientTimeoutInSeconds >> 1
        def server = new TestThriftServer(config, { TProtocol tProtocol, out ->
            try {
                tProtocol.readString()
            } catch (TTransportException tte) {
                def rootCause = Throwables.getRootCause(tte)
                if (rootCause instanceof SocketTimeoutException) {
                    readTimeout = true
                }
            }
            return false
        })

        when:
        def stopwatch = Stopwatch.createStarted()
        server.start()

        then:
        notThrown(Throwable)
        stopwatch.elapsed(TimeUnit.SECONDS) < 5

        when:
        def socket = new Socket('localhost', THRIFT_PORT)
        socket.withStreams { input, output ->
            Thread.sleep(3 * 1000)
        }

        then:
        notThrown(Throwable)
        stopwatch.elapsed(TimeUnit.SECONDS) < 5
        readTimeout

        when:
        server.stop()

        then:
        notThrown(Throwable)
        stopwatch.elapsed(TimeUnit.SECONDS) < 5
    }
}
