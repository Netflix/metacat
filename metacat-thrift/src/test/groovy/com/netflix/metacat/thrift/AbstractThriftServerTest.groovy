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

import com.google.common.base.Stopwatch
import com.google.common.base.Throwables
import com.netflix.metacat.common.server.properties.Config
import org.apache.thrift.TException
import org.apache.thrift.TProcessor
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.server.TServerEventHandler
import org.apache.thrift.transport.TTransportException
import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@Unroll
class AbstractThriftServerTest extends Specification {
    Config config = Mock(Config)

    int getRandomPort() {
        def rand = new Random()
        int port = 0;
        for (; port < 1024; port = rand.nextInt(65536));
        return port
    }

    class TestThriftServer extends AbstractThriftServer {
        final TProcessor processor

        protected TestThriftServer(Config config, int port, TProcessor processor) {
            super(config, port, "test-thrift-server-port-${port}-%d")
            this.processor = processor
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
        int port = randomPort
        config.thriftServerMaxWorkerThreads >> 10
        config.thriftServerSocketClientTimeoutInSeconds >> 5
        def server = new TestThriftServer(config, port, { TProtocol tProtocol, TProtocol out ->
            false
        })

        when:
        def stopwatch = Stopwatch.createStarted()
        server.start()

        then:
        notThrown(Throwable)
        stopwatch.elapsed(TimeUnit.SECONDS) < 5

        when:
        server.stop()

        then:
        notThrown(Throwable)
        stopwatch.elapsed(TimeUnit.SECONDS) < 5
    }

    def 'the server will not stop responding with it runs out of worker threads'() {
        given:
        int port = randomPort
        config.thriftServerMaxWorkerThreads >> 1
        config.thriftServerSocketClientTimeoutInSeconds >> 1
        def server = new TestThriftServer(config, port, { TProtocol tProtocol, TProtocol out -> false })

        when:
        server.start()

        then:
        notThrown(Throwable)

        when:
        def clientConnections = new AtomicInteger(0)
        def threads = (0..<100).collect {
            return new Thread({
                Thread.sleep(new Random().nextInt(500))
                def socket = new Socket('localhost', port)
                socket.withStreams { input, output ->
                    int count = clientConnections.incrementAndGet()
                    output << "test ${count}"
                }
            })
        }
        threads.each {
            it.start()
        }
        threads.each {
            it.join()
        }

        then:
        notThrown(Throwable)
        clientConnections.get() > 50

        when:
        server.stop()

        then:
        notThrown(Throwable)
    }

    def 'throwing an exception of type #exceptionTypeClass does not stop the server from serving'() {
        given:
        int port = randomPort
        config.thriftServerMaxWorkerThreads >> 50
        config.thriftServerSocketClientTimeoutInSeconds >> 1
        Class exceptionType = exceptionTypeClass
        def server = new TestThriftServer(config, port, { TProtocol tProtocol, TProtocol out ->
            def e = exceptionType.newInstance()
            throw e
        })

        when:
        server.start()

        then:
        notThrown(Throwable)

        when:
        def clientConnections = new AtomicInteger(0)
        def threads = (0..<100).collect {
            return new Thread({
                Thread.sleep(new Random().nextInt(500))
                def socket = new Socket('localhost', port)
                socket.withStreams { input, output ->
                    int count = clientConnections.incrementAndGet()
                    output << "test ${count}"
                }
            })
        }
        threads.each {
            it.start()
        }
        threads.each {
            it.join()
        }

        then:
        notThrown(Throwable)
        clientConnections.get() == 100

        when:
        server.stop()

        then:
        notThrown(Throwable)

        where:
        exceptionTypeClass << [RuntimeException, RejectedExecutionException, Throwable, TTransportException, Error, IllegalArgumentException]
    }

    def 'the server will close a connection after reads exceed the timeout'() {
        given:
        int port = randomPort
        boolean readTimeout = false
        config.thriftServerMaxWorkerThreads >> 5
        config.thriftServerSocketClientTimeoutInSeconds >> 1
        def server = new TestThriftServer(config, port, new TProcessor() {
            @Override
            boolean process(TProtocol tProtocol, TProtocol out) throws TException {
                try {
                    tProtocol.readString()
                } catch (TTransportException tte) {
                    def rootCause = Throwables.getRootCause(tte)
                    if (rootCause instanceof SocketTimeoutException) {
                        readTimeout = true
                    }
                }
                return false
            }
        })

        when:
        def stopwatch = Stopwatch.createStarted()
        server.start()

        then:
        notThrown(Throwable)
        stopwatch.elapsed(TimeUnit.SECONDS) < 5

        when:
        def socket = new Socket('localhost', port)
        socket.withStreams { input, output ->
            Thread.sleep(1500)
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
