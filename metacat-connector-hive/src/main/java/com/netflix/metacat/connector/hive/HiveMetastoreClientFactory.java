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

package com.netflix.metacat.connector.hive;

import com.google.common.net.HostAndPort;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketAddress;

/**
 * HiveMetastoreClientFactory.
 *
 * @author zhenl
 */
public class HiveMetastoreClientFactory {
    private final HostAndPort socksProxy;
    private int timeoutMillis = 3000;

    /**
     * Constructor.
     *
     * @param socksProxy address
     * @param timeoutMillis timeoutMillis
     */
    @Inject
    public HiveMetastoreClientFactory(@Nullable final HostAndPort socksProxy,
                                      final int timeoutMillis) {
        this.socksProxy = socksProxy;
        this.timeoutMillis = timeoutMillis;
    }

    private static Socket createSocksSocket(final HostAndPort proxy) {
        final SocketAddress address = InetSocketAddress.createUnresolved(proxy.getHostText(), proxy.getPort());
        return new Socket(new Proxy(Proxy.Type.SOCKS, address));
    }

    private static TTransportException rewriteException(final TTransportException e, final String host) {
        return new TTransportException(e.getType(), String.format("%s: %s", host, e.getMessage()), e.getCause());
    }

    /**
     * create.
     *
     * @param host hostname
     * @param port portnum
     * @return HiveMetastoreClient
     * @throws TTransportException TTransportException
     */
    public HiveMetastoreClient create(final String host, final int port)
            throws TTransportException {
        return new HiveMetastoreClient(createTransport(host, port));
    }

    protected TTransport createRawTransport(final String host, final int port)
            throws TTransportException {
        if (socksProxy == null) {
            final TTransport transport = new TSocket(host, port, timeoutMillis);

            try {
                transport.open();
                return transport;
            } catch (Throwable t) {
                transport.close();
                throw t;
            }
        }

        final Socket socks = createSocksSocket(socksProxy);
        try {
            try {
                socks.connect(InetSocketAddress.createUnresolved(host, port), timeoutMillis);
                socks.setSoTimeout(timeoutMillis);
                return new TSocket(socks);
            } catch (Throwable t) {
                closeQuietly(socks);
                throw t;
            }
        } catch (IOException e) {
            throw new TTransportException(e);
        }
    }

    protected TTransport createTransport(final String host, final int port)
            throws TTransportException {
        try {
            return new TTransportWrapper(createRawTransport(host, port), host);
        } catch (TTransportException e) {
            throw rewriteException(e, host);
        }
    }

    private static void closeQuietly(final Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
            // ignored
        }
    }

    private static class TTransportWrapper
            extends TTransport {
        private final TTransport transport;
        private final String host;

        TTransportWrapper(final TTransport transport, final String host) {
            this.transport = transport;
            this.host = host;
        }

        @Override
        public boolean isOpen() {
            return transport.isOpen();
        }

        @Override
        public boolean peek() {
            return transport.peek();
        }

        @Override
        public byte[] getBuffer() {
            return transport.getBuffer();
        }

        @Override
        public int getBufferPosition() {
            return transport.getBufferPosition();
        }

        @Override
        public int getBytesRemainingInBuffer() {
            return transport.getBytesRemainingInBuffer();
        }

        @Override
        public void consumeBuffer(final int len) {
            transport.consumeBuffer(len);
        }

        @Override
        public void close() {
            transport.close();
        }

        @Override
        public void open()
                throws TTransportException {
            try {
                transport.open();
            } catch (TTransportException e) {
                throw rewriteException(e, host);
            }
        }

        @Override
        public int readAll(final byte[] bytes, final int off, final int len)
                throws TTransportException {
            try {
                return transport.readAll(bytes, off, len);
            } catch (TTransportException e) {
                throw rewriteException(e, host);
            }
        }

        @Override
        public int read(final byte[] bytes, final int off, final int len)
                throws TTransportException {
            try {
                return transport.read(bytes, off, len);
            } catch (TTransportException e) {
                throw rewriteException(e, host);
            }
        }

        @Override
        public void write(final byte[] bytes)
                throws TTransportException {
            try {
                transport.write(bytes);
            } catch (TTransportException e) {
                throw rewriteException(e, host);
            }
        }

        @Override
        public void write(final byte[] bytes, final int off, final int len)
                throws TTransportException {
            try {
                transport.write(bytes, off, len);
            } catch (TTransportException e) {
                throw rewriteException(e, host);
            }
        }

        @Override
        public void flush()
                throws TTransportException {
            try {
                transport.flush();
            } catch (TTransportException e) {
                throw rewriteException(e, host);
            }
        }
    }
}
