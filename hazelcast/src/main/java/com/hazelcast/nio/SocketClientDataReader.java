/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.nio;

import com.hazelcast.nio.serialization.DataAdapter;

import java.nio.ByteBuffer;

class SocketClientDataReader implements SocketReader {

    private DataAdapter reader = null;

    final TcpIpConnection connection;
    final IOService ioService;

    public SocketClientDataReader(TcpIpConnection connection) {
        System.err.println("Client connected-> " + connection);
        this.connection = connection;
        this.ioService = connection.getConnectionManager().ioService;
    }

    public void read(ByteBuffer inBuffer) throws Exception {
        while (inBuffer.hasRemaining()) {
            if (reader == null) {
                reader = new DataAdapter(ioService.getSerializationContext());
            }
            boolean complete = reader.readFrom(inBuffer);
            if (complete) {
                Packet p = new Packet(reader.getData(), ioService.getSerializationContext());
                p.setHeader(Packet.HEADER_CLIENT, true);
                p.setConn(connection);
                connection.setType(ConnectionType.BINARY_CLIENT);
                ioService.handleClientPacket(p);
                reader = null;
            } else {
                break;
            }
        }
    }
}