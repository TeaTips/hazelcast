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

package com.hazelcast.clientv2;

import com.hazelcast.clientv2.op.ClientOperation;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataAdapter;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.Connection;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.executor.FastExecutor;
import com.hazelcast.util.executor.PoolExecutorThreadFactory;

import java.util.concurrent.TimeUnit;

/**
 * @mdogan 2/20/13
 */
public class ClientEngine {

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final FastExecutor executor;
    private final SerializationService serializationService;

    private volatile boolean started = false;

    public ClientEngine(Node node) {
        this.node = node;
        this.serializationService = node.getSerializationService();
        nodeEngine = node.nodeEngine;
        final String poolNamePrefix = node.getThreadPoolNamePrefix("client");
        executor = new FastExecutor(3, 100, 1 << 16, 250L, poolNamePrefix,
                new PoolExecutorThreadFactory(node.threadGroup, poolNamePrefix, node.getConfig().getClassLoader()),
                TimeUnit.MINUTES.toMillis(3), true, false);
    }

    public void handlePacket(Packet packet) {
        if (packet.isHeaderSet(Packet.HEADER_CLIENT)) {
            if (!started) {
                executor.start(); // calling multiple times has no effect.
                started = true;
            }
            executor.execute(new ClientPacketProcessor(packet));
        } else {
            throw new IllegalArgumentException("Unknown packet type !");
        }
    }


    private class ClientPacketProcessor implements Runnable {
        final Packet packet;

        private ClientPacketProcessor(Packet packet) {
            this.packet = packet;
        }

        public void run() {
            final Connection conn = packet.getConn();
            try {
//                final Address caller = conn.getEndPoint();
                final Data data = packet.getData();
                final ClientOperation op = (ClientOperation) serializationService.toObject(data);
                final String serviceName = op.getServiceName();
                final Object result = op.process(nodeEngine.<ClientBinaryService>getService(serviceName));
                final Data resultData = serializationService.toData(result);
                conn.write(new DataAdapter(resultData, serializationService.getSerializationContext()));
            } catch (Throwable e) {
                e.printStackTrace();
//                logger.log(Level.SEVERE, e.getMessage(), e);
            }
        }
    }

    public void shutdown() {
        executor.shutdown();
    }

}
