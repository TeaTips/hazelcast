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

package com.hazelcast.sample;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.SerializationConstants;
import com.hazelcast.spi.*;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @mdogan 3/25/13
 */
public class Main {

    public static void main(String[] args) throws Exception {
        final Config config = new XmlConfigBuilder().build();
        config.getServicesConfig().addServiceConfig(
                new ServiceConfig().setName(TestService.NAME).setServiceImpl(new TestService()).setEnabled(true));
        config.getMapConfig("test").setBackupCount(0);

        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
//        Hazelcast.newHazelcastInstance(config);

//        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(config, 2);
//        final HazelcastInstance hz = instances[0];

        final TestObject test = hz.getDistributedObject(TestService.NAME, "");
        final AtomicInteger count = new AtomicInteger();
        final IMap<Object,Object> map = hz.getMap("test");
        final int value = args.length == 0 ? 10 : Integer.parseInt(args[0]);

        new Thread() {
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        return;
                    }
                    int k = count.getAndSet(0) / 5;
                    System.err.println("Current -> " + k + ", Map-Size(" + value + "): " + map.size());
                }
            }
        }.start();

        final Data data = new Data(SerializationConstants.CONSTANT_TYPE_BYTE_ARRAY, new byte[value]);
        final int coreSize = Runtime.getRuntime().availableProcessors();
        for (int i = 0; i < coreSize * 20; i++) {
            new Thread() {
                public void run() {
                    Random rand = new Random();
                    while (true) {
                        // test.process(rand.nextInt(100000));
                        map.put(rand.nextInt(1000000), /*new byte[value]*/ data);
                        count.incrementAndGet();
                    }
                }
            }.start();
        }
    }

    static class TestObject extends AbstractDistributedObject<TestService> {

        protected TestObject(NodeEngine nodeEngine, TestService service) {
            super(nodeEngine, service);
        }

        void process(Object key) {
            final NodeEngine nodeEngine = getNodeEngine();
            final Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(getServiceName(),
                    new TestOperation(), nodeEngine.getPartitionService().getPartitionId(key)).build();

            final Future f = inv.invoke();
            try {
                f.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String getServiceName() {
            return TestService.NAME;
        }

        @Override
        public Object getId() {
            return "test";
        }

        @Override
        public String getName() {
            return "test";
        }
    }

    static class TestService implements ManagedService, RemoteService {
        static final String NAME = "test-service";

        NodeEngine nodeEngine;

        public void init(NodeEngine nodeEngine, Properties properties) {
            this.nodeEngine = nodeEngine;
        }

        public void reset() {
        }

        public void shutdown() {
        }

        public String getServiceName() {
            return NAME;
        }

        @Override
        public DistributedObject createDistributedObject(Object objectId) {
            return new TestObject(nodeEngine, this);
        }

        @Override
        public DistributedObject createDistributedObjectForClient(Object objectId) {
            return null;
        }

        @Override
        public void destroyDistributedObject(Object objectId) {
        }
    }

    static class TestOperation extends AbstractOperation implements BackupAwareOperation, IdentifiedDataSerializable {

        public void run() throws Exception {
        }

        @Override
        public boolean shouldBackup() {
            return true;
        }

        @Override
        public int getSyncBackupCount() {
            return 0;
        }

        @Override
        public int getAsyncBackupCount() {
            return 0;
        }

        @Override
        public Operation getBackupOperation() {
            return new TestBackupOperation();
        }

        @Override
        public int getId() {
            return 1111;
        }
    }

    static class TestBackupOperation extends AbstractOperation implements BackupOperation, IdentifiedDataSerializable {

        public void run() throws Exception {

        }

        @Override
        public int getId() {
            return 2222;
        }
    }

    static {
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.wait.seconds.before.join", "0");
        System.setProperty("hazelcast.partition.migration.interval", "0");
        System.setProperty("java.net.preferIPv4Stack", "true");
//        System.setProperty("hazelcast.socket.receive.buffer.size", "8");
//        System.setProperty("hazelcast.socket.send.buffer.size", "8");
    }
}
