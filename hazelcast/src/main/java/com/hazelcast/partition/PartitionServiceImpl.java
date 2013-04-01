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

package com.hazelcast.partition;

import com.hazelcast.client.ClientCommandHandler;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.SystemLogService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.client.PartitionsHandler;
import com.hazelcast.spi.*;
import com.hazelcast.spi.annotation.ExecutedBy;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.annotation.ThreadType;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.ResponseHandlerFactory;
import com.hazelcast.util.Clock;
import com.hazelcast.util.executor.ExecutorThreadFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

public class PartitionServiceImpl implements PartitionService, ManagedService,
        EventPublishingService<MigrationEvent, MigrationListener>, ClientProtocolService {

    public static final String SERVICE_NAME = "hz:core:partitionService";

    private static final long REPARTITIONING_CHECK_INTERVAL = TimeUnit.SECONDS.toMillis(300); // 5 MINUTES
    private static final int REPARTITIONING_TASK_COUNT_THRESHOLD = 20;
    private static final int REPARTITIONING_TASK_REPLICA_THRESHOLD = 2;

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final int partitionCount;
    private final PartitionInfo[] partitions;
    private final MigrationManagerThread migrationManagerThread;
    private final ExecutorService migrationExecutor;
    private final int partitionMigrationInterval;
    private final long partitionMigrationTimeout;
    private final int immediateBackupInterval;
    private final PartitionServiceProxy proxy;
    private final Lock lock = new ReentrantLock();
    private final AtomicInteger version = new AtomicInteger();
    private final BlockingQueue<Runnable> immediateTasksQueue = new LinkedBlockingQueue<Runnable>();
    private final Queue<Runnable> scheduledTasksQueue = new LinkedBlockingQueue<Runnable>();
    private final AtomicBoolean sendingDiffs = new AtomicBoolean(false);
    private final AtomicBoolean migrationActive = new AtomicBoolean(true);
    private final AtomicLong lastRepartitionTime = new AtomicLong();
    private final SystemLogService systemLogService;

    // updates will be done under lock, but reads will be multithreaded.
    private volatile boolean initialized = false;

    // updates will be done under lock, but reads will be multithreaded.
    private final ConcurrentMap<Integer, MigrationInfo> activeMigrations = new ConcurrentHashMap<Integer, MigrationInfo>(3, 0.75f, 1);
    // both reads and updates will be done under lock!
    private final LinkedList<MigrationInfo> completedMigrations = new LinkedList<MigrationInfo>();

    public PartitionServiceImpl(final Node node) {
        this.partitionCount = node.groupProperties.PARTITION_COUNT.getInteger();
        this.node = node;
        this.nodeEngine = node.nodeEngine;
        this.logger = this.node.getLogger(PartitionService.class.getName());
        this.partitions = new PartitionInfo[partitionCount];
        this.systemLogService = node.getSystemLogService();
        for (int i = 0; i < partitionCount; i++) {
            this.partitions[i] = new PartitionInfo(i, new PartitionListener() {
                public void replicaChanged(PartitionReplicaChangeEvent event) {
                    if (event.getReplicaIndex() == 0 && event.getNewAddress() == null
                            && node.isActive() && node.joined()) {
                        final String warning = "Owner of partition is being removed! " +
                                "Possible data loss for partition[" + event.getPartitionId() + "]. "
                                + event;
                        logger.log(Level.WARNING, warning);
                        systemLogService.logPartition(warning);
                    }
                    if (node.isMaster()) {
                        version.incrementAndGet();
                    }
                }
            });
        }

        partitionMigrationInterval = node.groupProperties.PARTITION_MIGRATION_INTERVAL.getInteger() * 1000;
        // partitionMigrationTimeout is 1.5 times of real timeout
        partitionMigrationTimeout = (long) (node.groupProperties.PARTITION_MIGRATION_TIMEOUT.getLong() * 1.5f);
        immediateBackupInterval = node.groupProperties.IMMEDIATE_BACKUP_INTERVAL.getInteger() * 1000;

        migrationManagerThread = new MigrationManagerThread(node);
        migrationExecutor = Executors.newSingleThreadExecutor(
                new ExecutorThreadFactory(node.threadGroup, node.getConfig().getClassLoader()) {
                    protected String newThreadName() {
                        return node.getThreadNamePrefix("migration-executor");
                    }
                });
        proxy = new PartitionServiceProxy(this);
    }

    public void init(final NodeEngine nodeEngine, Properties properties) {
        migrationManagerThread.start();

        int partitionTableSendInterval = node.groupProperties.PARTITION_TABLE_SEND_INTERVAL.getInteger();
        if (partitionTableSendInterval <= 0) {
            partitionTableSendInterval = 1;
        }

        nodeEngine.getExecutionService().scheduleAtFixedRate(new SendClusterStateTask(),
                partitionTableSendInterval, partitionTableSendInterval, TimeUnit.SECONDS);

        nodeEngine.getExecutionService().scheduleAtFixedRate(new Runnable() {
            public void run() {
                if (node.isMaster() && node.isActive()
                        && initialized && shouldCheckRepartitioning()) {
                    logger.log(Level.FINEST, "Checking partition table for repartitioning...");
                    immediateTasksQueue.add(new CheckRepartitioningTask());
                }
            }
        }, 180, 180, TimeUnit.SECONDS);
    }

    public Address getPartitionOwner(int partitionId) {
        if (!initialized) {
            firstArrangement();
        }
        if (partitions[partitionId].getOwner() == null && !node.isMaster() && node.joined()) {
            notifyMasterToAssignPartitions();
        }
        return partitions[partitionId].getOwner();
    }

    private void notifyMasterToAssignPartitions() {
        if (lock.tryLock()) {
            try {
                if (!initialized && !node.isMaster() && node.getMasterAddress() != null && node.joined()) {
                    Future f = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, new AssignPartitions(),
                            node.getMasterAddress()).setTryCount(1).build().invoke();
                    f.get(1, TimeUnit.SECONDS);
                }
            } catch (Exception e) {
                logger.log(Level.FINEST, e.getMessage(), e);
            } finally {
                lock.unlock();
            }
        }
    }

    public void firstArrangement() {
        if (!node.isMaster() || !node.isActive()) {
            return;
        }
        if (!initialized) {
            lock.lock();
            try {
                if (initialized) {
                    return;
                }
                PartitionStateGenerator psg = getPartitionStateGenerator();
                logger.log(Level.INFO, "Initializing cluster partition table first arrangement...");
                PartitionInfo[] newState = psg.initialize(node.getClusterService().getMembers(), partitionCount);
                if (newState != null) {
                    for (PartitionInfo partition : newState) {
                        partitions[partition.getPartitionId()].setPartitionInfo(partition);
                    }
                    initialized = true;
                    sendPartitionRuntimeState();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    public void memberAdded(final MemberImpl member) {
        if (node.isMaster() && node.isActive()) {
            if (sendingDiffs.get()) {
                logger.log(Level.INFO, "MigrationManagerThread is already sending diffs for dead member, " +
                        "no need to initiate task!");
            } else {
                // to avoid repartitioning during a migration process.
                clearTaskQueues();
                immediateTasksQueue.offer(new PrepareRepartitioningTask());
            }
        }
    }

    public void memberRemoved(final MemberImpl member) {
        final Address deadAddress = member.getAddress();
        final Address thisAddress = node.getThisAddress();
        if (deadAddress == null || deadAddress.equals(thisAddress)) {
            return;
        }
        clearTaskQueues();
        migrationManagerThread.onMemberRemove(deadAddress);
        lock.lock();
        try {
            if (!activeMigrations.isEmpty() && node.isMaster()) {
                rollbackActiveMigrationsFromPreviousMaster(node.getLocalMember().getUuid());
            }
            // inactivate migration and sending of PartitionRuntimeState (@see #sendPartitionRuntimeState)
            // let all members notice the dead and fix their own records and indexes.
            // otherwise new master may take action fast and send new partition state
            // before other members realize the dead one and fix their records.
            final boolean migrationStatus = migrationActive.getAndSet(false);
            // list of partitions those have dead member in their replicas
            // !! this should be calculated before dead member is removed from partition table !!
            int[] indexesOfDead = new int[partitions.length];
            for (PartitionInfo partition : partitions) {
                final int replicaIndexOfDead = partition.getReplicaIndexOf(deadAddress);
                indexesOfDead[partition.getPartitionId()] = replicaIndexOfDead;
                // shift partition table up.
                // safe removal of dead address from partition table.
                // there might be duplicate dead address in partition table
                // during migration tasks' execution (when there are multiple backups and
                // copy backup tasks or because of a bug.
                while (partition.onDeadAddress(deadAddress)) ;
            }
            fixPartitionsForDead(member, indexesOfDead);
            // activate migration back after connectionDropTime x 10 milliseconds,
            // thinking optimistically that all nodes notice the dead one in this period.
            final long waitBeforeMigrationActivate = node.groupProperties.CONNECTION_MONITOR_INTERVAL.getLong()
                    * node.groupProperties.CONNECTION_MONITOR_MAX_FAULTS.getInteger() * 10;
            nodeEngine.getExecutionService().schedule(new Runnable() {
                public void run() {
                    migrationActive.compareAndSet(false, migrationStatus);
                }
            }, waitBeforeMigrationActivate, TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }
    }

    private void rollbackActiveMigrationsFromPreviousMaster(final String masterUuid) {
        lock.lock();
        try {
            if (!activeMigrations.isEmpty()) {
                for (MigrationInfo migrationInfo : activeMigrations.values()) {
                    if (!masterUuid.equals(migrationInfo.getMasterUuid())) {
                        // Still there is possibility of the other endpoint commits the migration
                        // but this node roll-backs!
                        logger.log(Level.INFO, "Rolling-back migration instantiated by the old master -> " + migrationInfo);
                        finalizeActiveMigration(migrationInfo);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void fixPartitionsForDead(final MemberImpl deadMember, final int[] indexesOfDead) {
        if (node.isMaster() && node.isActive()) {
            lock.lock();
            try {
                sendingDiffs.set(true);
                logger.log(Level.INFO, "Starting to send partition replica diffs..." + sendingDiffs.get());
                int diffCount = 0;
//                final int maxBackupCount = getMaxBackupCount();
                for (int partitionId = 0; partitionId < indexesOfDead.length; partitionId++) {
                    int indexOfDead = indexesOfDead[partitionId];
                    if (indexOfDead != -1) {
                        PartitionInfo partition = partitions[partitionId];
                        Address owner = partition.getOwner();
                        if (owner == null) {
                            logger.log(Level.FINEST, "Owner of one of the replicas of Partition[" + partitionId
                                    + "] is dead, but partition owner could not be found either!");
                            logger.log(Level.FINEST, partition.toString());
                            continue;
                        }
                        // send replica diffs to new replica owners after partition table shift.
                        for (int replicaIndex = indexOfDead; replicaIndex < PartitionInfo.MAX_REPLICA_COUNT; replicaIndex++) {
                            Address target = partition.getReplicaAddress(replicaIndex);
                            if (target != null && !target.equals(owner)) {
                                if (getMember(target) != null) {
                                    // TODO: @mm - we can reduce copied data size by only selecting diffs.
                                    immediateTasksQueue.offer(new Migrator(new MigrationInfo(partitionId, replicaIndex,
                                            MigrationType.COPY, owner, target)));
                                    diffCount++;
                                } else {
                                    logger.log(Level.WARNING, "Target member of replica diff task couldn't found! "
                                            + "Replica: " + replicaIndex + ", Dead: " + deadMember + "\n" + partition);
                                }
                            }
                        }
                        // if index of dead member is equal to or less than maxBackupCount
                        // clear indexes greater than maxBackupCount of partition.
//                        if (indexOfDead <= maxBackupCount) {
//                            for (int index = maxBackupCount + 1; index < PartitionInfo.MAX_REPLICA_COUNT; index++) {
//                                partition.setReplicaAddress(index, null);
//                            }
//                        }
                    }
                }
                sendPartitionRuntimeState();
                final int totalDiffCount = diffCount;
                immediateTasksQueue.offer(new Runnable() {
                    public void run() {
                        logger.log(Level.INFO, "Total " + totalDiffCount + " partition replica diffs have been processed.");
                        sendingDiffs.set(false);
                    }
                });
                immediateTasksQueue.offer(new PrepareRepartitioningTask());
            } finally {
                lock.unlock();
            }
        }
    }

//    private int getMaxBackupCount() {
//        final Collection<MigrationAwareService> services = nodeEngine.getServices(MigrationAwareService.class);
//        int max = 1;
//        if (services != null && !services.isEmpty()) {
//            for (MigrationAwareService service : services) {
//                max = Math.max(max, service.getMaxBackupCount());
//            }
//        }
//        return max;
//    }

    private void sendPartitionRuntimeState() {
        if (!initialized) {
            // do not send partition state until initialized!
            return;
        }
        if (!node.isMaster() || !node.isActive() || !node.joined()) {
            return;
        }
        if (!migrationActive.get()) {
            // migration is disabled because of a member leave, wait till enabled!
            return;
        }
        final Collection<MemberImpl> members = node.clusterService.getMemberList();
        lock.lock();
        try {
            final long clusterTime = node.getClusterService().getClusterTime();
            PartitionStateOperation op = new PartitionStateOperation(members, partitions,
                    new ArrayList<MigrationInfo>(completedMigrations), clusterTime, version.get());

            for (MemberImpl member : members) {
                if (!member.localMember()) {
                    try {
                        nodeEngine.getOperationService().send(op, member.getAddress());
                    } catch (Exception e) {
                        logger.log(Level.FINEST, e.getMessage(), e);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    void processPartitionRuntimeState(PartitionRuntimeState partitionState) {
        lock.lock();
        try {
            if (!node.isActive() || !node.joined()) {
                return;
            }
            final Address sender = partitionState.getEndpoint();
            final Address master = node.getMasterAddress();
            if (node.isMaster()) {
                logger.log(Level.WARNING, "This is the master node and received a PartitionRuntimeState from "
                        + sender + ". Ignoring incoming state! ");
                return;
            } else {
                if (sender == null || !sender.equals(master)) {
                    if (node.clusterService.getMember(sender) == null) {
                        logger.log(Level.SEVERE, "Received a ClusterRuntimeState from an unknown member!" +
                                " => Sender: " + sender + ", Master: " + master + "! ");
                        return;
                    } else {
                        logger.log(Level.WARNING, "Received a ClusterRuntimeState, but its sender doesn't seem master!" +
                                " => Sender: " + sender + ", Master: " + master + "! " +
                                "(Ignore if master node has changed recently.)");
                    }
                }
            }

            final Set<Address> unknownAddresses = new HashSet<Address>();
            PartitionInfo[] newPartitions = partitionState.getPartitions();
            for (PartitionInfo newPartition : newPartitions) {
                PartitionInfo currentPartition = partitions[newPartition.getPartitionId()];
                for (int index = 0; index < PartitionInfo.MAX_REPLICA_COUNT; index++) {
                    Address address = newPartition.getReplicaAddress(index);
                    if (address != null && getMember(address) == null) {
                        if (logger.isLoggable(Level.FINEST)) {
                            logger.log(Level.FINEST,
                                    "Unknown " + address + " is found in partition table sent from master "
                                            + sender + ". Probably it's already left the cluster. Partition: " + newPartition);
                        }
                        unknownAddresses.add(address);
                    }
                }
                currentPartition.setPartitionInfo(newPartition);
            }
            if (!unknownAddresses.isEmpty()) {
                StringBuilder s = new StringBuilder("Following unknown addresses are found in partition table")
                        .append(" sent from master[").append(sender).append("].")
                        .append(" (Probably they have already left the cluster.)")
                        .append(" {");
                for (Address address : unknownAddresses) {
                    s.append("\n\t").append(address);
                }
                s.append("\n}");
                logger.log(Level.WARNING, s.toString());
            }

            Collection<MigrationInfo> completedMigrations = partitionState.getCompletedMigrations();
            for (MigrationInfo completedMigration : completedMigrations) {
                addCompletedMigration(completedMigration);
                finalizeActiveMigration(completedMigration);
            }
            if (!activeMigrations.isEmpty()) {
                final MemberImpl masterMember = getMasterMember();
                rollbackActiveMigrationsFromPreviousMaster(masterMember.getUuid());
            }
            version.set(partitionState.getVersion());
            initialized = true;
        } finally {
            lock.unlock();
        }
    }

    private void finalizeActiveMigration(final MigrationInfo migrationInfo) {
        if (activeMigrations.containsKey(migrationInfo.getPartitionId())) {
            lock.lock();
            try {
                if (activeMigrations.containsValue(migrationInfo)) {
                    if (migrationInfo.startProcessing()) {
                        try {
                            final Address thisAddress = node.getThisAddress();
                            final boolean source = thisAddress.equals(migrationInfo.getFromAddress());
                            final boolean destination = thisAddress.equals(migrationInfo.getToAddress());
                            if (source || destination) {
                                final int partitionId = migrationInfo.getPartitionId();
                                final int replicaIndex = migrationInfo.getReplicaIndex();
                                final PartitionInfo migratingPartition = getPartition(partitionId);
                                final Address replicaAddress = migratingPartition.getReplicaAddress(replicaIndex);
                                final boolean success = migrationInfo.getToAddress().equals(replicaAddress);
                                final MigrationEndpoint endpoint = source ? MigrationEndpoint.SOURCE : MigrationEndpoint.DESTINATION;
                                final FinalizeMigrationOperation op = new FinalizeMigrationOperation(endpoint,
                                        migrationInfo.getMigrationType(), migrationInfo.getCopyBackReplicaIndex(), success);
                                op.setPartitionId(partitionId).setReplicaIndex(replicaIndex)
                                        .setNodeEngine(nodeEngine).setValidateTarget(false).setService(this);
                                nodeEngine.getOperationService().runOperation(op);
                            }
                        } catch (Exception e) {
                            logger.log(Level.WARNING, e.getMessage(), e);
                        } finally {
                            migrationInfo.doneProcessing();
                        }
                    } else {
                        logger.log(Level.INFO, "Scheduling finalization of " + migrationInfo
                                + ", because migration process is currently running.");
                        nodeEngine.getExecutionService().schedule(new Runnable() {
                            public void run() {
                                finalizeActiveMigration(migrationInfo);
                            }
                        }, 3, TimeUnit.SECONDS);
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }

    public boolean isPartitionMigrating(int partitionId) {
        return activeMigrations.containsKey(partitionId);
    }

    void addActiveMigration(MigrationInfo migrationInfo) {
        lock.lock();
        try {
            final int partitionId = migrationInfo.getPartitionId();
            final MigrationInfo currentMigrationInfo = activeMigrations.putIfAbsent(partitionId, migrationInfo);
            if (currentMigrationInfo != null) {
                boolean oldMaster = false;
                MigrationInfo oldMigration;
                MigrationInfo newMigration;
                final MemberImpl masterMember = getMasterMember();
                final String master = masterMember.getUuid();
                if (!master.equals(currentMigrationInfo.getMasterUuid())) {  // master changed
                    oldMigration = currentMigrationInfo;
                    newMigration = migrationInfo;
                    oldMaster = true;
                } else if (!master.equals(migrationInfo.getMasterUuid())) {  // master changed
                    oldMigration = migrationInfo;
                    newMigration = currentMigrationInfo;
                    oldMaster = true;
                } else if (!currentMigrationInfo.isProcessing() && migrationInfo.isProcessing()) {
                    // new migration arrived before partition state!
                    oldMigration = currentMigrationInfo;
                    newMigration = migrationInfo;
                } else {
                    final String message = "Something is seriously wrong! There are two migration requests for the same partition!" +
                            " First-> " + currentMigrationInfo + ", Second -> " + migrationInfo;
                    final IllegalStateException error = new IllegalStateException(message);
                    logger.log(Level.SEVERE, message, error);
                    throw error;
                }

                if (oldMaster) {
                    logger.log(Level.INFO, "Finalizing migration instantiated by the old master -> " + oldMigration);
                }
                finalizeActiveMigration(oldMigration);
                activeMigrations.put(partitionId, newMigration);
            }
        } finally {
            lock.unlock();
        }
    }

    private MemberImpl getMasterMember() {
        return node.clusterService.getMember(node.getMasterAddress());
    }

    MigrationInfo removeActiveMigration(int partitionId) {
        return activeMigrations.remove(partitionId);
    }

    private void addCompletedMigration(MigrationInfo migrationInfo) {
        lock.lock();
        try {
            if (completedMigrations.size() > 10) {
                completedMigrations.removeFirst();
            }
            completedMigrations.add(migrationInfo);
        } finally {
            lock.unlock();
        }
    }

    private void evictCompletedMigrations() {
        lock.lock();
        try {
            if (!completedMigrations.isEmpty()) {
                completedMigrations.removeFirst();
            }
        } finally {
            lock.unlock();
        }
    }

    private PartitionStateGenerator getPartitionStateGenerator() {
        return PartitionStateGeneratorFactory.newConfigPartitionStateGenerator(node.getConfig().getPartitionGroupConfig());
    }

    public PartitionInfo[] getPartitions() {
        return partitions;
    }

    MemberImpl getMember(Address address) {
        return node.clusterService.getMember(address);
    }

    public int getVersion() {
        return version.get();
    }

    private PartitionInfo getPartition(int partitionId) {
        return partitions[partitionId];
    }

    public PartitionInfo getPartitionInfo(int partitionId) {
        PartitionInfo p = getPartition(partitionId);
        if (p.getOwner() == null) {
            // probably ownerships are not set yet.
            // force it.
            getPartitionOwner(partitionId);
        }
        return p;
    }

    public boolean hasOnGoingMigrationTask() {
        return !immediateTasksQueue.isEmpty() || !scheduledTasksQueue.isEmpty()
                || !activeMigrations.isEmpty() || hasActiveBackupTask();
    }

    public boolean hasActiveBackupTask() {
        if (!initialized) return false;
//        int maxBackupCount = getMaxBackupCount();
//        if (maxBackupCount == 0) return false;
        MemberGroupFactory mgf = PartitionStateGeneratorFactory.newMemberGroupFactory(node.config.getPartitionGroupConfig());
        if (mgf.createMemberGroups(node.getClusterService().getMembers()).size() < 2) return false;
        final int size = immediateTasksQueue.size();
        if (size == 0) {
            for (PartitionInfo partition : partitions) {
                if (partition.getReplicaAddress(1) == null) {
                    logger.log(Level.WARNING, "Should take immediate backup of partition: " + partition.getPartitionId());
                    return true;
                }
            }
        } else {
            logger.log(Level.WARNING, "Should complete ongoing total of " + size + " immediate migration tasks!");
            return true;
        }
        return false;
    }

    private boolean shouldCheckRepartitioning() {
        return immediateTasksQueue.isEmpty() && scheduledTasksQueue.isEmpty()
                && lastRepartitionTime.get() < (Clock.currentTimeMillis() - REPARTITIONING_CHECK_INTERVAL);
    }

    public final int getPartitionId(Data key) {
        int hash = key.getPartitionHash();
        return (hash == Integer.MIN_VALUE) ? 0 : Math.abs(hash) % partitionCount;
    }

    public final int getPartitionId(Object key) {
        return getPartitionId(nodeEngine.toData(key));
    }

    public final int getPartitionCount() {
        return partitionCount;
    }

    public Map<Command, ClientCommandHandler> getCommandsAsMap() {
        Map<Command, ClientCommandHandler> commandHandlers = new HashMap<Command, ClientCommandHandler>();
        commandHandlers.put(Command.PARTITIONS, new PartitionsHandler(this));
        return commandHandlers;
    }

    public Map<Address, List<Integer>> getMemberPartitionsMap() {
        final int members = node.getClusterService().getSize();
        Map<Address, List<Integer>> memberPartitions = new HashMap<Address, List<Integer>>(members);
        for (int i = 0; i < getPartitionCount(); i++) {
            Address owner = getPartitionOwner(i);
            List<Integer> ownedPartitions = memberPartitions.get(owner);
            if (ownedPartitions == null) {
                ownedPartitions = new ArrayList<Integer>();
                memberPartitions.put(owner, ownedPartitions);
            }
            ownedPartitions.add(i);
        }
        return memberPartitions;
    }


    public List<Integer> getMemberPartitions(Address target) {
        List<Integer> ownedPartitions = new LinkedList<Integer>();
        for (int i = 0; i < getPartitionCount(); i++) {
            Address owner = getPartitionOwner(i);
            if (target.equals(owner)) {
                ownedPartitions.add(i);
            }
        }
        return ownedPartitions;
    }

    @PrivateApi
    public void handleMigration(Packet packet) {
        migrationExecutor.execute(new RemoteMigrationPacketProcessor(packet));
    }

    private class RemoteMigrationPacketProcessor implements Runnable {
        final Packet packet;

        RemoteMigrationPacketProcessor(Packet packet) {
            this.packet = packet;
        }

        public void run() {
            final Connection conn = packet.getConn();
            try {
                final Address caller = conn.getEndPoint();
                final Data data = packet.getData();
                final Operation op = (Operation) nodeEngine.toObject(data);
                op.setNodeEngine(nodeEngine);
                OperationAccessor.setCallerAddress(op, caller);
                OperationAccessor.setConnection(op, conn);
                final ResponseHandler rh = ResponseHandlerFactory.createRemoteResponseHandler(nodeEngine, op);
                if (!OperationAccessor.isJoinOperation(op) && node.clusterService.getMember(op.getCallerAddress()) == null) {
                    rh.sendResponse(new CallerNotMemberException(op.getCallerAddress(), op.getPartitionId(),
                            op.getClass().getName(), op.getServiceName()));
                } else if (node.joined()) {
                    op.setResponseHandler(rh);
                    nodeEngine.getOperationService().runOperation(op);
                } else {
                    rh.sendResponse(new RetryableHazelcastException("Node is not joined to the cluster yet!"));
                }
            } catch (Throwable e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
            }
        }
    }

    public static class AssignPartitions extends AbstractOperation {
        public void run() {
            final PartitionServiceImpl service = getService();
            service.firstArrangement();
        }

        @Override
        public boolean returnsResponse() {
            return true;
        }

        @Override
        public Object getResponse() {
            return Boolean.TRUE;
        }
    }

    private class SendClusterStateTask implements Runnable {
        public void run() {
            if (node.isMaster() && node.isActive()) {
                if ((!scheduledTasksQueue.isEmpty() || !immediateTasksQueue.isEmpty()) && migrationActive.get()) {
                    logger.log(Level.INFO, "Remaining migration tasks in queue => Immediate-Tasks: " + immediateTasksQueue.size()
                            + ", Scheduled-Tasks: " + scheduledTasksQueue.size());
                }
                sendPartitionRuntimeState();
            }
        }
    }

    private class PrepareRepartitioningTask implements Runnable {
        final List<MigrationInfo> lostQ = new ArrayList<MigrationInfo>();
        final List<MigrationInfo> scheduledQ = new ArrayList<MigrationInfo>(partitionCount);
        final List<MigrationInfo> immediateQ = new ArrayList<MigrationInfo>(partitionCount * 2);

        private PrepareRepartitioningTask() {
        }

        public final void run() {
            if (node.isMaster() && node.isActive() && initialized) {
                doRun();
            }
        }

        void doRun() {
            prepareMigrationTasks();
            logger.log(Level.INFO, "Re-partitioning cluster data... Immediate-Tasks: "
                    + immediateQ.size() + ", Scheduled-Tasks: " + scheduledQ.size());
            fillMigrationQueues();
        }

        void prepareMigrationTasks() {
            PartitionStateGenerator psg = getPartitionStateGenerator();
            psg.reArrange(partitions, node.getClusterService().getMembers(),
                    partitionCount, lostQ, immediateQ, scheduledQ);
        }

        void fillMigrationQueues() {
            lastRepartitionTime.set(Clock.currentTimeMillis());
            if (!lostQ.isEmpty()) {
                immediateTasksQueue.offer(new AssignLostPartitions(lostQ));
                logger.log(Level.WARNING, "Assigning new owners for " + lostQ.size() +
                        " LOST partitions!");
            }
            for (MigrationInfo migrationInfo : immediateQ) {
                immediateTasksQueue.offer(new Migrator(migrationInfo));
            }
            immediateQ.clear();
            for (MigrationInfo migrationInfo : scheduledQ) {
                scheduledTasksQueue.offer(new Migrator(migrationInfo));
            }
            scheduledQ.clear();
        }
    }

    private class AssignLostPartitions implements Runnable {
        final List<MigrationInfo> lostQ;

        private AssignLostPartitions(final List<MigrationInfo> lostQ) {
            this.lostQ = lostQ;
        }

        public void run() {
            if (!node.isMaster() || !node.isActive()) return;
            lock.lock();
            try {
                for (MigrationInfo migrationInfo : lostQ) {
                    int partitionId = migrationInfo.getPartitionId();
                    int replicaIndex = migrationInfo.getReplicaIndex();
                    if (replicaIndex != 0 || partitionId >= partitionCount) {
                        logger.log(Level.WARNING, "Wrong task for lost partitions assignment process" +
                                " => " + migrationInfo);
                        continue;
                    }
                    PartitionInfo partition = partitions[partitionId];
                    Address newOwner = migrationInfo.getToAddress();
                    MemberImpl ownerMember = node.clusterService.getMember(newOwner);
                    if (ownerMember != null) {
                        partition.setReplicaAddress(replicaIndex, newOwner);
                        sendMigrationEvent(migrationInfo, MigrationStatus.STARTED);
                        sendMigrationEvent(migrationInfo, MigrationStatus.COMPLETED);
                    }
                }
                sendPartitionRuntimeState();
            } finally {
                lock.unlock();
            }
        }
    }

    private class CheckRepartitioningTask extends PrepareRepartitioningTask implements Runnable {
        void doRun() {
            if (shouldCheckRepartitioning()) {
                final int v = version.get();
                prepareMigrationTasks();
                int totalTasks = 0;
                for (MigrationInfo task : immediateQ) {
                    if (task.getReplicaIndex() <= REPARTITIONING_TASK_REPLICA_THRESHOLD) {
                        totalTasks++;
                    }
                }
                for (MigrationInfo task : scheduledQ) {
                    if (task.getReplicaIndex() <= REPARTITIONING_TASK_REPLICA_THRESHOLD) {
                        totalTasks++;
                    }
                }
                if (!lostQ.isEmpty() || totalTasks > REPARTITIONING_TASK_COUNT_THRESHOLD) {
                    logger.log(Level.WARNING, "Something weird! Migration task queues are empty," +
                            " last repartitioning executed on " + lastRepartitionTime.get() +
                            " but repartitioning check resulted " + totalTasks + " tasks" +
                            " and " + lostQ.size() + " lost partitions!");
                    if (version.get() == v && shouldCheckRepartitioning()) {
                        fillMigrationQueues();
                    }
                }
            }
        }
    }

    @ExecutedBy(ThreadType.MIGRATION_THREAD)
    private class Migrator implements Runnable {
        final MigrationRequestOperation migrationRequestOp;
        final MigrationInfo migrationInfo;
        volatile boolean valid = true;

        Migrator(MigrationInfo migrationInfo) {
            this.migrationInfo = migrationInfo;
            final MemberImpl masterMember = getMasterMember();
            migrationInfo.setMasterUuid(masterMember.getUuid());
            migrationInfo.setMaster(masterMember.getAddress());
            this.migrationRequestOp = new MigrationRequestOperation(migrationInfo);
        }

        void onMemberRemove(Address address) {
            if (address.equals(migrationInfo.getFromAddress()) || address.equals(migrationInfo.getToAddress())) {
                valid = false;
            }
        }

        public void run() {
            try {
                if (!node.isActive() || !node.isMaster()) {
                    return;
                }
                fireMigrationEvent(MigrationStatus.STARTED);
                if (migrationInfo.getToAddress() == null) {
                    // A member is dead, this replica should not have an owner!
                    logger.log(Level.FINEST, "Fixing partition, " + migrationRequestOp.getReplicaIndex()
                            + ". replica of partition[" + migrationRequestOp.getPartitionId() + "] should be removed.");
                    removeReplicaOwner();
                } else {
                    MemberImpl fromMember = null;
                    Boolean result = Boolean.FALSE;
                    if (migrationRequestOp.isMigration()) {
                        fromMember = getMember(migrationInfo.getFromAddress());
                    } else {
                        // ignore fromAddress of task and get actual owner from partition table
                        final int partitionId = migrationRequestOp.getPartitionId();
                        fromMember = getMember(partitions[partitionId].getOwner());
                    }
                    logger.log(Level.FINEST, "Started Migration : " + migrationRequestOp);
                    systemLogService.logPartition("Started Migration : " + migrationRequestOp);
                    if (fromMember != null) {
                        migrationInfo.setFromAddress(fromMember.getAddress());
                        Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME,
                                migrationRequestOp, migrationInfo.getFromAddress())
                                .setTryCount(10).setTryPauseMillis(1000)
                                .setReplicaIndex(migrationRequestOp.getReplicaIndex()).build();

                        Future future = inv.invoke();
                        try {
                            result = (Boolean) nodeEngine.toObject(future.get(partitionMigrationTimeout, TimeUnit.SECONDS));
                        } catch (Throwable e) {
                            final Level level = node.isActive() && valid ? Level.WARNING : Level.FINEST;
                            logger.log(level, "Failed migrating from " + fromMember, e);
                        }
                    } else {
                        // Partition is lost! Assign new owner and exit.
                        result = Boolean.TRUE;
                    }
                    logger.log(Level.FINEST, "Finished Migration : " + migrationRequestOp);
                    systemLogService.logPartition("Finished Migration : " + migrationRequestOp);
                    if (Boolean.TRUE.equals(result)) {
                        processMigrationResult();
                    } else {
                        // remove active partition migration
                        final Level level = valid ? Level.WARNING : Level.FINEST;
                        logger.log(level, "Migration task has failed => " + migrationRequestOp);
                        migrationTaskFailed();
                    }
                }
            } catch (Throwable t) {
                final Level level = valid ? Level.WARNING : Level.FINEST;
                logger.log(level, "Error [" + t.getClass() + ": " + t.getMessage() + "] " +
                        "while executing " + migrationRequestOp);
                logger.log(Level.FINEST, t.getMessage(), t);
                migrationTaskFailed();
            }
        }

        private void migrationTaskFailed() {
            systemLogService.logPartition("Migration task has failed => " + migrationRequestOp);
            finalizeMigration();
            fireMigrationEvent(MigrationStatus.FAILED);
        }

        private void processMigrationResult() {
            final int partitionId = migrationRequestOp.getPartitionId();
            final int replicaIndex = migrationRequestOp.getReplicaIndex();
            final PartitionInfo partition = partitions[partitionId];
            if (PartitionInfo.MAX_REPLICA_COUNT < replicaIndex) {
                String msg = "Migrated [" + partitionId + ":" + replicaIndex
                        + "] but cannot assign. Length:" + PartitionInfo.MAX_REPLICA_COUNT;
                logger.log(Level.WARNING, msg);
            } else {
                lock.lock();
                try {
                    Address newOwner = migrationInfo.getToAddress();
                    MemberImpl ownerMember = node.clusterService.getMember(newOwner);
                    if (ownerMember == null) return;
                    partition.setReplicaAddress(replicaIndex, newOwner);
                    // if this partition should be copied back,
                    // just set partition's replica address
                    // before data is cleaned up.
                    if (migrationInfo.getCopyBackReplicaIndex() > -1) {  // valid only for migrations (move)
                        partition.setReplicaAddress(migrationInfo.getCopyBackReplicaIndex(), migrationInfo.getFromAddress());
                    }
                    finalizeMigration();
                    fireMigrationEvent(MigrationStatus.COMPLETED);
                } finally {
                    lock.unlock();
                }
            }
        }

        private void finalizeMigration() {
            lock.lock();
            try {
                addCompletedMigration(migrationInfo);
                finalizeActiveMigration(migrationInfo);
                sendPartitionRuntimeState();
            } finally {
                lock.unlock();
            }
        }

        private void fireMigrationEvent(MigrationStatus status) {
            if (migrationRequestOp.isMigration() && migrationRequestOp.getReplicaIndex() == 0) {
                sendMigrationEvent(migrationInfo, status);
            }
        }

        private void removeReplicaOwner() {
            lock.lock();
            try {
                int partitionId = migrationRequestOp.getPartitionId();
                int replicaIndex = migrationRequestOp.getReplicaIndex();
                PartitionInfo partition = partitions[partitionId];
                partition.setReplicaAddress(replicaIndex, null);
            } finally {
                lock.unlock();
            }
        }
    }

    private class MigrationManagerThread implements Runnable {
        private final Thread thread;
        private boolean running = true;
        private boolean migrating = false;
        private volatile Runnable activeTask;

        MigrationManagerThread(Node node) {
            thread = new Thread(node.threadGroup, this, node.getThreadNamePrefix("migration-manager"));
        }

        public void run() {
            try {
                while (running) {
                    Runnable r = null;
                    while (isActive() && (r = immediateTasksQueue.poll()) != null) {
                        safeRunImmediate(r);
                    }
                    if (!running) {
                        break;
                    }
                    // wait for partitionMigrationInterval before executing scheduled tasks
                    // and poll immediate tasks occasionally during wait time.
                    long totalWait = 0L;
                    while (isActive() && (r != null || totalWait < partitionMigrationInterval)) {
                        long start = Clock.currentTimeMillis();
                        r = immediateTasksQueue.poll(1, TimeUnit.SECONDS);
                        safeRunImmediate(r);
                        totalWait += (Clock.currentTimeMillis() - start);
                    }
                    if (isActive()) {
                        r = scheduledTasksQueue.poll();
                        safeRun(r);
                    }
                    final boolean hasNoTasks = hasNoTasks();
                    if (!migrationActive.get() || hasNoTasks) {
                        if (hasNoTasks && migrating) {
                            migrating = false;
                            logger.log(Level.INFO, "All migration tasks has been completed, queues are empty.");
                        }
                        evictCompletedMigrations();
                        Thread.sleep(250);
                        continue;
                    }
                }
            } catch (InterruptedException e) {
                logger.log(Level.FINEST, "MigrationManagerThread is interrupted: " + e.getMessage());
                running = false;
            } finally {
                clearTaskQueues();
            }
        }

        boolean hasNoTasks() {
            return (immediateTasksQueue.isEmpty() && scheduledTasksQueue.isEmpty());
        }

        boolean isActive() {
            return running && !thread.isInterrupted() && migrationActive.get();
        }

        boolean safeRun(final Runnable r) {
            if (r == null || !running) return false;
            try {
                migrating = (r instanceof Migrator);
                activeTask = r;
                r.run();
            } catch (Throwable t) {
                logger.log(Level.WARNING, t.getMessage(), t);
            } finally {
                activeTask = null;
            }
            return true;
        }

        void safeRunImmediate(final Runnable r) throws InterruptedException {
            if (safeRun(r) && immediateBackupInterval > 0) {
                Thread.sleep(immediateBackupInterval);
            }
        }

        void onMemberRemove(Address address) {
            final Runnable r = activeTask;
            if (r != null && r instanceof Migrator) {
                ((Migrator) r).onMemberRemove(address);
            }
        }

        void start() {
            thread.start();
        }

        void stopNow() {
            clearTaskQueues();
            immediateTasksQueue.offer(new Runnable() {
                public void run() {
                    running = false;
                }
            });
            thread.interrupt();
        }
    }

    public void reset() {
        clearTaskQueues();
        lock.lock();
        try {
            initialized = false;
            for (PartitionInfo partition : partitions) {
                for (int i = 0; i < PartitionInfo.MAX_REPLICA_COUNT; i++) {
                    partition.setReplicaAddress(i, null);
                }
            }
            activeMigrations.clear();
            completedMigrations.clear();
            version.set(0);
        } finally {
            lock.unlock();
        }
    }

    public void shutdown() {
        logger.log(Level.FINEST, "Shutting down the partition service");
        migrationManagerThread.stopNow();
        migrationExecutor.shutdownNow();
        reset();
    }

    private void clearTaskQueues() {
        immediateTasksQueue.clear();
        scheduledTasksQueue.clear();
    }

    public long getImmediateTasksCount() {
        return immediateTasksQueue.size();
    }

    public long getScheduledTasksCount() {
        return scheduledTasksQueue.size();
    }

    public PartitionServiceProxy getPartitionServiceProxy() {
        return proxy;
    }

    private void sendMigrationEvent(final MigrationInfo migrationInfo, final MigrationStatus status) {
        final MemberImpl current = getMember(migrationInfo.getFromAddress());
        final MemberImpl newOwner = getMember(migrationInfo.getToAddress());
        final MigrationEvent event = new MigrationEvent(migrationInfo.getPartitionId(), current, newOwner, status);
        final EventService eventService = nodeEngine.getEventService();
        final Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, SERVICE_NAME);
        eventService.publishEvent(SERVICE_NAME, registrations, event);
    }

    public void addMigrationListener(MigrationListener migrationListener) {
        nodeEngine.getEventService().registerListener(SERVICE_NAME, SERVICE_NAME, migrationListener);
    }

    public void removeMigrationListener(MigrationListener migrationListener) {
        // TODO: @mm - implement migration listener removal.
    }

    public void dispatchEvent(MigrationEvent migrationEvent, MigrationListener migrationListener) {
        switch (migrationEvent.getStatus()) {
            case STARTED:
                migrationListener.migrationStarted(migrationEvent);
                break;
            case COMPLETED:
                migrationListener.migrationCompleted(migrationEvent);
                break;
            case FAILED:
                migrationListener.migrationFailed(migrationEvent);
                break;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PartitionManager[" + version + "] {\n");
        sb.append("\n");
        sb.append("immediateQ: ").append(immediateTasksQueue.size());
        sb.append(", scheduledQ: ").append(scheduledTasksQueue.size());
        sb.append("\n}");
        return sb.toString();
    }
}
