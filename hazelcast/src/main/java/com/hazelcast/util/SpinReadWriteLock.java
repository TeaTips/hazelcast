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

package com.hazelcast.util;

import com.hazelcast.core.HazelcastException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @mdogan 12/3/12
 */
public final class SpinReadWriteLock {

    private final long spinInterval; // in ms

    private final AtomicBoolean locked = new AtomicBoolean(false);

    private final AtomicInteger readCount = new AtomicInteger();

    public SpinReadWriteLock() {
        this.spinInterval = 1;
    }

    public SpinReadWriteLock(int spinInterval, TimeUnit unit) {
        final long millis = unit.toMillis(spinInterval);
        this.spinInterval = millis > 0 ? millis : 1;
    }

    public SpinLock readLock() {
        return new ReadLock();
    }

    public SpinLock writeLock() {
        return new WriteLock();
    }

    private boolean acquireReadLock(final long time, TimeUnit unit) throws InterruptedException {
        final long timeInMillis = unit.toMillis(time > 0 ? time : 0);
        final long spin = spinInterval;
        long elapsed = 0L;
        while (locked.get()) {
            Thread.sleep(spin);
            if ((elapsed += spin) > timeInMillis) {
                return false;
            }
        }
        readCount.incrementAndGet();
        if (locked.get()) {
            readCount.decrementAndGet();
            return acquireReadLock(timeInMillis - elapsed, TimeUnit.MILLISECONDS);
        }
        return true;
    }

    private void releaseReadLock() {
        readCount.decrementAndGet();
    }

    private boolean acquireWriteLock(final long time, TimeUnit unit) throws InterruptedException {
        final long spin = spinInterval;
        final long timeInMillis = unit.toMillis(time > 0 ? time : 0);
        long elapsed = 0L;
        while (!locked.compareAndSet(false, true)) {
            Thread.sleep(spin);
            if ((elapsed += spin) > timeInMillis) {
                return false;
            }
        }
        while (readCount.get() > 0) {
            Thread.sleep(spin);
            if ((elapsed += spin) > timeInMillis) {
                locked.set(false);
                return false;
            }
        }
        return true;
    }

    private void releaseWriteLock() {
        if (!locked.getAndSet(false)) {
            throw new IllegalMonitorStateException("Current thread is not owner of the lock!");
        }
    }

    private final class ReadLock implements SpinLock {

        public void lock() throws InterruptedException {
            if (!tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                throw new HazelcastException();
            }
        }

        public boolean tryLock() {
            try {
                return tryLock(0, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                return false;
            }
        }

        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            return acquireReadLock(time, unit);
        }

        public void unlock() {
            releaseReadLock();
        }
    }

    private final class WriteLock implements SpinLock {

        public void lock() throws InterruptedException{
            if (!tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                throw new HazelcastException();
            }
        }

        public boolean tryLock() {
            try {
                acquireWriteLock(0, TimeUnit.MILLISECONDS);
                return true;
            } catch (InterruptedException e) {
                return false;
            }
        }

        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            return acquireWriteLock(time, unit);
        }

        public void unlock() {
            releaseWriteLock();
        }
    }

}
