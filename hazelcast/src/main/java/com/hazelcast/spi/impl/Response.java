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

package com.hazelcast.spi.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;

public final class Response extends AbstractOperation implements ResponseOperation, IdentifiedDataSerializable {

    private Object result = null;
    private boolean exception = false;

    public Response() {
    }

    public Response(Object result) {
        this(result, (result instanceof Throwable));
    }

    public Response(Object result, boolean exception) {
        this.result = result;
        this.exception = exception;
    }

    @Override
    public void beforeRun() throws Exception {
    }

    public void run() throws Exception {
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        final long callId = getCallId();
        final Object response;
        if (exception) {
            response = nodeEngine.toObject(result);
        } else {
            response = result;
        }
        nodeEngine.operationService.notifyRemoteCall(callId, response);
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    public boolean isException() {
        return exception;
    }

    public Object getResult() {
        return result;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        final boolean isData = result instanceof Data;
        out.writeBoolean(isData);
        if (isData) {
            ((Data) result).writeData(out);
        } else {
            out.writeObject(result);
        }
        out.writeBoolean(exception);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        final boolean isData = in.readBoolean();
        if (isData) {
            Data data = new Data();
            data.readData(in);
            result = data;
        } else {
            result = in.readObject();
        }
        exception = in.readBoolean();
    }

    @Override
    public String toString() {
        return "Response{" +
               "result=" + getResult() +
               ", exception=" + exception +
               '}';
    }

    public int getId() {
        return DataSerializerSpiHook.RESPONSE;
    }
}
