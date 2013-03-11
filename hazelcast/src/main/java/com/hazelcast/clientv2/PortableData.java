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

import com.hazelcast.clientv2.op.ClientPortable;
import com.hazelcast.nio.serialization.*;

import java.io.IOException;

/**
 * @mdogan 2/20/13
 */
public class PortableData implements ClientPortable {

    private transient SerializationContext context;

    private Data data;

    public int getClassId() {
        return 1;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("type", data.getType());
        if (data.getClassDefinition() == null) {
            writer.writeInt("classId",  Data.NO_CLASS_ID);
        } else {
            writer.writeInt("classId", data.getClassDefinition().getClassId());
            writer.writeInt("version", data.getClassDefinition().getVersion());
            writer.writeByteArray("classDefinition", ((BinaryClassDefinition) data.getClassDefinition()).getBinary());
        }
//        writer.writeByteArray("buffer", data.buffer);
        writer.writeInt("hash", data.getPartitionHash());
    }

    public void readPortable(PortableReader r) throws IOException {
//        data = new Data();
//        data.getType() = r.readInt("type");
//        int classId = r.readInt("classId");
//        if (classId != Data.NO_CLASS_ID) {
//            int version = r.readInt("version");
//            data.getClassDefinition() = context.lookup(classId, version);
//            if (data.getClassDefinition() == null) {
//                byte[] classDefBytes = r.readByteArray("classDefinition");
//                data.getClassDefinition() = context.createClassDefinition(classDefBytes);
//            }
//        }
//        data.buffer = r.readByteArray("buffer");
//        data.setPartitionHash(r.readInt("hash"));
    }

    public void setSerializationContext(SerializationContext ctx) {
        this.context = ctx;
    }
}
