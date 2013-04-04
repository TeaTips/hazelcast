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

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;

final class RemoteCall {

    private final Address target;
    private final InvocationImpl invocation;

    RemoteCall(Address target, InvocationImpl invocation) {
        this.target = target;
        this.invocation = invocation;
    }

    void offerResponse(Object response) {
        invocation.notify(response);
    }

    void onMemberLeft(MemberImpl leftMember) {
        if (leftMember.getAddress().equals(target)) {
            invocation.onMemberLeft(leftMember);
        }
    }

    @Override
    public String toString() {
        return "RemoteCall{" +
               "target=" + target +
               ", invocation=" + invocation +
               '}';
    }
}
