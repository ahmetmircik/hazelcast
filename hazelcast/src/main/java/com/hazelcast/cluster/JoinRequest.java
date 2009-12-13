/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.cluster;

import com.hazelcast.impl.NodeType;
import com.hazelcast.nio.Address;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JoinRequest extends AbstractRemotelyProcessable {

    protected NodeType nodeType = NodeType.MEMBER;
    public Address address;
    public Address to;
    public String groupName;
    public String groupPassword;

    public JoinRequest() {
    }

    public JoinRequest(Address address, String groupName, String groupPassword, NodeType type) {
        this(null, address, groupName, groupPassword, type);
    }

    public JoinRequest(Address to, Address address, String groupName, String groupPassword, NodeType type) {
        super();
        this.to = to;
        this.address = address;
        this.groupName = groupName;
        this.groupPassword = groupPassword;
        this.nodeType = type;
    }

    @Override
    public void readData(DataInput in) throws IOException {
        boolean hasTo = in.readBoolean();
        if (hasTo) {
            to = new Address();
            to.readData(in);
        }
        address = new Address();
        address.readData(in);
        nodeType = NodeType.create(in.readInt());
        groupName = in.readUTF();
        groupPassword = in.readUTF();
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        boolean hasTo = (to != null);
        out.writeBoolean(hasTo);
        if (hasTo) {
            to.writeData(out);
        }
        address.writeData(out);
        out.writeInt(nodeType.getValue());
        out.writeUTF(groupName);
        out.writeUTF(groupPassword);
    }

    @Override
    public String toString() {
        return "JoinRequest{" +
                "nodeType=" + nodeType +
                ", address=" + address +
                ", groupName='" + groupName + '\'' +
                ", groupPassword='" + groupPassword + '\'' +
                '}';
    }

    public void process() {
        getNode().clusterManager.handleJoinRequest(this);
    }
}
