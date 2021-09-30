package com.company;

import com.company.Commons.Node;

import java.util.List;

public interface NodeManager {

    String listAllNodes();

    String listNodeData(String name);
    // for testing purpose. You may implement this with an empty method
    String listNodeMeta(String name);

    void setReplica(Long numReplica);

    void setReplica(Long numReplica, Long minCopy);

    void setHashRange(Long range);

    void addNode(String name);

    void addNode(String name, Long hashValue);

    void removeNode(String name);

    void unplugNode(String name);

    void loadBalancing(String name);
}
