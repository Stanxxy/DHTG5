package com.company;

public interface NodeManager {
    String listAllNodes();
    String listNodeData(String name); // for testing purpose. You may implement this with an empty method
    String listNodeMeta(String name);
    String debug();
    void addNode(String name);
    void addNode(String name, Long hashValue);
    void addNode(Double weight); //for ceph
    boolean removeNode(String name);
    void unplugNode(String name);
    void loadBalancing(String name);
    void loadBalancing(String name, Double weight);
}
