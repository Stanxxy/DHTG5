package com.company;

import com.company.Commons.Node;

import java.util.List;

public interface NodeManager {

    String listAllNodes();

    String listNodeData(String name);

    void addNode(String name);

    void addNode(String name, Long hashValue);

    void shutDownNode(String name);

    void breakDownNode(String name);

    void loadBalancing(String name);
}
