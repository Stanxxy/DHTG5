package com.company;

import com.company.Commons.Node;

import java.util.List;

public interface NodeManager {

    List<Node> listAllNodes();

    String addNode(String ip);

    String removeNode(String ip);

    boolean loadBalancing();

    boolean autoLB();
}
