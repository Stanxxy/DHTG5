package com.company.Commands;

import com.company.Ceph.CeCluster;
import com.company.Ceph.CeNode;
import com.company.Commons.Node;
import com.company.Commons.NodeCluster;
import com.company.Commons.NodeFactory;
import com.company.Main;

import java.util.List;
import java.util.TreeMap;

public class TestCeph extends Command {
    public TestCeph(Main main) {
        super(main, "tc",
                "Create a test instance of a Ceph cluster",
                "tc",
                0
        );
    }

    @Override
    protected void runOnLine(String[] args) {
        CeCluster dhtObj = new CeCluster(new TreeMap<>());
        main.foregroundManager = dhtObj;
        main.foregroundDHT = dhtObj;

        NodeCluster.setReplica(3L);
        NodeCluster.setHashRange(8L);
        List<Node> nodes = NodeFactory.initializeNodes("Ce", 10, new Double[]{ 1D, 2D, 3D, 4D, 5D, 6.0, 7.0, 8.0, 9.0, 10.0 });
        TreeMap<String, CeNode> nodeTree = new TreeMap<>();
        for(Node node : nodes) {
            nodeTree.put(node.getName(), (CeNode) node);
        }
        CeCluster cluster = (CeCluster) main.foregroundDHT;
        cluster.setGlobalNodeTable(nodeTree);
        main.foregroundDHT.insert(1L, "This is test insert #1");
        main.foregroundDHT.insert(11L, "This is TeST insert #2");
        main.foregroundDHT.insert(111L, "is TeST insert #3");
        main.foregroundDHT.insert(1111L, "This is TeST #4");
        main.foregroundDHT.insert(11111L, "This is test insert #5");
        main.foregroundDHT.insert(111111L, "This is TeST insert #6");
        main.foregroundDHT.insert(1111111L, "is TeST insert #7");
        main.foregroundDHT.insert(11111111L, "This is TeST #8");
    }
}
