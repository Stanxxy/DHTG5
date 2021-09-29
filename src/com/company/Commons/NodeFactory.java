package com.company.Commons;

import com.company.Cassandra.CaCluster;
import com.company.Cassandra.CaNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NodeFactory {

    // initialize needed nodes. For cassandra, simply put type "Ca"
    public static List<Node> initializeNodes(String type, Integer nodeNumber){
        List<Node> nodeList = new ArrayList<>(nodeNumber);
        if(type.equals("Ca")){ // Ca for Cassandra
            Long hashRange = CaCluster.getHashRange();
            long interval = (hashRange / (long) nodeNumber);
            long tmpHash = 0L;
            long counter = 0L;
            while(tmpHash < hashRange){
                // tmpHash is the endpoints, and it is inclusive;
                // counter is the sequence number for node.
                // node hash should not be the same with its sequence number. As the node may move.
                nodeList.add(generateNode(type, "Ca_Node-"+counter, tmpHash));
                tmpHash += interval;
                counter++;
            }
            return nodeList;
        } else {

            return null;
        }
    }

    public static Node generateNode(String type, String name, Long hash){
        // remember to convert to the appropriate return type by yourself
        if(type.equals("Ca")){
            CaCluster cluster = CaCluster.getCluster();
            Map<String, CaNode> existedNodes = cluster.getGlobalNodeTable();
            if(existedNodes.containsKey(name)){
                // null means fail to create node
                // Create two nodes with same name is not allowed
                return null;
            }
            List<String> seeds = new ArrayList<>();
            for(Object s : cluster.getGlobalNodeTable().keySet()){
                seeds.add((String) s);
            }
            CaNode node = new CaNode(name, hash, seeds);
            cluster.addNode(name, node);
            return node;
        } else {
            // for ceph
            return null;
        }
    }
}
