package com.company.Cassandra;

import com.company.Commons.NodeCluster;

import java.util.HashMap;

public class CaCluster extends NodeCluster<CaNode> {
    private static CaCluster cluster;

    public Long getNodeHash(String name){
        return getGlobalNodeTable().get(name).getHashValue();
    }

    public String searchForTheNearestAlive(String nodeName, String downName, int direction){
        // search for the nearest live node in the direction

        // here we ensure the node is not null
        CaNode node = globalNodeTable.get(nodeName);
        if(node == null){
            return null;
        }
        CaNode ansNode;
        if(direction == 1){
            // 1 denotes search on successor
            String nextNode = node.getTableInfo().get(downName)[1];
            ansNode = globalNodeTable.get(nextNode);
            while (ansNode == null){
                nextNode = node.getTableInfo().get(nextNode)[1]; // get the successor
                ansNode = globalNodeTable.get(nextNode);
            }
        } else {
            // 0 denotes search on predecessor
            String nextNode = node.getTableInfo().get(downName)[0];
            ansNode = globalNodeTable.get(nextNode);
            while (ansNode == null){
                nextNode = node.getTableInfo().get(nextNode)[0]; // get the successor
                ansNode = globalNodeTable.get(nextNode);
            }
        }
        return ansNode.getName();
    }

    public void insertIntoTable(String key, String[] value){
        // update the insertion into the cluster
        for(CaNode node : this.globalNodeTable.values()){
            node.getTableInfo().put(key, value);
        }
    }

    public void removeFromNodeTable(String key){
        // update the removal into the cluster
        for(CaNode node : this.globalNodeTable.values()){
            node.getTableInfo().remove(key);
        }
    }

    private CaCluster(){
        super(new HashMap<>());
    }

    public static synchronized CaCluster getCluster(){
        if(cluster == null){
            cluster = new CaCluster();
        }
        return cluster;
    }

}
