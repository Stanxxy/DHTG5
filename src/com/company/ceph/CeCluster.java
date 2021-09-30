package com.company.ceph;

import com.company.BasicDHT;
import com.company.Commons.DataObjPair;
import com.company.Commons.NodeCluster;
import com.company.NodeManager;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

public class CeCluster extends NodeCluster<CeNode> implements BasicDHT, NodeManager {
    public CeCluster(TreeMap<String, CeNode> nodes) {
        super(nodes);

        for(CeNode node : nodes.values()) {
            node.setCluster(this);
        }

        try {
            CephHashTools.initialize();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean insert(Long key, String value) {
        DataObjPair data = new DataObjPair(key, value);
        CephHashTools.computeDataLocation(this, data).insert(data);

        for(int i = 1; i < replicas; i ++) {
            DataObjPair replicaI = data.replicate(Long.valueOf(i));
            CephHashTools.computeDataLocation(this, replicaI).insert(replicaI);
        }
        return false;
    }

    @Override
    public boolean select(Long key) {
        return false;
    }

    @Override
    public boolean update(Long key, String value) {
        return false;
    }

    @Override
    public boolean delete(Long key) {
        return false;
    }

    @Override
    public String getName() {
        return CeCluster.class.getSimpleName();
    }

    @Override
    public String listAllNodes() {
        StringBuilder sb = new StringBuilder();
        sb.append("Ceph Nodes:\n");
        for(CeNode node : globalNodeTable.values()) {
            sb.append("\t" + node.getName() + "\n");
        }
        return sb.toString();
    }

    @Override
    public String listNodeData(String name) {
        if(globalNodeTable.containsKey(name)) {
            StringBuilder sb = new StringBuilder();
            sb.append(globalNodeTable.get(name).getMetaData());
            return sb.toString();
        }
        return null;
    }

    @Override
    public void addNode(String name) {

    }

    @Override
    public void addNode(String name, Long hashValue) {

    }

    @Override
    public void removeNode(String name) {

    }

    @Override
    public void unplugNode(String name) {

    }

    @Override
    public void loadBalancing(String name) {

    }
}
