package com.company.Ceph;

import com.company.BasicDHT;
import com.company.Commons.DataObjPair;
import com.company.Commons.NodeCluster;
import com.company.NodeManager;

import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class CeCluster extends NodeCluster<CeNode> implements BasicDHT, NodeManager {
    public CeCluster(TreeMap<String, CeNode> nodes) {
        super(nodes);

        setGlobalNodeTable(nodes);

        try {
            CephHashTools.initialize();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean insert(Long key, String value) {
        DataObjPair data = new DataObjPair(key, value);
        try {
            CeNode location = CephHashTools.computeDataLocation(this, data);
            return location.insert(data);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public DataObjPair select(Long key) {
        for(int i = 0; i < NodeCluster.getReplica(); i ++) {
            DataObjPair search = new DataObjPair(key);
            search.setReplicaI((long) i);
            CeNode location = CephHashTools.computeDataLocation(this, search);
            DataObjPair result = location.select(search);

            if(result != null) {
                return result;
            }
        }
        return null;
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
            sb.append("\t").append(node.getName()).append("\n");
        }
        return sb.toString();
    }

    @Override
    public String listNodeData(String name) {
        // should be put in "listNodeMeta"
        // here I simply want to list all data
        if(globalNodeTable.containsKey(name)) {
            StringBuilder sb = new StringBuilder();
            sb.append(globalNodeTable.get(name).getMetaData());
            return sb.toString();
        }
        return null;
    }

    @Override
    public String listNodeMeta(String name) {
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

    @Override
    public void setGlobalNodeTable(Map<String, CeNode> nodes) {
        this.globalNodeTable = nodes;

        for(CeNode node : nodes.values()) {
            node.setCluster(this);
        }
    }
}
