package com.company.Ceph;

import com.company.BasicDHT;
import com.company.Commons.DataObjPair;
import com.company.Commons.NodeCluster;
import com.company.NodeManager;

import javax.xml.crypto.Data;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
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
        boolean success = CephHashTools.computeDataLocation(this, data).insert(data);

        for(int i = 1; i < NodeCluster.getReplica(); i ++) {
            DataObjPair replicaI = data.replicate(Long.valueOf(i));
            CephHashTools.computeDataLocation(this, replicaI).insert(replicaI);
        }
        return success;
    }

    @Override
    public DataObjPair select(Long key) {
        DataObjPair search = new DataObjPair(key);
        for(int i = 0; i < NodeCluster.getReplica(); i ++) {
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
        DataObjPair search = new DataObjPair(key);
        boolean success = false;
        for(int i = 0; i < NodeCluster.getReplica(); i ++) {
            search.setReplicaI((long) i);
            CeNode location = CephHashTools.computeDataLocation(this, search);
            success = location.update(key, value) || success;
        }
        return success;
    }

    @Override
    public boolean delete(Long key) {
        boolean success = false;
        DataObjPair search = new DataObjPair(key);
        for(int i = 0; i < NodeCluster.getReplica(); i ++) {
            search.setReplicaI((long) i);
            CeNode location = CephHashTools.computeDataLocation(this, search);
            success = location.delete(key) || success;
        }
        return success;
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
            sb.append(globalNodeTable.get(name).getData());
            return sb.toString();
        }
        return null;
    }

    @Override
    public String listNodeMeta(String name) {
        if(globalNodeTable.containsKey(name)) {
            StringBuilder sb = new StringBuilder();
            sb.append(globalNodeTable.get(name).getMetaData());
            return sb.toString();
        }
        return null;
    }

    public String debug() {
        StringBuilder sb = new StringBuilder();
        sb.append("Ceph Debug...\n");
        for(CeNode node : getGlobalNodeTable().values()) {
            sb.append(node.getData());
        }
        sb.append("...end Ceph debug\n");
        return sb.toString();
    }

    @Override
    public void addNode(String name) {
        // do not use
    }

    @Override
    public void addNode(String name, Long hashValue) {
        // do not use
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
