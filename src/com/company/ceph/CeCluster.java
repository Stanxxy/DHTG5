package com.company.Ceph;

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
        boolean success = CephHashTools.computeDataLocation(this.getGlobalNodeTable().values(), data).insert(data);

        for(int i = 1; i < NodeCluster.getReplica(); i ++) {
            DataObjPair replicaI = data.replicate(Long.valueOf(i));
            CephHashTools.computeDataLocation(this.getGlobalNodeTable().values(), replicaI).insert(replicaI);
        }
        return success;
    }

    @Override
    public DataObjPair select(Long key) {
        DataObjPair search = new DataObjPair(key);
        for(int i = 0; i < NodeCluster.getReplica(); i ++) {
            search.setReplicaI((long) i);
            CeNode location = CephHashTools.computeDataLocation(this.getGlobalNodeTable().values(), search);
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
            CeNode location = CephHashTools.computeDataLocation(this.getGlobalNodeTable().values(), search);
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
            CeNode location = CephHashTools.computeDataLocation(this.getGlobalNodeTable().values(), search);
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
    public void addNode(Double weight) {
        int nodeIndex =  this.globalNodeTable.size();
        CeNode toAdd = new CeNode(Long.valueOf(nodeIndex), weight);
        globalNodeTable.put(toAdd.getName(), toAdd);

        //re-shuffle nodes
        TreeMap<String, CeNode> newTopology = new TreeMap<>(globalNodeTable);

        ArrayList<MovingDataObj> toMove = new ArrayList<>();
        for (CeNode node : globalNodeTable.values()) {
            toMove.addAll(node.shuffle(newTopology.values()));
        }

        for (MovingDataObj moving : toMove) {
            moving.getAddress().insert(moving.getData());
        }
        return;
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
    public boolean removeNode(String name) {
        if(globalNodeTable.containsKey(name)) {
            TreeMap<String, CeNode> newTopology = new TreeMap<>(globalNodeTable);
            newTopology.remove(name);

            ArrayList<MovingDataObj> toMove = new ArrayList<>();
            for (CeNode node : globalNodeTable.values()) {
                toMove.addAll(node.shuffle(newTopology.values()));
            }

            for (MovingDataObj moving : toMove) {
                moving.getAddress().insert(moving.getData());
            }
            globalNodeTable.remove(name);
            return true;
        }
        return false;
    }

    @Override
    public void unplugNode(String name) {
        globalNodeTable.remove(name);
        System.out.println("Detected node failure, balancing...");
        ArrayList<MovingDataObj> toMove = new ArrayList<>();
        for (CeNode node : globalNodeTable.values()) {
            toMove.addAll(node.shuffle(globalNodeTable.values()));
        }

        for (MovingDataObj moving : toMove) {
            moving.getAddress().insert(moving.getData());
        }
    }

    @Override
    public void loadBalancing(String name, Double weight) {
        //check if desired node exists
        if(globalNodeTable.containsKey(name)) {

            //change the weight of a node
            globalNodeTable.get(name).setWeight(weight);

            //reshuffle nodes
            TreeMap<String, CeNode> newTopology = new TreeMap<>(globalNodeTable);

            ArrayList<MovingDataObj> toMove = new ArrayList<>();
            for (CeNode node : globalNodeTable.values()) {
                toMove.addAll(node.shuffle(newTopology.values()));
            }

            for (MovingDataObj moving : toMove) {
                moving.getAddress().insert(moving.getData());
            }
        }
        else {
            System.out.println("Requested node does not exist");
        }
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
