package com.company.Ceph;

import com.company.Ceph.CeCluster;
import com.company.Commons.DataObjPair;
import com.company.Commons.Node;
import com.company.Commons.NodeCluster;
import com.company.issuables.Insert;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class CeNode extends Node {
    private CeCluster cluster;

    private Long index;
    private Double weight;

    public CeNode(Long index, Double weight) {
        this.index = index;
        this.weight = weight;
        this.name = "Ce_Node-" + index;

        storedData = new HashMap<>();
    }

    public boolean insert(DataObjPair data) {
        if(!storedData.containsKey(data.getKey())) {
            storedData.put(data.getKey(), data);
            return true;
        }
        return false;
    }

    public DataObjPair select(DataObjPair search) {
        if(storedData.containsKey(search.getKey())) {
            return storedData.get(search.getKey());
        }
        return null;
    }

    public boolean update(Long key, String newValue) {
        if(storedData.containsKey(key)) {
            storedData.get(key).setValue(newValue);
            return true;
        }
        return false;
    }

    public boolean delete(Long key) {
        if(storedData.containsKey(key)) {
            storedData.remove(key);
            return true;
        }
        return false;
    }

    //
    public boolean shuffle() {
        for(Long key : storedData.keySet()) {

        }
        return true;
    }

    public Long getIndex() {
        return index;
    }

    public void setCluster(CeCluster cluster) {
        this.cluster = cluster;
    }

    public CeCluster getCluster() {
        return cluster;
    }

    // when the weight changes, it is time to start reshuffling nodes
    // to other locations
    public void setWeight(Double weight) {
        this.weight = weight;
    }

    public Double getWeight() {
        return weight;
    }

    public Long getLoad() {
        return Long.valueOf(storedData.size());
    }

    // the issuing of various tasks (can be inbound from other nodes,
    // the UI command issuer, or from itself)

    public String getMetaData() {
        StringBuilder sb = new StringBuilder();
        sb.append(getName() + "\n");
        sb.append("\tWeight: " + weight + "; Load: " + getLoad() + "\n");
        return sb.toString();
    }

    public String getData() {
        StringBuilder sb = new StringBuilder();
        sb.append(getMetaData());
        sb.append("\tData:\n");
        for(DataObjPair data : storedData.values()) {
            sb.append("\t\t" + data + "\n");
        }
        return sb.toString();
    }
}
