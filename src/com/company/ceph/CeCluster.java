package com.company.ceph;

import com.company.BasicDHT;
import com.company.Commons.DataObjPair;
import com.company.Commons.NodeCluster;
import com.company.NodeManager;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

public class CeCluster implements BasicDHT {
    Map<Integer, CeNode> nodes;
    Long m; // 2^m hash space
    Long k; // number of replicas

    public CeCluster(TreeMap<Integer, CeNode> nodes, Long m, Long k) {
        this.nodes = nodes;
        this.m = m;
        this.k = k;

        for(CeNode node : nodes.values()) {
            node.setCluster(this);
        }

        try {
            CephHashTools.initialize();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    public Long getM() {
        return m;
    }

    public Long getK() {
        return k;
    }

    public Map<Integer, CeNode> getGlobalNodeTable() {
        return nodes;
    }

    @Override
    public boolean insert(Long key, String value) {
        DataObjPair data = new DataObjPair(key, value);
        CephHashTools.computeDataLocation(this, data).insert(data);

        for(int i = 1; i < getK(); i ++) {
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
        return null;
    }
}
