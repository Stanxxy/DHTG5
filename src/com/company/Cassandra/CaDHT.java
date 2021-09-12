package com.company.Cassandra;

import com.company.BasicDHT;
import com.company.Commons.Node;
import com.company.NodeManager;

import java.util.HashMap;
import java.util.List;

// CaDHT is the client of Cassandra DHT
public class CaDHT implements BasicDHT, NodeManager {

    List<String, > ipList;

    List<>

    public CaDHT(List<String> ipList) {
        this.ipList = ipList;
    }

    public void addNode(String ip){
        this.ipList.add()
    }

    @Override
    public String getName() {
         return CaDHT.class.getSimpleName();
    }

    @Override
    public String insert(String key, String value) {
        return null;
    }

    @Override
    public String select(String key) {
        return null;
    }

    @Override
    public String update(String key, String value) {
        return null;
    }

    @Override
    public String delete(String key) {
        return null;
    }

    @Override
    public List<Node> listAllNodes() {
        return null;
    }

    @Override
    public String addNode(String ip) {
        return null;
    }

    @Override
    public String removeNode(String ip) {
        return null;
    }

    @Override
    public boolean loadBalancing() {
        return false;
    }

    @Override
    public boolean autoLB() {
        return false;
    }
}
