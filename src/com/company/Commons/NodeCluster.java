package com.company.Commons;

import com.company.Cassandra.CaCluster;
import com.company.Ceph.CeNode;

import java.nio.channels.NotYetBoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NodeCluster<NType> {
    protected static Long hashRange; // m
    protected static Long replicas; // k
    protected static Long minCopy;
    protected Map<String, NType> globalNodeTable; // now string is the string hash value.

    public NodeCluster(Map<String, NType> hashMap) {
        this.globalNodeTable = hashMap;
    }

    public void setGlobalNodeTable(Map<String, NType> nodes) {
        this.globalNodeTable = nodes;
    }

    public Map<String, NType> getGlobalNodeTable(){
        return this.globalNodeTable;
    }

    public List<String> getNameList(){
        return new ArrayList<>(globalNodeTable.keySet());
    }

    public boolean addNode(String name, NType node){
        try{
            this.globalNodeTable.put(name, node);
            return true;
        } catch (Exception e){
            return false;
        }
    }

    public static void setReplica(Long numReplica) {
        NodeCluster.replicas = numReplica;
    }

    public static void setHashRange(Long range) {
        NodeCluster.hashRange = range;
    }

    public static void setMinCopy(Long minCopy){
        NodeCluster.minCopy = Math.min(minCopy, replicas);
    }

    public static Long getHashRange() {
        return hashRange;
    }

    public static Long getReplica() {
        return replicas;
    }

    public static Long getMinCopy() {
        return minCopy;
    }
}
