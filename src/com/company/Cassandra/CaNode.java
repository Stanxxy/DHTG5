package com.company.Cassandra;

import com.company.Commons.DataObjPair;
import com.company.Commons.Node;
import com.company.Utils.RingHashTools;

import javax.xml.crypto.Data;
import java.util.HashMap;
import java.util.List;

public class CaNode extends Node {

    private String successor;

    private String predessor;

    private Long hashValue;

    private CaCluster caCluster;

    public CaNode(String ip, Long capacity, List<String> seeds) {
        this.ip = ip;
        this.capacity = capacity;
        this.hashValue = RingHashTools.calculateNodeHash(ip);
        this.storedData = new HashMap<>();
        this.successor = findSuccessor(seeds);
        this.caCluster = CaCluster.getCluster();
    }

    

    public String findSuccessor(List<String> seeds){
        // talk to the other nodes in the cluster to get successor
        // seeds provides the node with node ip that existed in the address
        String closestIp = "0.0.0.0";
        Long closestHash = RingHashTools.oneMoveBackward(this.caCluster.getHashRange(), this.hashValue);

        String tmpIP = closestIp;
        Long tmpHash = closestHash;
        for(String seed : seeds){
            tmpHash = this.caCluster.getGlobalNodeTable().get(seed).getHashValue();
            if(RingHashTools.isGreaterThan(this.caCluster.getHashRange(), this.hashValue, tmpHash)){
                // If this is true, we need to see if this is the close one
                if(RingHashTools.hashDistance(this.caCluster.getHashRange(), this.hashValue, tmpHash)
                        < RingHashTools.hashDistance(this.caCluster.getHashRange(), this.hashValue, closestHash){
                    closestIp
                }
            }
        }
        this.hashValue
    }

    private String findPredessor(){

    }

    public void retrieveCluster(){

    }

    public void moveForward(){

    }

    private void loadData(String ip){

    }

    private void dumpData(){

    }

    public Long getCapacity() {
        return capacity;
    }

    public String getSuccessor() {
        return successor;
    }

    public Long getHashValue() {
        return hashValue;
    }

    public CaCluster getCaCluster() {
        return caCluster;
    }

    public int insertData(String key, String value){
        if(this.storedData.size() < capacity){
            this.storedData.put(key, new DataObjPair(key, value));
            this.load++;
        } else {
            System.out.println("Data Overloaded, now do load balancing.");
            this
        }

    }

    public DataObjPair selectData(String key){

    }

    public int updateData(String key, String value){

    }

    public int deleteData(String key){
        if(load > 0){
            this.load--;
        }
    }
}
