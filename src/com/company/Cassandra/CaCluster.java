package com.company.Cassandra;

import com.company.Commons.NodeCluster;

import java.util.HashMap;

public class CaCluster extends NodeCluster<CaNode> {

    private static CaCluster cluster;

    private Long hashRange;

    public void setHashRange(Long hashRange){
        this.hashRange = hashRange;
    }

    public Long getHashRange(){
        return this.hashRange;
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
