package com.company.ceph;

import com.company.Commons.DataObjPair;
import com.company.Commons.Node;
import com.company.issuables.Insert;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class CeNode extends Node {
    private CeCluster cluster;

    private Long index;
    private Double weight;

    private ThreadPoolExecutor asyncPool;

    public CeNode(Long index, Double weight) {
        this.index = index;
        this.weight = weight;
        this.name = "Ce_Node-" + index;

        storedData = new HashMap<>();
        asyncPool = new ThreadPoolExecutor(
                4,
                8,
                30,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(10000)
        );
    }

    public boolean insert(DataObjPair data) {
        storedData.put(data.getKey(), data);
        return true;
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

    public Boolean issueInsert(DataObjPair obj) throws ExecutionException, InterruptedException, TimeoutException {
        Insert insertTask = new Insert(obj, this);
        FutureTask<Boolean> ft = new FutureTask<>(insertTask);

        asyncPool.execute(ft);
        return ft.get(10L, TimeUnit.SECONDS);
    }

    public String getMetaData() {
        StringBuilder sb = new StringBuilder();
        sb.append(getName() + "\n");
        sb.append("\tWeight: " + weight + "; Load: " + getLoad() + "\n");
        return sb.toString();
    }
}
