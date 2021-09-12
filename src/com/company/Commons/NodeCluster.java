package com.company.Commons;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NodeCluster <NType> {

    Map<String, NType> globalNodeTable;

    public NodeCluster(Map<String, NType> hashMap) {
        this.globalNodeTable = hashMap;
    }

    public Map<String, NType> getGlobalNodeTable(){
        return this.globalNodeTable;
    }
}
