package com.company.Commons;

import java.nio.channels.NotYetBoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NodeCluster<NType> {

    protected Map<String, NType> globalNodeTable; // now string is the string hash value.

    public NodeCluster(Map<String, NType> hashMap) {
        this.globalNodeTable = hashMap;
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
}
