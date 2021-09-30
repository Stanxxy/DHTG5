package com.company.Cassandra;

import com.company.BasicDHT;
import com.company.Commons.DataObjPair;
import com.company.Commons.NodeFactory;
import com.company.NodeManager;
import com.company.Utils.RingHashTools;
import java.util.Arrays;
import java.util.List;
import java.util.Random;


// CaDHT is the client of Cassandra DHT
public class CaDHT implements BasicDHT, NodeManager {

    Random r = new Random();
    CaCluster caCluster;
    String type;

    public CaDHT(CaCluster cluster) {
        this.caCluster = cluster;
        this.type = "Ca";
    }

    private String randomNodeSelect(){
        // randomly return the node name
        int size = this.caCluster.getNameList().size();
        return this.caCluster.getNameList().get(r.nextInt(size - 1));
    }

    @Override
    public String getName() {
         return CaDHT.class.getSimpleName();
    }

    @Override
    public boolean insert(Long key, String value) {
        String nodeName = randomNodeSelect();
        try{
            caCluster.getGlobalNodeTable().get(nodeName).insertData(key, value);
            return true;
        } catch (Exception e){
            return false;
        }
    }

    @Override
    public DataObjPair select(Long key) {
        String nodeName = randomNodeSelect();
        try{
            return caCluster.getGlobalNodeTable().get(nodeName).selectData(key);
        } catch (Exception e){
            return null;
        }
    }

    @Override
    public boolean update(Long key, String value) {
        String nodeName = randomNodeSelect();
        try{
            caCluster.getGlobalNodeTable().get(nodeName).updateData(key, value);
            return true;
        } catch (Exception e){
            return false;
        }
    }

    @Override
    public boolean delete(Long key) {
        String nodeName = randomNodeSelect();
        try{
            caCluster.getGlobalNodeTable().get(nodeName).deleteData(key);
            return true;
        } catch (Exception e){
            return false;
        }
    }

    @Override
    public String listNodeData(String nodeName) {
        StringBuilder sb;
        sb = new StringBuilder();
        sb.append("=========================\n");
        sb.append("Node: " + nodeName + "\n");
        List<List<DataObjPair>> dataView = caCluster.getGlobalNodeTable().get(nodeName).viewData();
        sb.append("stored data: \n");
        for(DataObjPair data : dataView.get(0)){
            sb.append(data + "\n");
        }
        sb.append("replica: \n");
        for(DataObjPair data : dataView.get(1)){
            sb.append(data + "\n");
        }
        return sb.toString();
    }

    @Override
    public String listNodeMeta(String nodeName) {
        StringBuilder sb;
        sb = new StringBuilder();
        sb.append("=========================\n");
        sb.append("Node: " + nodeName + "\n");
        Long hash = caCluster.getGlobalNodeTable().get(nodeName).getHashValue();
        sb.append(String.format("hash value: %d\n", hash));
        Long capacity = caCluster.getGlobalNodeTable().get(nodeName).getCapacity();
        sb.append(String.format("capacity: %d\n", capacity));
        Long load = caCluster.getGlobalNodeTable().get(nodeName).getLoad();
        sb.append(String.format("current load: %d\n", load));
        return sb.toString();
    }

    @Override
    public String debug() {
        return null;
    }

    @Override
    public String listAllNodes() {
        StringBuilder sb = new StringBuilder();
        this.caCluster.getNameList().forEach(e -> sb.append(e).append("\n"));
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    @Override
    public void addNode(String name) {
        Long hashValue = RingHashTools.hashNode(name, CaCluster.getHashRange());
        addNode(name, hashValue);
    }

    @Override
    public void addNode(String name, Long hashValue) {
        NodeFactory.generateNode(this.type, name, hashValue);
    }

    @Override
    public void removeNode(String name) {
        try{
            caCluster.getGlobalNodeTable().get(name).shutdownNode(false);
        } catch (Exception e){
            System.out.println("shutdown Fail.\n" + Arrays.toString(e.getStackTrace()));
        }
    }

    @Override
    public void unplugNode(String name) {
        try{
            caCluster.getGlobalNodeTable().get(name).shutdownNode(true);
        } catch (Exception e){
            System.out.println("shutdown Fail.\n" + Arrays.toString(e.getStackTrace()));
        }
    }

    @Override
    public void loadBalancing(String name) {
        try{
            caCluster.getGlobalNodeTable().get(name).loadBalancing();
        } catch (Exception e){
            System.out.println("shutdown Fail.\n" + Arrays.toString(e.getStackTrace()));
        }
    }
}
