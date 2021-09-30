package com.company.Cassandra;

import com.company.Commons.DataObjPair;
import com.company.Commons.Node;
import com.company.Utils.RingHashTools;

import javax.xml.crypto.Data;
import java.sql.SQLOutput;
import java.util.*;
import java.util.concurrent.*;

// this is the node it self.
// could be node service
public class CaNode extends Node {

    private String successor;

    private String predecessor;

    private CaCluster caCluster;

    private Map<Long, DataObjPair> dupStore;

    private Long hashValue;

    private Map<String, String[]> tableInfo;

    private ThreadPoolExecutor asyncPool;

    public CaNode(String name, Long hash, List<String> seeds) {


        this.name = name;
        this.load = 0L;
        this.caCluster = CaCluster.getCluster(); // static method, singleton

        this.hashValue = hash == null ? RingHashTools.hashNode(name, CaCluster.getHashRange()) : hash;

        this.successor = findSuccessor(seeds); // find successor node
        this.predecessor = findPredecessor(seeds); // find predecessor node

        calculateCapacity();

        this.storedData = new HashMap<>(); // initialize the stored data container
        this.dupStore = new HashMap<>(); // initialize the replica repo
        initDataAndDup();

        this.tableInfo = new HashMap<>();
        initTableInfo();

        this.asyncPool = new ThreadPoolExecutor(4, 8, 30,
                TimeUnit.SECONDS, new ArrayBlockingQueue<>(10000));
    }

    public Map<String, String[]> getTableInfo() {
        return tableInfo;
    }

    public Long getHashValue() {
        return hashValue;
    }

    private void calculateCapacity(){
        if(this.predecessor == null || callPredecessor() == null){
            String nearestAlivePred = callSuccessor() == null ? null :
                    caCluster.searchForTheNearestAlive(this.successor, this.successor, 0);
            this.capacity = nearestAlivePred == null ? CaCluster.getHashRange() :
                    RingHashTools.hashDistance(caCluster.getNodeHash(nearestAlivePred), hashValue, CaCluster.getHashRange());
        } else {
            this.capacity = (this.hashValue - callPredecessor().hashValue
                    + CaCluster.getHashRange()) % CaCluster.getHashRange();
        }
    }

    private String findSuccessor(List<String> seeds){
        // talk to the other nodes in the cluster to get successor
        // seeds provides the node with node ip that existed in the address
        return RingHashTools.findClosestSuccessor(seeds, this.hashValue);
    }

    private String findPredecessor(List<String> seeds){
        // talk to the other nodes in the cluster to get successor
        // seeds provides the node with node ip that existed in the address
        return RingHashTools.findClosestPredecessor(seeds, this.hashValue);
    }

    private CaNode callSuccessor(){
        return this.caCluster.getGlobalNodeTable().get(this.successor);
    }

    private CaNode callPredecessor(){
        return this.caCluster.getGlobalNodeTable().get(this.predecessor);
    }

    public List<List<DataObjPair>> viewData(){
        // dump all data
        // first is original data
        // second is duplicate
        List<List<DataObjPair>> dumpObj = new ArrayList<>(2);
        dumpObj.add(new ArrayList<>(this.storedData.values()));
        dumpObj.add(new ArrayList<>(this.dupStore.values()));
        return dumpObj;
    }

    private void initTableInfo(){

        this.caCluster.getGlobalNodeTable().put(this.name, this);
        for(Map.Entry<String, CaNode> entry : caCluster.getGlobalNodeTable().entrySet()){
            this.tableInfo.put(entry.getKey(),
                    new String[]{entry.getValue().predecessor, entry.getValue().successor});
        }
        caCluster.insertIntoTable(this.name, new String[]{this.predecessor, this.successor});
    }

    public void initDataAndDup(){
        if(this.successor == null){
            return; // the initial node
        }
        Long beginHash = (callPredecessor().hashValue + 1) % CaCluster.getHashRange();
        // tell predecessor the node is online
        callSuccessor().predecessor = this.name;
        // tell successor the node is online
        callPredecessor().successor = this.name;
        // load data from successor
        Long moveNum = this.loadData(this, beginHash, hashValue, this.successor);
        // remove data from successor
        Long removeNum = callSuccessor() == null ? 0 : callSuccessor().dumpData(callSuccessor(), beginHash, hashValue);

        if(callSuccessor() != null){
            // load replica into successor from its successor
            callSuccessor().loadDup(callSuccessor(), beginHash, hashValue, callSuccessor().successor, CaCluster.getReplica());
            // remove replica from the very last node
            callSuccessor().dumpDup(callSuccessor(), beginHash, hashValue, CaCluster.getReplica());
        }
        // increase the node load
        this.load += moveNum;
        if(callSuccessor() != null){
            // decrease the successor load
            callSuccessor().load -= removeNum;
            // update capacity of successor
            callSuccessor().capacity -= RingHashTools.hashDistance(beginHash, hashValue, CaCluster.getHashRange());
        }
    }

    public void shutdownNode(boolean force) throws InterruptedException, ExecutionException, TimeoutException {
        if(this.predecessor == null){
            return; // the last node
        }
        if(force){
            caCluster.getGlobalNodeTable().remove(this.name); // denotes the node has died

            if(callSuccessor() != null){
                callSuccessor().predecessor = null;
            }
            if(callPredecessor() != null){
                callPredecessor().successor = null;
            }
            String nearestAlivePred = caCluster.searchForTheNearestAlive(this.successor, this.name, 0);
            String nearestAliveSuc = caCluster.searchForTheNearestAlive(this.predecessor, this.name, 1);
            if(callSuccessor() != null){
                callSuccessor().predecessor = callSuccessor().name.equals(nearestAlivePred) ? null : nearestAlivePred;
            }
            if(callPredecessor() != null){
                callPredecessor().successor = callPredecessor().name.equals(nearestAliveSuc) ? null : nearestAliveSuc;
            }
            // then do data movement
            if(callSuccessor() != null){
                if(nearestAlivePred == null){ // only one single node left
                    Long moveNum = 0L;
                    for(Map.Entry<Long, DataObjPair> entry : callSuccessor().dupStore.entrySet()){
                        if(!callSuccessor().storedData.containsKey(entry.getKey())){
                            moveNum++;
                            callSuccessor().insertData(entry.getKey(), entry.getValue().getValue());
                        }
                    }
                    callSuccessor().dupStore.clear(); // no need to replicate anything
                    callSuccessor().load += moveNum;
                    callSuccessor().capacity = CaCluster.getHashRange();
                } else { // if we have multiple nodes
                    Long beginHash = caCluster.getGlobalNodeTable().get(nearestAlivePred).hashValue + 1;
                    Long endHash = callSuccessor().hashValue;
                    // do the recovery we do not take care of replica
                    Long moveNum = 0L;
                    if(beginHash > endHash){
                        moveNum += filteredMapSync(callSuccessor().dupStore,
                                callSuccessor().storedData,
                                beginHash,
                                CaCluster.getHashRange() - 1);
                        moveNum += filteredMapSync(callSuccessor().dupStore,
                                callSuccessor().storedData,
                                0L,
                                beginHash);
                    } else {
                        moveNum += filteredMapSync(callSuccessor().dupStore,
                                callSuccessor().storedData,
                                beginHash,
                                endHash);
                    }

                    if(callSuccessor().callSuccessor() != null){
                        // the very last node load replica from the predecessor
                        callSuccessor().callSuccessor().loadDup(
                                callSuccessor().callSuccessor(), beginHash, endHash, this.successor,
                                Math.max(CaCluster.getReplica() -1, 0));
                        // remove the replica data from successor
                        callSuccessor().dumpDup(callSuccessor(), beginHash, endHash, 0L);
                    }

                    callSuccessor().capacity = RingHashTools.hashDistance(beginHash, endHash, CaCluster.getHashRange());
                    callSuccessor().load += moveNum;
                }
            }
        } else {
            // find the begin hash of current node
            Long beginHash = callSuccessor() == null ? 0 : (callPredecessor().hashValue + 1) % CaCluster.getHashRange();
            // successor loads data from current node
            Long moveNum = callSuccessor() == null ? 0 : callSuccessor().loadData(callSuccessor(), beginHash, hashValue, this.name);
            // dump data from current node
            Long removeNum = this.dumpData(this, beginHash, hashValue);

            if(this.callSuccessor() != null){
                // the very last node load replica from the predecessor
                callSuccessor().loadDup(callSuccessor(), beginHash, hashValue, this.name, CaCluster.getReplica());
                // remove the replica data from successor
                callSuccessor().dumpDup(callSuccessor(), beginHash, hashValue, 0L);
                // increase the node load
                callSuccessor().load += moveNum;
            }
            // decrease the successor load
            this.load -= removeNum;
            if(callSuccessor() != null){
                // update capacity of successor
                callSuccessor().capacity += RingHashTools.hashDistance(beginHash, hashValue, CaCluster.getHashRange());
                // tell predecessor the node is offline
                callSuccessor().predecessor = this.predecessor;
            }
            if(this.callPredecessor() != null){
                // tell successor the node is offline
                callPredecessor().successor = this.successor;
            }
            // officially get the current node offline
            caCluster.getGlobalNodeTable().remove(this.name);
        }
        caCluster.removeFromNodeTable(this.name);
    }

    public void loadBalancing(){
        // only do load balancing current node with its predecessor or successor
        // first calculate the load factor
        double preLoadFactor = (double) callPredecessor().load / (double) callPredecessor().capacity;
        double sucLoadFactor = (double) callSuccessor().load / (double) callSuccessor().capacity;
        double curLoadFactor = (double) load / (double) capacity;
        // compare if we can do load balance with neighbour
        if(curLoadFactor <= Math.min(preLoadFactor, sucLoadFactor)){
            // two wins are even more crowded, then we give up.
            return;
        }
        // if not, then do load balance with the one with less load
        if(preLoadFactor < sucLoadFactor){
            // predecessor move forward
            List<Long> allKeys = new ArrayList<>(this.storedData.keySet());
            Collections.sort(allKeys);
            List<Long> allKeysPre = new ArrayList<>(callPredecessor().storedData.keySet());
            Collections.sort(allKeysPre);
            // find out all keys and find the hash position
            allKeysPre.addAll(allKeys);
            int load = allKeysPre.size() / 2;
            Long endHash = allKeysPre.get(load);
            // begin to move
            this.movePredecessorForward(endHash);
        } else {
            // current node move backward
            List<Long> allKeys = new ArrayList<>(this.storedData.keySet());
            Collections.sort(allKeys);
            List<Long> allKeysSuc = new ArrayList<>(callSuccessor().storedData.keySet());
            Collections.sort(allKeysSuc);
            // find out all keys and find the hash position
            allKeys.addAll(allKeysSuc);
            int load = allKeys.size() / 2;
            Long endHash = allKeys.get(load);
            // begin to move
            this.moveCurrentBackward(endHash);
        }
    }

    private void movePredecessorForward(Long hash){
        if(predecessor == null){
            return; // cannot do any load balancing
        }
        // predecessor loads data from current
        Long moveNum = callPredecessor().loadData(callSuccessor(), callPredecessor().hashValue, hash, this.name);
        // dump data from current node
        Long removeNum = this.dumpData(this, callPredecessor().hashValue, hashValue);
        // load replica into current node
        this.loadDup(this, callPredecessor().hashValue, hash, this.successor, CaCluster.getReplica());
        // remove replica from the current node
        this.dumpDup(this, callPredecessor().hashValue, hash, CaCluster.getReplica());
        // increase the load of predecessor
        callPredecessor().load += moveNum;
        // decrease the load of current node
        this.load -= removeNum;
        // increase the capacity of predecessor
        callPredecessor().capacity += RingHashTools.hashDistance(callPredecessor().hashValue, hash, CaCluster.getHashRange()) - 1;
        // decrease the capacity of current node
        this.capacity -= RingHashTools.hashDistance(callPredecessor().hashValue, hash, CaCluster.getHashRange()) - 1;
        // update the hash value of predecessor
        callPredecessor().hashValue = hash;

    }

    private void moveCurrentBackward(Long hash){
        if(this.successor == null){
            return; // cannot do any load balancing
        }
        // successor loads data from current node
        Long moveNum = callSuccessor().loadData(callSuccessor(), hash, hashValue, this.name);
        // dump data from current node
        Long removeNum = this.dumpData(this, hash, hashValue);
        // the very last node load replica from the predecessor
        callSuccessor().loadDup(callSuccessor(), hash, hashValue, this.name, CaCluster.getReplica());
        // remove the replica data from successor
        callSuccessor().dumpDup(callSuccessor(), hash, hashValue, 0L);
        // increase the node load
        callSuccessor().load += moveNum;
        // decrease the successor load
        this.load -= removeNum;
        // update capacity of successor
        callSuccessor().capacity += RingHashTools.hashDistance(hash, hashValue, CaCluster.getHashRange()) - 1;
        // decrease the capacity of current node
        this.capacity -= RingHashTools.hashDistance(hash, hashValue, CaCluster.getHashRange()) - 1;
        // update the hash value of current node
        this.hashValue = hash;
    }


    public Long loadData(CaNode node, Long beginHash, Long endHash, String source){
        Long counter = 0L;
        if(source.equals(node.successor)) {
            // beginHash and endHash should be inclusive
            if(node.callSuccessor() == null){
                return 0L;
            }
            Map<Long, DataObjPair> sucMap = node.callSuccessor().storedData;
            System.out.println(String.format("The following data copied from %s to %s ", source, node.getName()));
            System.out.println("-----------------------------------------------");
            if(endHash < beginHash){
                // values in [beginHash, maxHash] /cup [0, endHash]
                counter += filteredMapSync(sucMap, node.storedData, beginHash, CaCluster.getHashRange() - 1);
                counter += filteredMapSync(sucMap, node.storedData, 0L, endHash);
            } else {
                counter += filteredMapSync(sucMap, node.storedData, beginHash, endHash);
            }
        } else if (source.equals(node.predecessor)){
            if(node.callPredecessor() == null){
                return 0L;
            }
            Map<Long, DataObjPair> predMap = node.callPredecessor().storedData;
            System.out.println(String.format("The following data copied from %s to %s ", source, node.getName()));
            System.out.println("-----------------------------------------------");
            if(endHash < beginHash){
                // values in [beginHash, maxHash] /cup [0, endHash]
                counter += filteredMapSync(predMap, node.storedData, beginHash, CaCluster.getHashRange() - 1);
                counter += filteredMapSync(predMap, node.storedData, 0L, endHash);
            } else {
                counter += filteredMapSync(predMap, node.storedData, beginHash, endHash);
            }
        }
        return counter;
    }

    public void loadDup(CaNode node, Long beginHash, Long endHash, String source, Long dupLeft){
        if(dupLeft < 0){
            // don't repeat yourself
            return;
        }

        if(source.equals(node.successor)){
            if(node.callSuccessor() == null || node.storedData.containsKey(beginHash)){
                return;
            }
            if(dupLeft.equals(CaCluster.getReplica())){
                Map<Long, DataObjPair> sucMap = node.callSuccessor().dupStore;
                System.out.println(String.format("The following replica copied from %s to %s ", source, node.getName()));
                System.out.println("-----------------------------------------------");
                if(endHash < beginHash){
                    // values in [beginHash, maxHash] /cup [0, endHash]
                    node.filteredMapSync(sucMap, node.dupStore, beginHash, CaCluster.getHashRange() - 1);
                    node.filteredMapSync(sucMap, node.dupStore, 0L, endHash);
                } else {
                    node.filteredMapSync(sucMap, node.dupStore, beginHash, endHash);
                }
            }
            // beginHash and endHash should be inclusive
        } else if (source.equals(node.predecessor)){
            if(node.callPredecessor() == null){
                return;
            }
            if(node.storedData.containsKey(beginHash) || dupLeft > 0){
                if(node.callSuccessor() != null){
                    node.loadDup(node.callSuccessor(), beginHash, endHash, node.callSuccessor().predecessor, dupLeft - 1);
                }
            } else {
                Map<Long, DataObjPair> predMap = node.callPredecessor().dupStore;
                System.out.println(String.format("The following replica copied from %s to %s ", source, node.getName()));
                System.out.println("-----------------------------------------------");
                if(endHash < beginHash){
                    // values in [beginHash, maxHash] /cup [0, endHash]
                    node.filteredMapSync(predMap, node.dupStore, beginHash, CaCluster.getHashRange() - 1);
                    node.filteredMapSync(predMap, node.dupStore, 0L, endHash);
                } else {
                    node.filteredMapSync(predMap, node.dupStore, beginHash, endHash);
                }
                System.out.println("-----------------------------------------------");
            }
        }
    }

    private Long filteredMapSync(Map<Long, DataObjPair> oriMap,
                                 Map<Long, DataObjPair> newMap,
                                 Long beginHash, Long endHash){
        // we may use logger to tract the data movement
        Long counter = 0L;
        for(long i : oriMap.keySet()){
            // beginHash and endHash is inclusive
            if(i >= beginHash && i <= endHash && !newMap.containsKey(i)){
                counter++;
                DataObjPair obj = oriMap.get(i);
                System.out.println(obj);
                newMap.put(obj.getKey(), obj);
            }
        }
        return counter;
    }

    public Long dumpData(CaNode node, Long beginHash, Long endHash){
        Long counter = 0L;
        System.out.println(String.format("The following data removed from %s", node.getName()));
        System.out.println("-----------------------------------------------");
        if(endHash < beginHash){
            // values in [beginHash, maxHash] /cup [0, endHash]
            counter += node.filteredMapRemove(node.storedData, beginHash, CaCluster.getHashRange() - 1);
            counter += node.filteredMapRemove(node.storedData, 0L, endHash);
        } else {
            counter += node.filteredMapRemove(node.storedData, beginHash, endHash);
        }
        System.out.println("-----------------------------------------------");
        return counter;
    }

    public void dumpDup(CaNode node, Long beginHash, Long endHash, Long dupLeft){
        if(dupLeft == 0){
            System.out.println(String.format("The following replica removed from %s", node.getName()));
            System.out.println("-----------------------------------------------");
            // from successor
            if(endHash < beginHash){
                // values in [beginHash, maxHash] /cup [0, endHash]
                node.filteredMapRemove(node.dupStore, beginHash, CaCluster.getHashRange() - 1);
                node.filteredMapRemove(node.dupStore, 0L, endHash);
            } else {
                node.filteredMapRemove(node.dupStore, beginHash, endHash);
            }
            System.out.println("-----------------------------------------------");
        } else {
            if(node.callSuccessor() == null){
                return;
            }
            node.callSuccessor().dumpDup(node.callSuccessor(), beginHash, endHash, dupLeft - 1);
        }
    }

    private Long filteredMapRemove(Map<Long, DataObjPair> oriMap,
                             Long beginHash, Long endHash) {
        // Same as filteredMap we may use logger to tract the data movement.
        Long counter = 0L;
        Iterator<Map.Entry<Long, DataObjPair>> itor = oriMap.entrySet().iterator();
        while(itor.hasNext()){
            Map.Entry<Long, DataObjPair> entry = itor.next();
            if(entry.getKey() >= beginHash && entry.getKey() <= endHash){
                counter++;
                System.out.println(entry.getValue());
                itor.remove();
            }
        }
        return counter;
    }

    public int insertData(Long key, String value) throws ExecutionException, InterruptedException, TimeoutException {
        if(key >= CaCluster.getHashRange()){
            return -1; // error
        }
        if(RingHashTools.inCurrent(caCluster.getNodeHash(this.predecessor), this.hashValue, key)){
            // simply overwrite the previous data. Here we mimic Redis
            if(this.storedData.containsKey(key)){
                return 1; // key already exist
            }
            DataObjPair obj = new DataObjPair(key, value, 0L);
            System.out.println(String.format("Data %s inserted in  %s", obj, this.getName()));
            this.storedData.put(key, obj);
            this.load++;
            // also do duplication
            return insertDup(key, value, CaCluster.getReplica());
        } else {
            System.out.println(String.format("Sent to next node %s", callSuccessor().name));
            return callSuccessor().insertData(key, value);
        }
    }

    class InsertDupTask implements Callable<Integer> {
        Long key;
        String value;
        Long dupLeft;
        CaNode node;

        InsertDupTask(Long key, String value, Long dupLeft, CaNode node){
            this.key = key;
            this.value = value;
            this.dupLeft = dupLeft;
            this.node = node;
        }

        @Override
        public Integer call(){
            try{
                if(node == null || dupLeft == 0 || node.storedData.containsKey(key)){
                    // go to the self node or have established data
                    return 0;
                }
                DataObjPair obj = new DataObjPair(key, value, CaCluster.getReplica() - dupLeft + 1);
                System.out.println(String.format("Replica %s inserted in  %s", obj, this.node.getName()));
                node.dupStore.put(key, obj);
                return callSuccessor().insertDup(key, value, dupLeft - 1);
            } catch (Exception e){
                System.out.println(Arrays.toString(e.getStackTrace()));
                return -1;
            }
        }
    }

    private int insertDup(Long key, String value, Long dupLeft) throws ExecutionException, InterruptedException, TimeoutException {
        FutureTask<Integer> ft = new FutureTask<>(new InsertDupTask(key, value, dupLeft, callSuccessor()));
        asyncPool.execute(ft);
        if(dupLeft <= CaCluster.getReplica() - CaCluster.getMinCopy()){
            return 0;
        } else {
            return ft.get(10L, TimeUnit.SECONDS);
        }
    }

    public DataObjPair selectData(Long key){
        try{
            if(key >= CaCluster.getHashRange()){
                return null; // error
            }
            if(RingHashTools.inCurrent(caCluster.getNodeHash(this.predecessor), this.hashValue, key)){
                // simply overwrite the previous data. Here we mimic Redis
                System.out.println(String.format("Retrieved from original data %s", this.getName()));
                return this.storedData.get(key);
            } else {
                if(this.dupStore.containsKey(key)){
                    System.out.println(String.format("Retrieved from replica data %s", this.getName()));
                    return this.dupStore.get(key);
                }
                System.out.println(String.format("Sent to next node %s", callSuccessor().name));
                return callSuccessor().selectData(key);
            }
        } catch (Exception e){
            System.out.println(Arrays.toString(e.getStackTrace()));
            return null;
        }
    }

    public int updateData(Long key, String value) throws ExecutionException, InterruptedException, TimeoutException {
        if(key >= CaCluster.getHashRange()){
            return -1; // error
        }
        if(RingHashTools.inCurrent(caCluster.getNodeHash(this.predecessor), this.hashValue, key)){
            // simply overwrite the previous data. Here we mimic Redis
            if(this.storedData.get(key) != null){
                DataObjPair obj = new DataObjPair(key, value, 0L);
                System.out.println(String.format("Data %s updated in  %s", obj, this.getName()));
                this.storedData.put(key, obj);
                // update dups
                return updateDup(key, value, CaCluster.getReplica());
            } else {
                System.out.println(String.format("No such data %s", key));
                return 1;
            }
        } else {
            System.out.println(String.format("Sent to next node %s", callSuccessor().name));
            return callSuccessor().updateData(key, value);
        }
    }

    class UpdateDupTask implements Callable<Integer> {
        Long key;
        String value;
        Long dupLeft;
        CaNode node;

        UpdateDupTask(Long key, String value, Long dupLeft, CaNode node){
            this.key = key;
            this.value = value;
            this.dupLeft = dupLeft;
            this.node = node;
        }

        @Override
        public Integer call(){
            try{
                if(node == null || dupLeft == 0){
                    // finished replica
                    return 0;
                } else if (node.storedData.containsKey(key)){
                    // go to the self node
                    return 0;
                }
                if(node.dupStore.containsKey(key)){
                    DataObjPair obj = new DataObjPair(key, value,  node.dupStore.get(key).getReplicaI());
                    System.out.println(String.format("Replica %s updated in  %s", obj, node.getName()));
                    node.dupStore.put(key, obj);
                    dupLeft -= 1;
                }
                if(callSuccessor() != null){
                    return callSuccessor().updateDup(key, value, dupLeft);
                }
                return 0;
            } catch (Exception e){
                System.out.println(Arrays.toString(e.getStackTrace()));
                return -1;
            }
        }
    }

    private int updateDup(Long key, String value, Long dupLeft) throws ExecutionException, InterruptedException, TimeoutException {
        FutureTask<Integer> ft = new FutureTask<>(new UpdateDupTask(key, value, dupLeft, callSuccessor()));
        asyncPool.execute(ft);
        if(dupLeft < CaCluster.getReplica() - CaCluster.getMinCopy()){
            return 0;
        } else {
            return ft.get(10L, TimeUnit.SECONDS);
        }
    }

    public int deleteData(Long key) throws ExecutionException, InterruptedException, TimeoutException {
        if(key >= CaCluster.getHashRange()){
            return -1; // error
        }
        if(RingHashTools.inCurrent(caCluster.getNodeHash(this.predecessor), this.hashValue, key)){
            // simply overwrite the previous data. Here we mimic Redis
            System.out.println(String.format("Data %s removed from  %s", this.storedData.get(key), this.getName()));
            DataObjPair obj = this.storedData.remove(key);
            if(obj != null) {
                this.load--;
                return deleteDup(key, CaCluster.getReplica());
            }
            return 1; // 1 denotes key doesn't exist
        } else {
            System.out.println(String.format("Sent to next node %s", callSuccessor().name));
            return callSuccessor().deleteData(key);
        }
    }

    class DeleteDupTask implements Callable<Integer> {
        Long key;
        Long dupLeft;
        CaNode node;

        DeleteDupTask(Long key, Long dupLeft, CaNode node){
            this.key = key;
            this.dupLeft = dupLeft;
            this.node = node;
        }

        @Override
        public Integer call(){
            try{
                if(node == null || dupLeft == 0){
                    // finished replica
                    return 0;
                } else if (RingHashTools.inCurrent(caCluster.getNodeHash(node.predecessor), node.hashValue, key)){
                    // go to the self node
                    return 0;
                }
                if(node.dupStore.containsKey(key)){
                    System.out.println(String.format("Replica %s removed from  %s", node.dupStore.get(key), node.getName()));
                    node.dupStore.remove(key);
                    dupLeft -= 1;
                }
                if(callSuccessor() != null){
                    return callSuccessor().deleteDup(key, dupLeft);
                }
                return 0;
            } catch (Exception e){
                System.out.println(Arrays.toString(e.getStackTrace()));
                return -1;
            }
        }
    }

    private int deleteDup(Long key, Long dupLeft) throws ExecutionException, InterruptedException, TimeoutException {
        FutureTask<Integer> ft = new FutureTask<>(new DeleteDupTask(key, dupLeft, callSuccessor()));
        asyncPool.execute(ft);
        if(dupLeft < CaCluster.getReplica() - CaCluster.getMinCopy()){
            return 0;
        } else {
            return ft.get(10L, TimeUnit.SECONDS);
        }
    }
}
