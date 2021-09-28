package com.company.Cassandra;

import com.company.Commons.DataObjPair;
import com.company.Commons.Node;
import com.company.Utils.RingHashTools;

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
        this.hashValue = hash == null ? RingHashTools.hashNode(name, CaCluster.getHashRange()) : hash;

        this.successor = findSuccessor(seeds); // find successor node
        this.predecessor = findPredecessor(seeds); // find predecessor node

        this.storedData = new HashMap<>(); // load data from previous node
        this.dupStore = new HashMap<>(); // load dup from previous node
        initDataAndDup();

        this.caCluster = CaCluster.getCluster(); // static method, singleton
        this.capacity = this.predecessor == null ? CaCluster.getHashRange() :
                (this.hashValue - callPredecessor().hashValue
                        + CaCluster.getHashRange()) % CaCluster.getHashRange();
        initTableInfo();

        this.asyncPool = new ThreadPoolExecutor(4, 8, 30,
                TimeUnit.SECONDS, new ArrayBlockingQueue<>(10000));
    }

    public Map<String, String[]> getTableInfo() {
        return tableInfo;
    }

    public String getSuccessor() {
        return successor;
    }

    public String getPredecessor(){
        return predecessor;
    }

    public Long getHashValue() {
        return hashValue;
    }

    public CaCluster getCaCluster() {
        return caCluster;
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
        // load data from successor
        Long moveNum = this.loadData(beginHash, hashValue, this.successor);
        // remove data from successor
        Long removeNum = callSuccessor().dumpData(beginHash, hashValue);
        // load replica into successor from its successor
        callSuccessor().loadDup(beginHash, hashValue, callSuccessor().successor, CaCluster.getReplica());
        // remove replica from the very last node
        callSuccessor().dumpDup(beginHash, hashValue, CaCluster.getReplica());
        // increase the node load
        this.load += moveNum;
        // decrease the successor load
        callSuccessor().load -= removeNum;
        // update capacity of successor
        callSuccessor().capacity -= RingHashTools.hashDistance(beginHash, hashValue, CaCluster.getHashRange());
        // tell predecessor the node is online
        callSuccessor().predecessor = this.name;
        // tell successor the node is online
        callPredecessor().successor = this.name;
    }

    public void shutdownNode(boolean force) throws InterruptedException, ExecutionException, TimeoutException {
        if(this.predecessor == null){
            return; // the last node
        }
        if(force){
            caCluster.getGlobalNodeTable().remove(this.name); // denotes the node has died
            String nearestAlivePred = caCluster.searchForTheNearestAlive(callSuccessor().name, 0);
            String nearestAliveSuc = caCluster.searchForTheNearestAlive(callPredecessor().name, 1);
            callSuccessor().predecessor = callSuccessor().name.equals(nearestAlivePred) ? null : nearestAlivePred;
            callPredecessor().successor = callPredecessor().name.equals(nearestAliveSuc) ? null : nearestAliveSuc;
            // then do data movement
            if(nearestAlivePred == null){ // only a single node is left
                for(Map.Entry<Long, DataObjPair> entry : callSuccessor().dupStore.entrySet()){
                    callSuccessor().insertData(entry.getKey(), entry.getValue().getValue());
                }
                callSuccessor().dupStore.clear(); // no need to dup anything
            } else { // if we have multiple nodes
                Long startHash = caCluster.getGlobalNodeTable().get(nearestAlivePred).hashValue;
                Long endHash = callSuccessor().hashValue;
                // do the recovery we do not take care of replica
                filteredMapSync(callSuccessor().dupStore,
                        callSuccessor().storedData,
                        startHash,
                        endHash);
            }
        } else {
            // find the begin hash of current node
            Long beginHash = (callPredecessor().hashValue + 1) % CaCluster.getHashRange();
            // successor loads data from current node
            Long moveNum = callSuccessor().loadData(beginHash, hashValue, this.name);
            // dump data from current node
            Long removeNum = this.dumpData(beginHash, hashValue);
            // the very last node load replica from the predecessor
            callSuccessor().loadDup(beginHash, hashValue, this.name, CaCluster.getReplica());
            // remove the replica data from successor
            callSuccessor().dumpDup(beginHash, hashValue, 1L);
            // increase the node load
            callSuccessor().load += moveNum;
            // decrease the successor load
            this.load -= removeNum;
            // update capacity of successor
            callSuccessor().capacity += RingHashTools.hashDistance(beginHash, hashValue, CaCluster.getHashRange());
            // tell predecessor the node is offline
            callSuccessor().predecessor = this.predecessor;
            // tell successor the node is offline
            callPredecessor().successor = this.successor;
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
        if(curLoadFactor >= Math.min(preLoadFactor, sucLoadFactor)){
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
        Long moveNum = callPredecessor().loadData(callPredecessor().hashValue, hash, this.name);
        // dump data from current node
        Long removeNum = this.dumpData(callPredecessor().hashValue, hashValue);
        // load replica into current node
        this.loadDup(callPredecessor().hashValue, hash, this.successor, CaCluster.getReplica());
        // remove replica from the current node
        this.dumpDup(callPredecessor().hashValue, hash, CaCluster.getReplica());
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
        Long moveNum = callSuccessor().loadData(hash, hashValue, this.name);
        // dump data from current node
        Long removeNum = this.dumpData(hash, hashValue);
        // the very last node load replica from the predecessor
        callSuccessor().loadDup(hash, hashValue, this.name, CaCluster.getReplica());
        // remove the replica data from successor
        callSuccessor().dumpDup(hash, hashValue, 1L);
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


    public Long loadData(Long beginHash, Long endHash, String source){
        Long counter = 0L;
        if(source.equals(this.successor)) {
            // beginHash and endHash should be inclusive
            Map<Long, DataObjPair> sucMap = callSuccessor().storedData;
            if(endHash < beginHash){
                // values in [beginHash, maxHash] /cup [0, endHash]
                counter += filteredMapSync(sucMap, this.storedData, beginHash, CaCluster.getHashRange() - 1);
                counter += filteredMapSync(sucMap, this.storedData, 0L, endHash);
            } else {
                counter += filteredMapSync(sucMap, this.storedData, beginHash, endHash);
            }
        } else if (source.equals(this.predecessor)){
            Map<Long, DataObjPair> predMap = callPredecessor().storedData;
            if(endHash < beginHash){
                // values in [beginHash, maxHash] /cup [0, endHash]
                counter += filteredMapSync(predMap, this.storedData, beginHash, CaCluster.getHashRange() - 1);
                counter += filteredMapSync(predMap, this.storedData, 0L, endHash);
            } else {
                counter += filteredMapSync(predMap, this.storedData, beginHash, endHash);
            }
        }
        return counter;
    }

    public void loadDup(Long beginHash, Long endHash, String source, Long dupLeft){
        if(this.storedData.containsKey(beginHash)){
            // don't repeat yourself
            return;
        }

        if(source.equals(this.successor)){
            if(dupLeft.equals(CaCluster.getReplica())){
                Map<Long, DataObjPair> sucMap = callSuccessor().dupStore;
                if(endHash < beginHash){
                    // values in [beginHash, maxHash] /cup [0, endHash]
                    filteredMapSync(sucMap, this.dupStore, beginHash, CaCluster.getHashRange() - 1);
                    filteredMapSync(sucMap, this.dupStore, 0L, endHash);
                } else {
                    filteredMapSync(sucMap, this.dupStore, beginHash, endHash);
                }
            }
            // beginHash and endHash should be inclusive
        } else if (source.equals(this.predecessor)){
            if(dupLeft == 1){
                Map<Long, DataObjPair> predMap = callPredecessor().dupStore;
                if(endHash < beginHash){
                    // values in [beginHash, maxHash] /cup [0, endHash]
                    filteredMapSync(predMap, this.dupStore, beginHash, CaCluster.getHashRange() - 1);
                    filteredMapSync(predMap, this.dupStore, 0L, endHash);
                } else {
                    filteredMapSync(predMap, this.dupStore, beginHash, endHash);
                }
            } else {
                loadDup(beginHash, endHash, callPredecessor().predecessor, dupLeft - 1);
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
            if(i >= beginHash && i <= endHash){
                counter++;
                DataObjPair obj = oriMap.get(i);
                newMap.put(obj.getKey(), obj);
            }
        }
        return counter;
    }

    public Long dumpData(Long beginHash, Long endHash){
        return filteredMapRemove(this.storedData, beginHash, endHash);
    }

    public void dumpDup(Long beginHash, Long endHash, Long dupLeft){
        if(dupLeft == 1){
            // from successor
            callSuccessor().filteredMapRemove(callSuccessor().dupStore, beginHash, endHash);
        } else {
            callSuccessor().dumpDup(beginHash, endHash, dupLeft - 1);
        }
    }

    private Long filteredMapRemove(Map<Long, DataObjPair> oriMap,
                             Long beginHash, Long endHash) {
        // Same as filteredMap we may use logger to tract the data movement.
        Long counter = 0L;
        for(long i : oriMap.keySet()){
            if(i >= beginHash && i <= endHash){
                counter++;
                oriMap.remove(i);
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
            this.storedData.put(key, new DataObjPair(key, value));
            this.load++;
            // also do duplication
            return insertDup(key, value, CaCluster.getReplica());
        } else {
            System.out.println(String.format("Sent to next node {}", callSuccessor().name));
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
                node.dupStore.put(key, new DataObjPair(key, value));
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
                return this.storedData.get(key);
            } else {
                if(this.dupStore.containsKey(key)){
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
                this.storedData.put(key, new DataObjPair(key, value));
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
                    node.dupStore.put(key, new DataObjPair(key, value));
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
