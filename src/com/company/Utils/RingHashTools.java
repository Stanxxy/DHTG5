package com.company.Utils;

import com.company.Cassandra.CaCluster;
import java.util.*;

public class RingHashTools {

    @Deprecated
    public static Long hashNode(String string, Long space){
        Long hash = (long) string.hashCode();
        if(0 < hash || hash >= space){
            return null;
        }
        // calculate node hash here
        return hash % space;
    }

    public static Long hashData(Long hash, Long space){
        // calculate key hash here
        // currently we don't consider hash collision
        if(hash >= space){
            return null;
        }
        return hash % space;
    }

    public static String findClosestSuccessor(List<String> seeds, Long hash){
        if(seeds.isEmpty()){
            return null; // for empty table, simply return null
        }
        Map<Long, String> hashToName = new HashMap<>(seeds.size());
        seeds.forEach(e -> hashToName.put(CaCluster.getCluster().getNodeHash(e), e));
        List<Long> hashVals = new ArrayList<>(hashToName.keySet());
        Collections.sort(hashVals);
        Long suc = hashVals.get(0);
        if(hash < hashVals.get(hashVals.size() - 1)){
            for(int i=1;i<hashVals.size();i++){
                if(hash > hashVals.get(i-1) && hash < hashVals.get(i)){
                    suc = hashVals.get(i);
                    break;
                }
            }
        }
        return hashToName.get(suc);
    }

    public static String findClosestPredecessor(List<String> seeds, Long hash) {
        if (seeds.isEmpty()) {
            return null; // for empty table, simply return null
        }
        Map<Long, String> hashToName = new HashMap<>(seeds.size());
        seeds.forEach(e -> hashToName.put(CaCluster.getCluster().getNodeHash(e), e));
        List<Long> hashVals = new ArrayList<>(hashToName.keySet());
        Collections.sort(hashVals);
        Long pre = hashVals.get(seeds.size() - 1);
        if(hash > hashVals.get(0)){
            for (int i = seeds.size() - 2; i >= 0; i--) {
                if(hash < hashVals.get(i+1) && hash > hashVals.get(i)){
                    pre = hashVals.get(i);
                    break;
                }
            }
        }
        return hashToName.get(pre);
    }

    public static boolean inCurrent(Long pre, Long cur, Long hash){
        // because we use hash ring. we have to handle the end and begin of ring
        if(pre < cur){
            return hash > pre && hash <= cur;
        } else {
            // pre > r
            return hash > pre || hash <= cur;
        }
    }

    public static Long hashDistance(Long start, Long end, Long space){
        // what is the size of this interval. the start and end value is inclusive
        if(end > start){
            return end - start + 1;
        } else {
            return space - start + end + 1;
        }
    }
}
