package com.company.ceph;

import com.company.Commons.DataObjPair;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class CephHashTools {
    private static MessageDigest md;

    public static void initialize() throws NoSuchAlgorithmException {
        md = MessageDigest.getInstance("SHA256");
    }

    public static CeNode computeDataLocation(CeCluster cluster, DataObjPair data) {
        Collection<CeNode> nodeCollection = cluster.getGlobalNodeTable().values();
        Stack<CeNode> nodes = new Stack<>();
        for(CeNode node : nodeCollection) {
            nodes.push(node);
        }

        System.out.println("Computing the CEPH data location for " + data);
        while(!nodes.isEmpty()) {
            CeNode location = nodes.peek();
            Long m = cluster.getM();
            Long x = data.getKey();
            Long r = data.getReplicaI();
            Long cid = location.getIndex();
            Double h = h(m, x, r, cid);

            Double w = location.getWeight();
            // use the remaining nodes to calculate the sum of weights
            Double sumOfWeights = sumOfWeights(nodes);
            Double weightRatio = nodes.size() == 0 ? 1 : w / sumOfWeights;

            System.out.println("\th(" + x + ", " + r + ", " + cid + ")=" + h + " - " + "weightRatio=" + weightRatio);

            if(h < weightRatio) {
                System.out.println("\th(x, r, cid)<weightRatio -> TRUE! Selecting Node<" + location.getIndex() + ">");
                return location;
            }
            nodes.pop();
        }
        return null; // how did we end up here?
    }

    // awesome hash function!
    public static Double h(Long m, Long x, Long r, Long cid) {
        int space = (int)Math.pow(2, m);
        int hashFunctionSpace = 32; // since we're using SHA-256
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);

        // first, concat x, r, cid and put hash them
        Long toHash = Long.parseLong("" + x + "" + r + "" + cid);

        buffer.putLong(toHash);
        byte[] hash = md.digest(buffer.array());

        // convert hash back to Long
        buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(Arrays.copyOfRange(hash, hash.length - 8, hash.length));
        buffer.flip();

        Long hashed = buffer.getLong();
        Long hashedWithinSpace = Math.abs(hashed) % space;

        return (double)hashedWithinSpace / (double)space;
    }

    public static Double sumOfWeights(Collection<CeNode> nodes) {
        Double sum = 0.0;
        for(CeNode node : nodes) {
            sum += node.getWeight();
        }
        return sum;
    }
}
