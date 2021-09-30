package com.company.commands;

import com.company.Cassandra.CaNode;
import com.company.Commons.Node;
import com.company.Commons.NodeCluster;
import com.company.Commons.NodeFactory;
import com.company.Main;
import com.company.ceph.CeCluster;
import com.company.ceph.CeNode;

import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

public class SetClusterMeta extends Command{
    public SetClusterMeta(Main main) {
        super(main, "sm",
                "set the metadata for the cluster",
                "sm <init_node_num> <replica> <hashRange> [<min_copy>] [<weight>] [<weight>] ...",
                3
        );
    }

    @Override
    protected void runOnLine(String[] args) {
        String type = main.foregroundDHT.getName();

        Integer nodeNum = Integer.parseInt(args[1]);
        Long replica = Long.parseLong(args[2]);
        Long hashRange = Long.parseLong(args[3]);

        NodeCluster.setHashRange(hashRange);
        if(type.equals("CaDHT")) {
            if(args.length == 5) {
                Long min_copy = Long.parseLong(args[4]);
                NodeCluster.setReplica(replica, min_copy);
            }
            NodeFactory.initializeNodes("Ca", nodeNum, null);
        }
        else if(type.equals("CeCluster") && args.length >= 5) {
            NodeCluster.setReplica(replica);
            String[] weightStrings = Arrays.copyOfRange(args, 4, args.length);
            Double[] weights = new Double[weightStrings.length];
            for(int i = 0; i < weightStrings.length; i ++) {
                weights[i] = Double.parseDouble(weightStrings[i]);
            }

            List<Node> nodes = NodeFactory.initializeNodes("Ce", nodeNum, weights);
            TreeMap<String, CeNode> nodeTree = new TreeMap<>();
            for(Node node : nodes) {
                nodeTree.put(node.getName(), (CeNode) node);
            }
            CeCluster cluster = (CeCluster) main.foregroundDHT;
            cluster.setGlobalNodeTable(nodeTree);
        }
        System.out.println("Successfully updated Cluster Meta");
    }
}
