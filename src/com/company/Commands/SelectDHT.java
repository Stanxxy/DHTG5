package com.company.commands;

import com.company.BasicDHT;
import com.company.Cassandra.CaCluster;
import com.company.Cassandra.CaDHT;
import com.company.Main;
import com.company.NodeManager;
import com.company.ceph.CeCluster;
import com.sun.source.tree.Tree;

import java.util.TreeMap;

public class SelectDHT extends Command {
    public SelectDHT(Main main) {
        super(main, "s",
                "select a DHT type (\"Ca\" for Cassandra, \"Ce\" for Ceph)",
                "s <type>",
                1
        );
    }

    @Override
    protected void runOnLine(String[] args) {
        String type = args[1];
        if(type.equals("Ca")) {
            if(main.backgroundDHT == null) {
                CaDHT dhtObj = new CaDHT(CaCluster.getCluster());
                main.foregroundDHT = dhtObj;
                main.foregroundManager = dhtObj;
                System.out.println("Selected Ceph");
            }
            else if(!main.foregroundDHT.getName().equals("CaDHT")) {
                swap();
                System.out.println("Swapping to Cassandra");
            }
        } else if(type.equals("Ce")) {
            if(main.backgroundDHT == null) {
                CeCluster cluster = new CeCluster(new TreeMap<>());
                main.foregroundDHT = cluster;
                main.foregroundManager = cluster;
                System.out.println("Selected Ceph");
            }
            else if(!main.foregroundDHT.getName().equals("CeCluster")) {
                swap();
                System.out.println("Swapping to Ceph");
            }
        } else {
            System.out.println("No such DHT type exists");
        }
    }

    private void swap(){
        BasicDHT tmpDHT = main.foregroundDHT;
        main.foregroundDHT = main.backgroundDHT;
        main.backgroundDHT = tmpDHT;

        NodeManager tmpManager = main.foregroundManager;
        main.foregroundManager = main.backgroundManager;
        main.backgroundManager = tmpManager;
    }
}
