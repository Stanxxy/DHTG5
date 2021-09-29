package com.company.commands;

import com.company.BasicDHT;
import com.company.Cassandra.CaCluster;
import com.company.Cassandra.CaDHT;
import com.company.Main;
import com.company.NodeManager;

public class SelectDHT extends Command {
    public SelectDHT(Main main) {
        super(main, "s", "select a DHT type", 0);
    }

    @Override
    protected void runOnLine(String[] args) {
        System.out.print("Please input the type of DHT you want. Ca for Cassandra and Ce for Ceph: ");
        String type = main.getScanner().nextLine();
        if(type.equals("Ca")){
            if(main.backgroundDHT == null){
                CaDHT dhtObj = new CaDHT(CaCluster.getCluster());
                main.foregroundDHT = dhtObj;
                main.foregroundManager = dhtObj;
            } else if(!main.foregroundDHT.getName().equals("CaDHT")){
                swap();
            }
        } else if(type.equals("Ce")) {
            // ## do your own code here ##
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
