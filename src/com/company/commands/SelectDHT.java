package com.company.commands;

import com.company.BasicDHT;
import com.company.Main;

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
                // main.foregroundDHT = new CaDHT();
            } else if(!main.foregroundDHT.getName().equals("CaDHT")){
                BasicDHT tmp = main.foregroundDHT;
                main.foregroundDHT = main.backgroundDHT;
                main.backgroundDHT = tmp;
            }
        } else if(type.equals("Ce")) {
            // ## do your own code here ##
        } else {
            System.out.println("No such DHT type exists");
        }
    }
}
