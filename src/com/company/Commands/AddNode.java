package com.company.Commands;

import com.company.Main;

public class AddNode extends Command{
    public AddNode(Main main) {
        super(main, "an", "add a node into the cluster", 1);
        // current we assume the hash code of a node is set on the
    }

    @Override
    protected void runOnLine(String[] args) {
        String type = main.foregroundDHT.getName();

        if(type.equals("CaDHT")) {
            String nodeName = args[1];
            if (args.length == 3) {
                Long hashValue = Long.parseLong(args[2]);
                main.foregroundManager.addNode(nodeName, hashValue);
            } else {
                main.foregroundManager.addNode(nodeName);
            }
        }
        else if(type.equals("CeCluster")) {
            Double weight = Double.parseDouble(args[1]);
            main.foregroundManager.addNode(weight);
        }
    }
}
