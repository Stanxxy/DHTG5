package com.company.Commands;

import com.company.Main;

public class BalanceLoad extends Command {
    public BalanceLoad(Main main) {
        super(main, "bn", "do load balancing of certain node", 1);
    }

    @Override
    protected void runOnLine(String[] args) {

        String type = main.foregroundDHT.getName();
        if(type.equals("CaDHT")) {
            String nodeName = args[1];
            main.foregroundManager.loadBalancing(nodeName);
        }
        else if(type.equals("CeCluster")) {
            String nodeName = args[1];
            Double newWeight = Double.parseDouble(args[2]);
            main.foregroundManager.loadBalancing(nodeName, newWeight);
        }
    }
}
