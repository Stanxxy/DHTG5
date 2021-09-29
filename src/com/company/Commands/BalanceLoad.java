package com.company.Commands;

import com.company.Main;

public class BalanceLoad extends Command {
    public BalanceLoad(Main main) {
        super(main, "b", "do load balancing of certain node", 1);
    }

    @Override
    protected void runOnLine(String[] args) {
        String nodeName = args[1];

        main.foregroundManager.loadBalancing(nodeName);
    }
}
