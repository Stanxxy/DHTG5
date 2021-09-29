package com.company.Commands;

import com.company.Main;

public class UnplugNode extends Command{
    public UnplugNode(Main main){
        super(main, "u", "unplug a db node from the cluster", 1);
    }

    @Override
    protected void runOnLine(String[] args) {
        String nodeName = args[1];
        main.foregroundManager.unplugNode(nodeName);
    }
}
