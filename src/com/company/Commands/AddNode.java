package com.company.Commands;

import com.company.Main;

public class AddNode extends Command{
    public AddNode(Main main) {
        super(main, "an", "add a node into the cluster", 2);
        // current we assume the hash code of a node is set on the
    }

    @Override
    protected void runOnLine(String[] args) {
        String nodeName = args[1];
        Long hashValue = Long.parseLong(args[2]);
        main.foregroundManager.addNode(nodeName, hashValue);
    }
}
