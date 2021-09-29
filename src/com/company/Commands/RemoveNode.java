package com.company.Commands;

import com.company.Main;

public class RemoveNode extends Command{
    public RemoveNode(Main main) {
        super(main, "rm", "remove a node from the cluster", 1);
    }

    @Override
    protected void runOnLine(String[] args) {
        String name = args[1];

        main.foregroundManager.removeNode(name);
    }
}
