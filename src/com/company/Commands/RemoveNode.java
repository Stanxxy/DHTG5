package com.company.Commands;

import com.company.Main;

public class RemoveNode extends Command{
    public RemoveNode(Main main) {
        super(main, "rn", "remove a node from the cluster", 1);
    }

    @Override
    protected void runOnLine(String[] args) {
        String name = args[1];

        System.out.println("Removing node " + name + "...");
        if(main.foregroundManager.removeNode(name)) {
            System.out.println("Removed!");
        }
        else {
            System.out.println("Failed to remove!");
        }
    }
}
