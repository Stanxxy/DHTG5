package com.company.Commands;

import com.company.Main;

public class ListAllNodes extends Command{
    public ListAllNodes(Main main) {
        super(main, "la", "list the name of all nodes in cluster", 0);
    }

    @Override
    protected void runOnLine(String[] args) {
        System.out.println(main.foregroundManager.listAllNodes());
    }
}
