package com.company.Commands;

import com.company.Main;

public class ListNodeData extends Command{

    public ListNodeData(Main main) {
        super(main, "ln", "list the stored data of node", 1);
    }

    @Override
    protected void runOnLine(String[] args) {
        String nodeName = args[1];

        System.out.println(main.foregroundManager.listNodeData(nodeName));

    }
}
