package com.company.Commands;

import com.company.Main;

public class ListNodeMeta extends Command{

    public ListNodeMeta(Main main) {
        super(main, "lm", "list the meta data of node", 1);
    }

    @Override
    protected void runOnLine(String[] args) {
        String nodeName = args[1];

        System.out.println(main.foregroundManager.listNodeMeta(nodeName));

    }
}
