package com.company.commands;

import com.company.Main;

public class Delete extends Command {
    public Delete(Main main) {
        super(main, "d", "delete a dataObject", 1);
    }

    @Override
    protected void runOnLine(String[] args) {
        String key = args[1];

    }
}
