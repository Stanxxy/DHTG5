package com.company.commands;

import com.company.Main;

public class Update extends Command {
    public Update(Main main) {
        super(main, "u", "update a dataObject", 2);
    }

    @Override
    protected void runOnLine(String[] args) {
        Long key = Long.parseLong(args[1]);
        String newValue = args[2];

    }
}
