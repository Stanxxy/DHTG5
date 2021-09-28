package com.company.commands;

import com.company.Main;

public class Retrieve extends Command {
    public Retrieve(Main main) {
        super(main, "r", "retrieve a dataObject", 1);
    }

    @Override
    protected void runOnLine(String[] args) {
        String key = args[1];

        main.foregroundDHT.select(key);
    }
}
