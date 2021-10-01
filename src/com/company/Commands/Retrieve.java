package com.company.Commands;

import com.company.Commons.DataObjPair;
import com.company.Main;

public class Retrieve extends Command {
    public Retrieve(Main main) {
        super(main, "r",
                "retrieve a dataObject",
                "r <key>",
                1
        );
    }

    @Override
    protected void runOnLine(String[] args) {
        Long key = Long.parseLong(args[1]);

        System.out.println("Retrieving " + key + "...");
        DataObjPair retrieved = main.foregroundDHT.select(key);
        System.out.println("Retrieved: " + retrieved);
    }
}
