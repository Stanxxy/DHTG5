package com.company.Commands;

import com.company.Main;

public class Debug extends Command {
    public Debug(Main main) {
        super(main, "dg",
                "Debug command, prints out the entire contents of the DHT",
                "dg",
                0
        );
    }

    @Override
    protected void runOnLine(String[] args) {
        System.out.println(main.foregroundManager.debug());
    }
}
