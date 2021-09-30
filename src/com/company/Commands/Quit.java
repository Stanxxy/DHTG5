package com.company.Commands;

import com.company.Main;

public class Quit extends Command {
    public Quit(Main main) {
        super(main, "q", "quit the program", 0);
    }

    @Override
    protected void runOnLine(String[] args) {
        main.stop();

    }
}
