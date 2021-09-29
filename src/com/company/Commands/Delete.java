package com.company.Commands;

import com.company.Main;

public class Delete extends Command {
    public Delete(Main main) {
        super(main, "d", "delete a dataObject", 1);
    }

    @Override
    protected void runOnLine(String[] args) {
        Long key = Long.parseLong(args[1]);

        main.foregroundDHT.delete(key);
    }
}
