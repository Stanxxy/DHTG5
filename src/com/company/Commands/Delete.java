package com.company.Commands;

import com.company.Main;

public class Delete extends Command {
    public Delete(Main main) {
        super(main, "d",
                "delete a dataObject",
                "d <key>",
                1
        );
    }

    @Override
    protected void runOnLine(String[] args) {
        Long key = Long.parseLong(args[1]);

        System.out.println("Deleting" + key + "...");
        if(main.foregroundDHT.delete(key)) {
            System.out.println("Deleted!");
        }
        else {
            System.out.println("Failed to delete!");
        }
    }
}
