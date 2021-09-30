package com.company.Commands;

import com.company.Main;

import java.util.Arrays;

public class Update extends Command {
    public Update(Main main) {
        super(main, "u",
                "update a dataObject",
                "u <key> <newValue>",
                2);
    }

    @Override
    protected void runOnLine(String[] args) {
        Long key = Long.parseLong(args[1]);
        String[] arrayOfSplitValue = Arrays.copyOfRange(args, 2, args.length);
        StringBuilder valueBuilder = new StringBuilder();
        for(String split : arrayOfSplitValue) {
            valueBuilder.append(split).append(" ");
        }
        String value = valueBuilder.toString();

        System.out.println("Updating " + key + "...");
        if(main.foregroundDHT.update(key, value)) {
            System.out.println("Updated!");
        }
        else {
            System.out.println("Failed to update!");
        }
    }
}
