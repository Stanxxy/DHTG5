package com.company.Commands;

import com.company.Main;

import java.util.Arrays;

public class Insert extends Command {
    public Insert(Main main) {
        super(main, "i",
                "insert a dataObject",
                "i <key> <value>",
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

        System.out.println("Inserting " + key + "...");
        if(main.foregroundDHT.insert(key, value)) {
            System.out.println("Inserted!");
        }
        else {
            System.out.println("Failed to insert...");
        }
    }
}
