package com.company.commands;

import com.company.Main;

import java.util.Arrays;

public class Insert extends Command {
    public Insert(Main main) {
        super(main, "i", "insert a dataObject", 2);
    }

    @Override
    protected void runOnLine(String[] args) {
        String key = args[1];
        String[] arrayOfSplitValue = Arrays.copyOfRange(args, 2, args.length);
        StringBuilder valueBuilder = new StringBuilder();
        for(String split : arrayOfSplitValue) {
            valueBuilder.append(split);
        }
        String value = valueBuilder.toString();

        System.out.println(key + ": " + value);
    }
}
