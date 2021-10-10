package com.company.Commands;

import com.company.Main;

public class GenerateRandomData extends Command {
    public GenerateRandomData(Main main) {
        super(main, "rd",
                "generate x random items to insert",
                "rd <x>",
                1
        );
    }

    protected void runOnLine(String args[]) {
        Integer x = Integer.parseInt(args[1]);

        System.out.println("Inserting " + x + " items...");
        for(int i = 0; i < x; i ++) {
            Long key = (long) i;
            String value = "GENERATED ITEM " + i + "/" + x;
            main.foregroundDHT.insert(key, value);
        }
    }

}
