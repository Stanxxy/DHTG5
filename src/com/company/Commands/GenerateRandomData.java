package com.company.Commands;

import com.company.Cassandra.CaCluster;
import com.company.Commons.NodeCluster;
import com.company.Main;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

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

        if(main.foregroundDHT.getName().equals("CaDHT")) {
            Random r = new Random();
            Set<Long> randomKeySet = new HashSet<>();
            while(randomKeySet.size() < x) {
                Long key = (long) (r.nextDouble() * CaCluster.getHashRange());
                if(!randomKeySet.contains(key)){
                    String value = "GENERATED ITEM " + key + "/" + x;
                    randomKeySet.add(key);
                    main.foregroundDHT.insert(key, value);
                }
            }
        } else {
            for(int i = 0; i < x; i ++) {
                Long key = (long) i;
                String value = "GENERATED ITEM " + i + "/" + x;
                main.foregroundDHT.insert(key, value);
            }
        }
    }

}
