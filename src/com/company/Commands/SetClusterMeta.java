package com.company.Commands;

import com.company.Commons.NodeFactory;
import com.company.Main;

public class SetClusterMeta extends Command{
    public SetClusterMeta(Main main) {
        super(main, "sm", "set the meta data for a cluster. " +
                "<init_node_num> <replica> <hashRange> [<min_copy>]", 3);
    }

    @Override
    protected void runOnLine(String[] args) {
        Integer nodeNum = Integer.parseInt(args[1]);
        Long replica = Long.parseLong(args[2]);
        Long hashRange = Long.parseLong(args[3]);

        main.foregroundManager.setHashRange(hashRange);

        if(args.length == 5){
            Long min_copy = Long.parseLong(args[4]);
            main.foregroundManager.setReplica(replica, min_copy);
        } else {
            main.foregroundManager.setReplica(replica);
        }

        if(main.foregroundDHT.getName().equals("CaDHT"))
        NodeFactory.initializeNodes("Ca", nodeNum);
    }
}
