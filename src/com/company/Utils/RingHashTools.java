package com.company.Utils;

import java.security.MessageDigest;

public class RingHashTools {

    private MessageDigest md;

    public RingHashTools(){
        md = MessageDigest.getInstance("sha-1");
    }

    public static Long calculateNodeHash(String ip){
        // calculate node hash here

        return 0L;
    }

    public static Long calculateDataHash(String key){
        // calculate data hash here
        return 0L;
    }

    public static boolean isGreaterThan(Long space, Long hash1, Long hash2){
        // because we use hash ring. we have to handle the end and begin of ring
        return true;
    }

    public static Long oneMoveForward(Long space, Long hash){
        return null;
    }

    public static Long oneMoveBackward(Long space, Long hash){
        return null;
    }

    public static Long hashDistance(Long space, Long start, Long end){
        return null;
    }
}
