package com.company.Commons;

import java.util.HashMap;
import java.util.Map;

public abstract class Node {

    protected String name;

    protected Long capacity;

    protected Long load;

    protected Map<Long, DataObjPair> storedData;


    public Long getCapacity() {
        return capacity;
    }

    public String getName() {
        return name;
    }

    public Long getLoad(){
        return load;
    }

}
