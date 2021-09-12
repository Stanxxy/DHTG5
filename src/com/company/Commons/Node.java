package com.company.Commons;

import java.util.HashMap;
import java.util.Map;

public abstract class Node {

    protected String ip;

    protected Long capacity;

    protected Long load;

    protected Map<String, DataObjPair> storedData;

}
