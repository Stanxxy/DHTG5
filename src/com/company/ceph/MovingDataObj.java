package com.company.Ceph;

import com.company.Commons.DataObjPair;

public class MovingDataObj {
    private CeNode address;
    private DataObjPair data;

    public MovingDataObj(CeNode address, DataObjPair data) {
        this.address = address;
        this.data = data;
    }

    public CeNode getAddress() {
        return address;
    }

    public DataObjPair getData() {
        return data;
    }
}
