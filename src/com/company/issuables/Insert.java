package com.company.issuables;

import com.company.Commons.DataObjPair;
import com.company.Ceph.CeNode;

public class Insert extends Issuable<Boolean> {
    private DataObjPair toInsert;

    public Insert(DataObjPair toInsert, CeNode node) {
        super(node);
        this.toInsert = toInsert;
    }

    @Override
    public Boolean call() throws Exception {
        return node.insert(toInsert);
    }
}