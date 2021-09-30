package com.company.issuables;

import com.company.Ceph.CeNode;

import java.util.concurrent.Callable;

public abstract class Issuable<NType> implements Callable<NType> {
    protected CeNode node;

    public Issuable(CeNode node) {
        this.node = node;
    }

    public CeNode getNode() {
        return node;
    }
}
