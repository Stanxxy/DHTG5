package com.company;

import com.company.Commons.DataObjPair;

public interface BasicDHT {
    boolean insert(Long key, String value);
    DataObjPair select(Long key);
    boolean update(Long key, String value);
    boolean delete(Long key);
    String getName();
}
