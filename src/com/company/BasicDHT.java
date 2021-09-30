package com.company;

public interface BasicDHT {

    boolean insert(Long key, String value);

    String select(Long key);

    boolean update(Long key, String value);

    boolean delete(Long key);

    String getName();
}
