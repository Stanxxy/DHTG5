package com.company;

public interface BasicDHT {

    String insert(String key, String value);

    String select(String key);

    String update(String key, String value);

    String delete(String key);

    String getName();
}
