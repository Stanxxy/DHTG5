package com.company.Commons;


public class DataObjPair {
    String key;
    String value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) throws IllegalAccessException {
        if(this.key == null){
            this.key = key;
        } else {
            throw new IllegalAccessException("Cannot modify data key.");
        }
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public DataObjPair(){

    }

    public DataObjPair(String key, String value) {
        this.key = key;
        this.value = value;
    }
}
