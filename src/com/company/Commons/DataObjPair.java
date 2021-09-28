package com.company.Commons;


public class DataObjPair {
    Long key;
    String value;

    public Long getKey() {
        return key;
    }

    public void setKey(Long key) throws IllegalAccessException {
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

    public DataObjPair(Long key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return "DataObjPair{" +
                "key=" + key +
                ", value='" + value + '\'' +
                '}';
    }
}
