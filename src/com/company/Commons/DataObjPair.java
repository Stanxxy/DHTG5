package com.company.Commons;


public class DataObjPair {
    Long key;
    Long replicaI;
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

    public Long getReplicaI() { return replicaI; }

    public void setReplicaI(Long replicaI) {
        this.replicaI = replicaI;
    }

    public DataObjPair(){

    }

    public DataObjPair(Long search) {
        this.key = search;
    }

    public DataObjPair(Long key, String value) {
        this.key = key;
        this.value = value;
        this.replicaI = 0L;
    }

    public DataObjPair(Long key, String value, Long replicaI) {
        this.key = key;
        this.value = value;
        this.replicaI = replicaI;
    }

    public DataObjPair replicate(Long replicaI) {
        return new DataObjPair(key, value, replicaI);
    }

    @Override
    public String toString() {
        return "DataObjPair{" +
                "key=" + key +
                ", value='" + value + '\'' +
                ", replicaI='" + replicaI + "'" +
                '}';
    }
}
