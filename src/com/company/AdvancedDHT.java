package com.company;

import com.company.Commons.DataObjPair;

import java.util.List;
import java.util.Set;

public interface AdvancedDHT extends BasicDHT{
    List<String> batchInsert(List<DataObjPair> dataObjList);

    List<DataObjPair> batchSelect(Set<String> keySet);

    List<String> batchUpdate(List<DataObjPair> dataObjList);

    List<String> batchDelete(List<DataObjPair> dataObjList);
}
