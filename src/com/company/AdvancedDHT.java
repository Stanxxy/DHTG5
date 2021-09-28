package com.company;

import com.company.Commons.DataObjPair;

import java.util.List;
import java.util.Set;

public interface AdvancedDHT extends BasicDHT{
    boolean batchInsert(List<DataObjPair> dataObjList);

    boolean batchSelect(Set<String> keySet);

    boolean batchUpdate(List<DataObjPair> dataObjList);

    boolean batchDelete(List<DataObjPair> dataObjList);
}
