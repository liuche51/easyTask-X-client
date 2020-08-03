package com.github.liuche51.easyTaskX.client.core;

import com.github.liuche51.easyTaskX.client.dto.Task;

import java.util.concurrent.ConcurrentSkipListMap;

public class Slice {
    private ConcurrentSkipListMap<String, Task> list=new ConcurrentSkipListMap<String, Task>();;

    public ConcurrentSkipListMap<String, Task> getList() {
        return list;
    }

    public void setList(ConcurrentSkipListMap<String, Task> list) {
        this.list = list;
    }
}
