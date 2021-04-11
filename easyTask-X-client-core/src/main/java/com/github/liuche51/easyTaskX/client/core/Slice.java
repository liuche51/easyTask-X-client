package com.github.liuche51.easyTaskX.client.core;

import com.github.liuche51.easyTaskX.client.dto.InnerTask;

import java.util.concurrent.ConcurrentSkipListMap;

public class Slice {
    /**
     * 任务容器。跳表
     */
    private ConcurrentSkipListMap<String, InnerTask> list=new ConcurrentSkipListMap<String, InnerTask>();;

    public ConcurrentSkipListMap<String, InnerTask> getList() {
        return list;
    }

    public void setList(ConcurrentSkipListMap<String, InnerTask> list) {
        this.list = list;
    }
}
