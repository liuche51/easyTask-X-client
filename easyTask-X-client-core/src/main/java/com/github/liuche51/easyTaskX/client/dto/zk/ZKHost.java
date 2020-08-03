package com.github.liuche51.easyTaskX.client.dto.zk;

import com.alibaba.fastjson.annotation.JSONField;
import com.github.liuche51.easyTaskX.client.core.AnnularQueue;
import com.github.liuche51.easyTaskX.client.enume.NodeSyncDataStatusEnum;

public class ZKHost {
    private String host;
    private int port= AnnularQueue.getInstance().getConfig().getServerPort();
    /**
     * 数据一致性状态。
     */
    private int dataStatus= NodeSyncDataStatusEnum.SYNC;
    public ZKHost(String host) {
        this.host = host;
    }
    public ZKHost(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getDataStatus() {
        return dataStatus;
    }

    public void setDataStatus(int dataStatus) {
        this.dataStatus = dataStatus;
    }

    @JSONField(serialize = false)
    public String getAddress(){
        StringBuffer str=new StringBuffer(this.host);
        str.append(':').append(this.port);
        return str.toString();
    }
}
