package com.github.liuche51.easyTaskX.client.dto.zk;


import com.github.liuche51.easyTaskX.client.core.AnnularQueue;

import java.util.LinkedList;
import java.util.List;

public class ZKNode {
    private String host;
    private int port= AnnularQueue.getInstance().getConfig().getServerPort();
    /**
     * 最近一次心跳时间
     */
    private String lastHeartbeat;
    private String createTime;
    /**
     * serverNodes
     */
    private List<ZKHost> serverNodes=new LinkedList<>();
    public ZKNode(){}
    public ZKNode(String host, int port){
        this.host=host;
        this.port=port;
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

    public List<ZKHost> getServerNodes() {
        return serverNodes;
    }

    public void setServerNodes(List<ZKHost> serverNodes) {
        this.serverNodes = serverNodes;
    }

    public String getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void setLastHeartbeat(String lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }
}
