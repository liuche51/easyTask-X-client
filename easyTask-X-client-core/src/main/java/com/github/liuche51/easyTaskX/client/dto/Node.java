package com.github.liuche51.easyTaskX.client.dto;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 节点对象
 */
public class Node extends BaseNode {
    /**
     * 集群所有可用的brokers
     */
    private CopyOnWriteArrayList<BaseNode> brokers = new CopyOnWriteArrayList<BaseNode>();
    /**
     * leader
     */
    private BaseNode clusterLeader=null;
    public Node(BaseNode baseNode){
        super(baseNode.getHost(), baseNode.getPort());
    }
    public Node(String host, int port) {
        super(host, port);
    }

    public Node(String address) {
        super(address);
    }

    public CopyOnWriteArrayList<BaseNode> getBrokers() {
        return brokers;
    }

    public void setBrokers(CopyOnWriteArrayList<BaseNode> brokers) {
        this.brokers = brokers;
    }

    public BaseNode getClusterLeader() {
        return clusterLeader;
    }

    public void setClusterLeader(BaseNode clusterLeader) {
        this.clusterLeader = clusterLeader;
    }
}
