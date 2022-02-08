package com.github.liuche51.easyTaskX.client.dto;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 客户端节点对象
 */
public class ClientNode extends BaseNode {
    public ClientNode(BaseNode baseNode){
        super(baseNode.getHost(), baseNode.getPort());
    }
    public ClientNode(String host, int port) {
        super(host, port);
    }

    public ClientNode(String address) {
        super(address);
    }
}
