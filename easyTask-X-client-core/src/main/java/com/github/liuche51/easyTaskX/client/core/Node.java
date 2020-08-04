package com.github.liuche51.easyTaskX.client.core;

import com.github.liuche51.easyTaskX.client.dto.ClockDiffer;
import com.github.liuche51.easyTaskX.client.enume.NodeSyncDataStatusEnum;
import com.github.liuche51.easyTaskX.client.netty.client.NettyClient;
import com.github.liuche51.easyTaskX.client.netty.client.NettyConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 节点对象
 */
public class Node implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(Node.class);
    private String host = "";
    private int port = AnnularQueue.getInstance().getConfig().getServerPort();
    /**
     * 数据一致性状态。
     */
    private int dataStatus = NodeSyncDataStatusEnum.SYNC;
    /**
     * 与目标主机的时钟差距
     */
    private ClockDiffer clockDiffer=new ClockDiffer();
    /**
     * 当前节点的所有serverNodes
     */
    private ConcurrentHashMap<String,Node> brokers = new ConcurrentHashMap<>();

    public Node(String host, int port) {
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

    public ClockDiffer getClockDiffer() {
        return clockDiffer;
    }

    public void setClockDiffer(ClockDiffer clockDiffer) {
        this.clockDiffer = clockDiffer;
    }

    public ConcurrentHashMap<String,Node> getBrokers() {
        return brokers;
    }

    public void setBrokers(ConcurrentHashMap<String,Node> brokers) {
        this.brokers = brokers;
    }

    public String getAddress() {
        StringBuffer str = new StringBuffer(this.host);
        str.append(":").append(this.port);
        return str.toString();
    }

    public NettyClient getClient() throws InterruptedException {
        return NettyConnectionFactory.getInstance().getConnection(host, port);
    }

    /**
     * 获取Netty客户端连接。带重试次数
     * 目前一次通信，占用一个Netty连接。
     * @param tryCount
     * @return
     */
    public NettyClient getClientWithCount(int tryCount) throws Exception {
        if (tryCount == 0) throw new Exception("getClientWithCount()-> exception!");
        try {
            return getClient();
        } catch (Exception e) {
            log.info("getClientWithCount tryCount=" + tryCount + ",objectHost="+this.getAddress());
            log.error("getClientWithCount()-> exception!", e);
            return getClientWithCount(tryCount);
        } finally {
            tryCount--;
        }
    }
}
