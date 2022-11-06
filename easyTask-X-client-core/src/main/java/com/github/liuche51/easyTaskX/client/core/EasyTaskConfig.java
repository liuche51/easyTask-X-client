package com.github.liuche51.easyTaskX.client.core;

import com.github.liuche51.easyTaskX.client.util.StringUtils;
import com.github.liuche51.easyTaskX.client.util.Util;

import java.util.concurrent.Executors;

/**
 * 系统配置项
 */
public class EasyTaskConfig {
    /**
     * zk地址。必填 如:127.0.0.1:2181,192.168.1.128:2181
     */
    private String zkAddress;
    /**
     * 设置当前节点Netty服务端口号。默认2020
     */
    private int serverPort = 2020;


    private AdvanceConfig advanceConfig = new AdvanceConfig();

    public String getZkAddress() {
        return zkAddress;
    }

    public void setZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
    }


    public String getAddress() throws Exception {
        StringBuffer buffer = new StringBuffer(Util.getLocalIP());
        buffer.append(":").append(getServerPort());
        return buffer.toString();
    }

    public int getServerPort() {
        return serverPort;
    }

    /**
     * set ServerPort，default 2020
     *
     * @param port
     * @throws Exception
     */
    public void setServerPort(int port) throws Exception {
        if (port == 0)
            throw new Exception("ServerPort must not empty");
        this.serverPort = port;
    }


    public AdvanceConfig getAdvanceConfig() {
        return advanceConfig;
    }

    public void setAdvanceConfig(AdvanceConfig advanceConfig) {
        this.advanceConfig = advanceConfig;
    }

    /**
     * 必填项验证
     *
     * @param config
     * @throws Exception
     */
    public static void validateNecessary(EasyTaskConfig config) throws Exception {
        if (StringUtils.isNullOrEmpty(config.zkAddress))
            throw new Exception("zkAddress is necessary!");
        if (config.getAdvanceConfig().getWorkers() == null)
            config.getAdvanceConfig().setWorkers(Executors.newCachedThreadPool());
        if (config.getAdvanceConfig().getClusterPool() == null)
            config.getAdvanceConfig().setClusterPool(Executors.newCachedThreadPool());
    }
}
