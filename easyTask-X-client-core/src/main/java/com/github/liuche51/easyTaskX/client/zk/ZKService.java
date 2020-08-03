package com.github.liuche51.easyTaskX.client.zk;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.client.core.AnnularQueue;
import com.github.liuche51.easyTaskX.client.dto.zk.ZKNode;
import com.github.liuche51.easyTaskX.client.util.StringConstant;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.List;

public class ZKService {
    private static Logger log = LoggerFactory.getLogger(ZKService.class);
    /**
     * 创建命名空间下某个子节点目录
     *
     * @param name
     */
    public static void createZKNode(String name) {
        try {
            String path = StringConstant.CHAR_SPRIT + name;
            //检查是否存在节点。如果连不上zk，这里就会卡主线程，进入循环重试连接。直到连接成功
            Stat stat1 = ZKUtil.getClient().checkExists().forPath(path);
            if (stat1 == null) {
                //创建永久节点
                ZKUtil.getClient().create().withMode(CreateMode.PERSISTENT).forPath(path);
            }
        } catch (Exception e) {
            log.error("createZKNode exception！", e);
        }
    }
    /**
     * 当前节点注册为永久节点
     *
     * @param data
     */
    public static void register(ZKNode data) {
        try {
            String path = StringConstant.CHAR_SPRIT+ StringConstant.CLIENT+StringConstant.CHAR_SPRIT + AnnularQueue.getInstance().getConfig().getAddress();
            //检查是否存在节点。如果连不上zk，这里就会卡主线程，进入循环重试连接。直到连接成功
            Stat stat1 = ZKUtil.getClient().checkExists().forPath(path);
            if (stat1 != null) {
                ZKUtil.getClient().setData().forPath(path, JSONObject.toJSONString(data).getBytes());//重新覆盖注册信息
                return;
            } else {
                //创建永久节点
                ZKUtil.getClient().create().withMode(CreateMode.PERSISTENT).forPath(path, JSONObject.toJSONString(data).getBytes());
            }
        } catch (Exception e) {
            log.error("register exception！", e);
        }
    }

    /**
     * 获取命名空间下Client子目录下的的节点信息
     *
     * @return
     */
    public static List<String> getChildrenByClientNode() {
        String path = StringConstant.CHAR_SPRIT+ StringConstant.CLIENT+StringConstant.CHAR_SPRIT;
        return getChildrenByPath(path);
    }
    /**
     * 获取命名空间下SERVER子目录下的的节点信息
     *
     * @return
     */
    public static List<String> getChildrenByServerNode() {
        String path = StringConstant.CHAR_SPRIT+ StringConstant.SERVER+StringConstant.CHAR_SPRIT;
        return getChildrenByPath(path);
    }

    /**
     * 获取当前节点下的子节点信息
     *
     * @return
     */
    public static List<String> getChildrenByCurrentNode() throws UnknownHostException {
        String path = StringConstant.CHAR_SPRIT+ StringConstant.CLIENT+StringConstant.CHAR_SPRIT + AnnularQueue.getInstance().getConfig().getAddress();
        return getChildrenByPath(path);
    }

    /**
     * 根据节点路径，获取节点下的子节点名称
     *
     * @param path
     * @return
     */
    public static List<String> getChildrenByPath(String path) {
        try {
            List<String> list = ZKUtil.getClient().getChildren().forPath(path);
            return list;
        } catch (Exception e) {
            log.error("getChildrenByPath exception!", e);
        }
        return null;
    }

    /**
     * 获取当前节点的值信息
     *
     * @return
     */
    public static ZKNode getDataByCurrentNode() throws UnknownHostException {
        String path = StringConstant.CHAR_SPRIT+ StringConstant.CLIENT+StringConstant.CHAR_SPRIT + AnnularQueue.getInstance().getConfig().getAddress();
        return getDataByPath(path);
    }

    /**
     * 根据节点路径，获取节点值信息
     *
     * @param path
     * @return
     */
    public static ZKNode getDataByPath(String path) {
        try {
            byte[] bytes = ZKUtil.getClient().getData().forPath(path);
            return JSONObject.parseObject(bytes, ZKNode.class);
        } catch (Exception e) {
            //节点不存在了，属于正常情况。
            log.error("normally exception!getDataByPath():"+e.getMessage());
        }
        return null;
    }

    /**
     * 设置当前节点的值信息
     *
     * @param data
     * @return
     */
    public static boolean setDataByCurrentNode(ZKNode data) throws UnknownHostException {
        String path = StringConstant.CHAR_SPRIT+ StringConstant.CLIENT+StringConstant.CHAR_SPRIT + AnnularQueue.getInstance().getConfig().getAddress();
        return setDataByPath(path, data);
    }

    /**
     * 根据节点路径，设置新值
     *
     * @param path
     * @return
     */
    public static boolean setDataByPath(String path, ZKNode data) {
        try {
            ZKUtil.getClient().setData().forPath(path, JSONObject.toJSONString(data).getBytes());
            return true;
        } catch (Exception e) {
            log.error("", e);
        }
        return false;
    }

    /**
     * 根据节点路径，删除节点
     *
     * @param path
     * @return
     */
    public static boolean deleteNodeByPath(String path) {
        try {
            //检查是否存在节点
            Stat stat1 = ZKUtil.getClient().checkExists().forPath(path);
            if (stat1 != null) {
                ZKUtil.getClient().delete().forPath(path);
            }
            return true;
        } catch (Exception e) {
            // 删除失败也无关紧要
            //log.error("deleteNodeByPath", e);
        }
        return false;
    }

    /**
     * 根据节点路径，删除节点。用于不需要知道删除结果的逻辑
     *
     * @param path
     * @return
     */
    public static void deleteNodeByPathIgnoreResult(String path) {
        try {
            ZKUtil.getClient().delete().forPath(path);
        } catch (Exception e) {
            // 删除失败也无关紧要，
        }
    }
}