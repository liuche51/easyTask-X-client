package com.github.liuche51.easyTaskX.client.zk;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.client.cluster.NodeService;
import com.github.liuche51.easyTaskX.client.dto.BaseNode;
import com.github.liuche51.easyTaskX.client.dto.zk.LeaderData;
import com.github.liuche51.easyTaskX.client.util.StringConstant;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKService {
    private static Logger log = LoggerFactory.getLogger(ZKService.class);
    /**
     * 获取当前节点的值信息
     *
     * @param usingWatcher 是否监听节点值变化。一次有效
     * @return
     */
    public static LeaderData getLeaderData(boolean usingWatcher) throws Exception {
        String path = StringConstant.CHAR_SPRIT + StringConstant.LEADER;
        return getDataByPath(path, LeaderData.class, usingWatcher ? new LeaderChangeWatcher() : null);
    }

    /**
     * 监听leader节点数据变化。持续监听
     * 如果leader变了，需要及时修改本地leader信息
     *
     * @throws Exception
     */
    public static void listenLeaderDataNode() throws Exception {
        String path = StringConstant.CHAR_SPRIT + StringConstant.LEADER;
        NodeCache nodeCache = new NodeCache(ZKUtil.getClient(), path);
        nodeCache.start(true);
        // 为缓存的节点添加watcher，或者说添加监听器
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            // 节点数据change事件的通知方法
            public void nodeChanged() throws Exception {
                String data = "null";
                if (nodeCache.getCurrentData() != null)
                    data = new String(nodeCache.getCurrentData().getData());
                log.info("listenLeaderDataNode()->leader节点变更事件触发!value={}", data);
                // 防止节点被删除时发生错误
                if (nodeCache.getCurrentData() == null) {
                    log.error("listenLeaderDataNode()->exception!nodeCache.getCurrentData() == null，可能该节点已被删除");
                    NodeService.CURRENTNODE.setClusterLeader(null);
                } else {
                    // 获取节点最新的数据
                    LeaderData ld = JSONObject.parseObject(nodeCache.getCurrentData().getData(), LeaderData.class);
                    if (ld == null)
                        NodeService.CURRENTNODE.setClusterLeader(null);
                    else
                        NodeService.CURRENTNODE.setClusterLeader(new BaseNode(ld.getHost(), ld.getPort()));
                }
            }
        });
    }

    /**
     * 根据节点路径，获取节点值信息
     *
     * @param path
     * @return
     */
    public static <T> T getDataByPath(String path, Class clazz, CuratorWatcher watcher) {
        try {
            byte[] bytes = null;
            if (watcher != null)
                bytes = ZKUtil.getClient().getData().usingWatcher(watcher).forPath(path);
            else
                bytes = ZKUtil.getClient().getData().forPath(path);
            return JSONObject.parseObject(bytes, clazz);
        } catch (Exception e) {
            //节点不存在了，属于正常情况。
            log.error("normally exception!getDataByPath():" + e.getMessage());
        }
        return null;
    }
}
