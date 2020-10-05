package com.github.liuche51.easyTaskX.client.cluster;

import com.github.liuche51.easyTaskX.client.dto.BaseNode;
import com.github.liuche51.easyTaskX.client.dto.Task;
import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.client.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.client.netty.client.NettyClient;
import com.github.liuche51.easyTaskX.client.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.client.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class BrokerService {
    private static final Logger log = LoggerFactory.getLogger(BrokerService.class);

    /**
     * 提交新任务到集群
     * 如果有多个Broker，则采用随机算法挑选一个
     *
     * @param task
     * @throws Exception
     */
    public static void submitTask(Task task) throws Exception {
        ScheduleDto.Schedule schedule = task.toScheduleDto();
        ConcurrentHashMap<String, BaseNode> brokers = NodeService.CURRENTNODE.getBrokers();
        Iterator<Map.Entry<String, BaseNode>> items = brokers.entrySet().iterator();
        BaseNode selectedNode = null;
        if (brokers.size() > 1) {
            Random random = new Random();
            int index = random.nextInt(brokers.size());//随机生成的随机数范围就变成[0,size)。
            int flag = 0;
            while (items.hasNext()) {
                if (index == flag) {
                    selectedNode = items.next().getValue();
                    break;
                }
            }
        } else
            selectedNode = items.next().getValue();
        task.getTaskExt().setBroker(selectedNode.getAddress());//将任务所属服务端节点标记一下
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.CLIENT_SUBMIT_TASK).setSource(NodeService.getConfig().getAddress())
                .setBodyBytes(schedule.toByteString());
        NettyClient client = selectedNode.getClientWithCount(1);
        boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, client, 1, 0, null);
        if (!ret) {
            throw new Exception("sendSyncMsgWithCount()->exception! ");
        }
    }

    /**
     * 删除任务。
     * 已执行完毕的任务，系统自动删除用
     *
     * @param taskId
     * @param brokerAddress
     * @throws Exception
     */
    public static boolean deleteTask(String taskId, String brokerAddress) {
        try {
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.CLIENT_DELETE_TASK).setSource(NodeService.getConfig().getAddress())
                    .setBody(taskId);
            NettyClient client = new BaseNode(brokerAddress).getClientWithCount(1);
            boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, client, 1, 0, null);
            return ret;

        } catch (Exception e) {
            log.error("deleteTask()-> exception!", e);
        }
        return false;
    }
}
