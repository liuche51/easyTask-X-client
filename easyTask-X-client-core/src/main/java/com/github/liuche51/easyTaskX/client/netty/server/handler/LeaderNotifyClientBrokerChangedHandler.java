package com.github.liuche51.easyTaskX.client.netty.server.handler;

import com.github.liuche51.easyTaskX.client.cluster.BrokerService;
import com.github.liuche51.easyTaskX.client.cluster.ClientService;
import com.github.liuche51.easyTaskX.client.dto.BaseNode;
import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.task.AnnularQueueTask;
import com.github.liuche51.easyTaskX.client.util.StringConstant;
import com.google.protobuf.ByteString;

import java.util.Iterator;

/**
 *  Clinet响应：Leader通知Clinets。Broker发生变更。
 */
public class LeaderNotifyClientBrokerChangedHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body = frame.getBody();
        String[] items = body.split(StringConstant.CHAR_SPRIT_STRING);//type+Broker地址+新master地址
        String broker=items[1];
        switch (items[0]) {
            case StringConstant.ADD:
                ClientService.BROKERS.add(new BaseNode(broker));// 任务发送队列WAIT_SEND_TASK，在实际添加时判断新增
                break;
            case StringConstant.DELETE:
                //更新现有任务中的broker
                final String oldBroker=broker;
                final String newBroker=items[2];
                ClientService.getConfig().getClusterPool().submit(new Runnable() {
                    @Override
                    public void run() {
                        AnnularQueueTask.getInstance().changeBroker(newBroker,oldBroker);
                    }
                });
                //更新可用broker列表
                Iterator<BaseNode> temps = ClientService.BROKERS.iterator();
                while (temps.hasNext()) {
                    BaseNode bn = temps.next();
                    if (bn.getAddress().equals(oldBroker))
                        ClientService.BROKERS.remove(bn);
                }
                //移除任务发送到该Broker的队列
                BrokerService.WAIT_SEND_TASK.remove(oldBroker);
                //移除删除任务发送队列
                BrokerService.WAIT_DELETE_TASK.remove(oldBroker);
                break;
            default:break;
        }
        return null;
    }
}
