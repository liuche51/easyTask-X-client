package com.github.liuche51.easyTaskX.client.netty.server.handler;

import com.github.liuche51.easyTaskX.client.cluster.NodeService;
import com.github.liuche51.easyTaskX.client.dto.BaseNode;
import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.task.AnnularQueueTask;
import com.github.liuche51.easyTaskX.client.util.StringConstant;
import com.google.protobuf.ByteString;

import java.util.Iterator;

/**
 *  Clinet响应：通知Clinets。Broker发生变更。
 */
public class LeaderNotifyClientBrokerChangedHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body = frame.getBody();
        String[] items = body.split(StringConstant.CHAR_SPRIT_STRING);//type+Broker地址+新master地址
        switch (items[0]) {
            case StringConstant.ADD:
                NodeService.CURRENT_NODE.getBrokers().add(new BaseNode(items[1]));
                break;
            case StringConstant.DELETE:
                //更新现有任务中的broker
                final String oldBroker=items[1];
                final String newBroker=items[2];
                NodeService.getConfig().getClusterPool().submit(new Runnable() {
                    @Override
                    public void run() {
                        AnnularQueueTask.getInstance().changeBroker(newBroker,oldBroker);
                    }
                });
                //更新可用broker列表
                Iterator<BaseNode> temps = NodeService.CURRENT_NODE.getBrokers().iterator();
                while (temps.hasNext()) {
                    BaseNode bn = temps.next();
                    if (bn.getAddress().equals(items[1]))
                        NodeService.CURRENT_NODE.getBrokers().remove(bn);
                }
                break;
            default:break;
        }
        return null;
    }
}
