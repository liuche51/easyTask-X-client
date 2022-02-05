package com.github.liuche51.easyTaskX.client.netty.server.handler;

import com.github.liuche51.easyTaskX.client.cluster.BrokerService;
import com.github.liuche51.easyTaskX.client.dto.SubmitTaskResult;
import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.dto.proto.StringListDto;
import com.github.liuche51.easyTaskX.client.util.StringConstant;
import com.google.protobuf.ByteString;

import java.util.List;

/**
 * Broker通知Client，部分提交的任务状态
 */
public class BrokerNotifyClientSubmitTaskResultHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        StringListDto.StringList list=StringListDto.StringList.parseFrom(frame.getBodyBytes()) ;
        List<String> tasks=list.getListList();
        for(String task:tasks){
            String[] split = task.split(StringConstant.CHAR_SPRIT_COMMA);//任务ID,状态,错误信息
            SubmitTaskResult submitTaskResult = BrokerService.TASK_SYNC_BROKER_STATUS.get(split[0]);
            if(submitTaskResult!=null){
                submitTaskResult.setStatus(Integer.parseInt(split[1]));
                submitTaskResult.setError(split[2]);
                synchronized (submitTaskResult){//通知其他锁定次对象的线程唤醒继续执行
                    submitTaskResult.notify();
                }
            }
        }
        return null;
    }
}
