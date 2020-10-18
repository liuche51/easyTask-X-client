package com.github.liuche51.easyTaskX.client.netty.server.handler;



import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.enume.NettyInterfaceEnum;
import com.google.protobuf.ByteString;

import java.util.HashMap;
import java.util.Map;

public abstract class BaseHandler {
    public static Map<String,BaseHandler> INSTANCES;
    static {
        INSTANCES=new HashMap<String,BaseHandler>(){
            {
                put(NettyInterfaceEnum.LeaderNotifyClientUpdateBrokerChange,new LeaderNotifyClientsUpdateBrokerChangeHandler());
            }
        };
    }
    public abstract ByteString process(Dto.Frame frame) throws Exception;
}
