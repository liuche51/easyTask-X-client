package com.github.liuche51.easyTaskX.client.netty.server.handler;



import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.enume.NettyInterfaceEnum;

import java.util.HashMap;
import java.util.Map;

public abstract class BaseHandler {
    public static Map<String,BaseHandler> INSTANCES;
    static {
        INSTANCES=new HashMap<String,BaseHandler>(){
            {
                put(NettyInterfaceEnum.CLIENT_SUBMIT_TASK,null);
            }
        };
    }
    public abstract String process(Dto.Frame frame) throws Exception;
}
