package com.github.liuche51.easyTaskX.client.netty.server.handler;


import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.util.StringConstant;

public class TranTrySaveTaskHandler extends BaseHandler {
    @Override
    public String process(Dto.Frame frame) throws Exception {
        return StringConstant.EMPTY;
    }
}
