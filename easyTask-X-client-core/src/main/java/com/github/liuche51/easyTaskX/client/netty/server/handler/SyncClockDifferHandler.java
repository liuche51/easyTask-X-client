package com.github.liuche51.easyTaskX.client.netty.server.handler;


import com.github.liuche51.easyTaskX.client.dto.proto.Dto;

public class SyncClockDifferHandler extends BaseHandler{
    @Override
    public String process(Dto.Frame frame) throws Exception {
        return String.valueOf(System.currentTimeMillis());
    }
}
