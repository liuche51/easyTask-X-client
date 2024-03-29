package com.github.liuche51.easyTaskX.client.netty.server;

import com.github.liuche51.easyTaskX.client.cluster.ClientService;
import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.dto.proto.ResultDto;
import com.github.liuche51.easyTaskX.client.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.client.util.LogUtil;
import com.github.liuche51.easyTaskX.client.util.StringConstant;
import com.google.protobuf.ByteString;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.net.InetAddress;

/**
 *
 */
public class ServerHandler extends SimpleChannelInboundHandler<Object> {

    /**
     * 接受客户端发过来的消息。
     *
     * @param ctx
     * @param msg
     */
    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) {
        // 收到消息直接打印输出
        StringBuilder str = new StringBuilder("Received Client:");
        str.append(ctx.channel().remoteAddress()).append(" send : ").append(msg);
        LogUtil.debug(str.toString());
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        ResultDto.Result.Builder result = ResultDto.Result.newBuilder();
        result.setResult(StringConstant.TRUE);
        try {
            builder.setSource(ClientService.getConfig().getAddress());
            Dto.Frame frame = (Dto.Frame) msg;
            builder.setInterfaceName(frame.getInterfaceName());
            builder.setIdentity(frame.getIdentity());
            BaseHandler handler = BaseHandler.INSTANCES.get(frame.getInterfaceName());
            if (handler == null) throw new Exception("unknown interface method!");
            ByteString bs = handler.process(frame);
            if (bs != null)
                result.setBodyBytes(bs);
        } catch (Exception e) {
            LogUtil.error("Deal client msg occured error！", e);
            result.setResult(StringConstant.FALSE);
            result.setMsg(e.getMessage());
        }
        builder.setBodyBytes(result.build().toByteString());
        ctx.writeAndFlush(builder.build());
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        System.out.println("RamoteAddress : " + ctx.channel().remoteAddress() + " active !");
        try {
            ctx.writeAndFlush("Welcome to " + InetAddress.getLocalHost().getHostName() + " service!\n");
            super.channelActive(ctx);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LogUtil.error("channelActive exception!", e);
        }

    }
}