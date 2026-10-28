package com.ispengya.remoting.protocol;

import com.alibaba.fastjson2.JSON;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class RemotingCommand {
    private static final AtomicInteger REQUEST_ID = new AtomicInteger(0);

    private static final int FLAG_REQUEST = 0;
    private static final int FLAG_RESPONSE = 1;
    private static final int FLAG_PUSH = 2;

    // Header fields
    private int code;
    private int opaque = REQUEST_ID.getAndIncrement();
    private int flag; // 0: request, 1: response, 2: server push
    private String remark;
    private Map<String, String> extFields = new HashMap<>();

    // Body (not included in header serialization)
    private transient byte[] body;

    public static RemotingCommand createRequestCommand(int code, Map<String, String> extFields) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.setExtFields(extFields);
        cmd.setFlag(FLAG_REQUEST);
        return cmd;
    }

    public static RemotingCommand createResponseCommand(int code, String remark) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.setRemark(remark);
        cmd.setFlag(FLAG_RESPONSE);
        return cmd;
    }

    public static RemotingCommand createResponseCommand(int code, String remark, int opaque) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.setRemark(remark);
        cmd.setFlag(FLAG_RESPONSE);
        cmd.setOpaque(opaque);
        return cmd;
    }

    /**
     * 创建一条服务端主动推送消息。
     *
     * @param code 消息码，客户端按该值选择推送监听器
     * @param extFields 扩展字段
     * @return 推送消息
     */
    public static RemotingCommand createPushCommand(int code, Map<String, String> extFields) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        if (extFields != null) {
            cmd.setExtFields(extFields);
        }
        cmd.setFlag(FLAG_PUSH);
        return cmd;
    }

    /**
     * 创建一条服务端主动推送消息。
     *
     * @param code 消息码
     * @return 推送消息
     */
    public static RemotingCommand createPushCommand(int code) {
        return createPushCommand(code, null);
    }

    public void markResponseType() {
        this.flag = FLAG_RESPONSE;
    }

    /**
     * 标记为服务端推送消息。
     */
    public void markPushType() {
        this.flag = FLAG_PUSH;
    }

    public boolean isResponseType() {
        return this.flag == FLAG_RESPONSE;
    }

    /**
     * 判断是否为服务端推送消息。
     *
     * @return 推送消息返回 true
     */
    public boolean isPushType() {
        return this.flag == FLAG_PUSH;
    }

    public CommandType getType() {
        if (this.isResponseType()) {
            return CommandType.RESPONSE;
        }
        if (this.isPushType()) {
            return CommandType.PUSH;
        }
        return CommandType.REQUEST;
    }

    // Encoding helpers
    public byte[] headerEncode() {
        return JSON.toJSONString(this).getBytes(StandardCharsets.UTF_8);
    }

    public static RemotingCommand headerDecode(byte[] headerData) {
        return JSON.parseObject(headerData, RemotingCommand.class);
    }

    // Getters and Setters
    public int getCode() { return code; }
    public void setCode(int code) { this.code = code; }

    public int getOpaque() { return opaque; }
    public void setOpaque(int opaque) { this.opaque = opaque; }

    public int getFlag() { return flag; }
    public void setFlag(int flag) { this.flag = flag; }

    public String getRemark() { return remark; }
    public void setRemark(String remark) { this.remark = remark; }

    public Map<String, String> getExtFields() { return extFields; }
    public void setExtFields(Map<String, String> extFields) { this.extFields = extFields; }

    public byte[] getBody() { return body; }
    public void setBody(byte[] body) { this.body = body; }

    @Override
    public String toString() {
        return "RemotingCommand{" +
                "code=" + code +
                ", opaque=" + opaque +
                ", flag=" + flag +
                ", remark='" + remark + '\'' +
                ", extFields=" + extFields +
                '}';
    }
}
