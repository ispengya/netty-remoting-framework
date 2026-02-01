package com.ispengya.remoting.protocol;

import com.alibaba.fastjson2.JSON;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class RemotingCommand {
    private static final AtomicInteger REQUEST_ID = new AtomicInteger(0);

    // Header fields
    private int code;
    private int opaque = REQUEST_ID.getAndIncrement();
    private int flag; // 0: request, 1: response
    private String remark;
    private Map<String, String> extFields = new HashMap<>();

    // Body (not included in header serialization)
    private transient byte[] body;

    public static RemotingCommand createRequestCommand(int code, Map<String, String> extFields) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.setExtFields(extFields);
        cmd.setFlag(0); // Request
        return cmd;
    }

    public static RemotingCommand createResponseCommand(int code, String remark) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.setRemark(remark);
        cmd.setFlag(1); // Response
        return cmd;
    }
    
    public static RemotingCommand createResponseCommand(int code, String remark, int opaque) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.setRemark(remark);
        cmd.setFlag(1); // Response
        cmd.setOpaque(opaque);
        return cmd;
    }

    public void markResponseType() {
        this.flag = 1;
    }

    public boolean isResponseType() {
        return this.flag == 1;
    }
    
    public CommandType getType() {
        if (this.isResponseType()) {
            return CommandType.RESPONSE;
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
