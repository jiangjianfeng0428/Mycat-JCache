package io.mycat.jcache.net.conn.handler;

import io.mycat.jcache.net.conn.Connection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;

/**
 * redis io handler
 */
public class RedisIOHandler implements IOHandler {
    public static final int REDIS_OK = 0, REDIS_ERR = -1;

    @Override
    public boolean doReadHandler(Connection conn) throws IOException {
        ByteBuffer readBuffer = conn.getReadDataBuffer();
        int lastMsgPos = conn.getLastMessagePos();
        int limit = readBuffer.position();
        IntStream.range(lastMsgPos, limit).forEach((i)->{
            if(readBuffer.get(i) == 13){

            }
        });
        return false;
    }

    private int processMultibulkBuffer(RedisMessage redisMessage){
        String message = redisMessage.message();
        if(message.charAt(0) != '*'){
            discardCurrentCmd(redisMessage, message);
            return REDIS_ERR;
        }

        int lineEndPos = message.indexOf("\r\n");
        if(lineEndPos < 0) return REDIS_ERR;

        int multibulkLen = 0;
        try{
            multibulkLen = Integer.parseInt(message.substring(1, lineEndPos));
        }catch (NumberFormatException e){
            discardCurrentCmd(redisMessage, message);
            return REDIS_ERR;
        }

        if(multibulkLen > 1024 * 1024){
            addErrReplay(redisMessage, "Protocol error: invalid multibulk length");
            discardCurrentCmd(redisMessage, message);
            return REDIS_ERR;
        }

        if(multibulkLen <= 0) return REDIS_OK; // "Multibulk processing could see a <= 0 length"

        String[] cmdParams = new String[multibulkLen];
        while(multibulkLen > 0){
            if(message.charAt(lineEndPos + 1) != '$'){
                discardCurrentCmd(redisMessage, message);
                return REDIS_ERR;
            }
            lineEndPos = message.indexOf("\r\n");

        }
        return REDIS_ERR;
    }

    /**
     * 丢弃当前指令
     *
     * @param redisMessage redis报文信息
     * @return 如果有下一条指令：返回true; 否则：返回false
     */
    private void discardCurrentCmd(RedisMessage redisMessage, String message){
        int nextCmdPos = message.indexOf('*');
        if(nextCmdPos > 0){
            // 清理nextCmdPos前无法解析的字符
            redisMessage.position(nextCmdPos);
        }else{
            // 清理无法解析的字符
            redisMessage.position(redisMessage.limit());
        }
    }

    private void addErrReplay(RedisMessage redisMessage, String replay){
        redisMessage.replay("-ERR" + replay + "\r\n");
    }

    private class RedisMessage{
        private final ByteBuffer connReadBuf;
        private int position, limit;
        private String replay;

        public RedisMessage(ByteBuffer connReadBuf, int position, int limit) {
            this.connReadBuf = connReadBuf;
            this.position = position;
            this.limit = limit;
        }

        public String message(){
            return new String(this.connReadBuf.array(), position, limit - position);
        }

        public int limit(){
            return this.limit;
        }

        public int position(){
            return this.position;
        }

        public void position(int position){
            this.position = position;
        }

        public void replay(String replay){
            this.replay = replay;
        }
    }
}
