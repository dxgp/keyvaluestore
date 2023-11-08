package threads;

import java.io.DataOutputStream;
import java.net.Socket;

import storage.KeyValueStore;

public class HandleDeleteThread implements Runnable{
    KeyValueStore kv_store;
    String key;
    Socket reply_socket;
    public HandleDeleteThread(KeyValueStore kv_store,String key,Socket reply_socket){
        this.kv_store = kv_store;
        this.key = key;
        this.reply_socket = reply_socket;
    }
    public void run(){
        try{
            kv_store.peer_table.remove(key);
            reply_socket.getOutputStream().write(("EXECUTED\n").getBytes());
            reply_socket.getOutputStream().flush();
            // dos.writeBytes("EXECUTED\n");
        } catch(Exception e){e.printStackTrace();}
    }
}
