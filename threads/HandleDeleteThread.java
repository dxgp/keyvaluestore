package threads;

import java.net.Socket;

import storage.KeyValueStore;

/*
 * A thread for handling an incoming DELETE request from another node.
 */
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
            kv_store.peer_table.remove(key); //remove from peer table
            reply_socket.getOutputStream().write(("EXECUTED\n").getBytes()); //write to output stream
            reply_socket.getOutputStream().flush();
        } catch(Exception e){e.printStackTrace();}
    }
}
