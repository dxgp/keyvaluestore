package threads;

import java.net.Socket;

import storage.KeyValueStore;

/*
 * A thread for handling an incoming EXIT request from another node.
 */
public class HandleGetThread implements Runnable {
    KeyValueStore kv_store;
    String key;
    Socket reply_socket;
    public HandleGetThread(KeyValueStore kv_store,String key,Socket reply_socket){
        this.kv_store = kv_store;
        this.key = key;
        this.reply_socket = reply_socket;
    }
    public void run(){
        try{
            // if it contains key, return the key.
            if(kv_store.local_store.containsKey(key)){
                reply_socket.getOutputStream().write((kv_store.local_store.get(key)+"\n").getBytes());
                reply_socket.getOutputStream().flush();
            } else{
                System.out.println("DOES NOT CONTAIN KEY");
            }
        } catch(Exception e){e.printStackTrace();}
    }
}
