package threads;

import java.io.DataOutputStream;
import java.net.Socket;

import storage.KeyValueStore;

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
            DataOutputStream dos = new DataOutputStream(reply_socket.getOutputStream());
            if(kv_store.local_store.containsKey(key)){
                dos.writeBytes(kv_store.local_store.get(key)+"\n");
            } else{
                System.out.println("DOES NOT CONTAIN KEY");
            }
            dos.flush();
        } catch(Exception e){}
    }
}
