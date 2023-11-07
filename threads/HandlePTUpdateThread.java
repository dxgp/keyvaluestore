package threads;

import java.io.DataOutputStream;
import java.net.Socket;

import storage.KeyValueStore;

public class HandlePTUpdateThread implements Runnable {
    KeyValueStore kv_store;
    String key;
    Integer host_id;
    Socket socket;
    public HandlePTUpdateThread(KeyValueStore kv_store,String key,Integer host_id,Socket socket){
        this.kv_store = kv_store;
        this.key = key;
        this.host_id = host_id;
        this.socket = socket;
    }
    public void run(){
        try{
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            if(kv_store.peer_table.containsKey(key)){
                System.out.println("Key already exists in peer table");
                dos.writeBytes("ERR\n");
            } else{
                kv_store.peer_table.put(key, host_id);
                dos.writeBytes("EXECUTED\n");
            }
            dos.flush();
            dos.close();
        } catch(Exception e){}
    }
}
