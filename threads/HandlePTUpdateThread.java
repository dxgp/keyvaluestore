package threads;

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
            if(kv_store.peer_table.containsKey(key)){
                System.out.println("Key already exists in peer table");
                socket.getOutputStream().write(("ERR\n").getBytes());
            } else{
                kv_store.peer_table.put(key, host_id);
                socket.getOutputStream().write(("EXECUTED\n").getBytes());
            }
            socket.getOutputStream().flush();
        } catch(Exception e){e.printStackTrace();}
    }
}
