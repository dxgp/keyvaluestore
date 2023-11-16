package threads;

import java.net.Socket;
import java.util.Map;

import storage.KeyValueStore;

public class HandleExitThread implements Runnable{
    KeyValueStore kv_store;
    int host_id;
    Socket sock;
    public HandleExitThread(KeyValueStore kv_store,int host_id,Socket sock){
        this.kv_store = kv_store;
        this.host_id = host_id;
        this.sock = sock;
    }
    public void run(){
        for(Map.Entry<String,Integer> entry: kv_store.peer_table.entrySet()){
            if(entry.getValue() == host_id){
                kv_store.peer_table.remove(entry.getKey());
            }
        }
        kv_store.total_host_count = kv_store.total_host_count - 1;
        kv_store.peers.remove(host_id);
        try{
            sock.getOutputStream().write(("EXECUTED\n").getBytes());
        } catch(Exception e){}
    }
}
