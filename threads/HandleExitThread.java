package threads;

import java.net.Socket;
import java.util.Map;

import storage.KeyValueStore;

/*
 * A thread for handling an incoming EXIT request from another node.
 */
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
        //delete all associated peer table entries for that host
        for(Map.Entry<String,Integer> entry: kv_store.peer_table.entrySet()){
            if(entry.getValue() == host_id){
                kv_store.peer_table.remove(entry.getKey());
            }
        }
        kv_store.total_host_count = kv_store.total_host_count - 1; //decrement total host count
        kv_store.peers.remove(host_id); //remove node entry from peers
        try{
            sock.getOutputStream().write(("EXECUTED\n").getBytes()); //write to stream
        } catch(Exception e){}
    }
}
