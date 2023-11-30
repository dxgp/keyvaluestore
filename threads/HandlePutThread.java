package threads;

import storage.KeyValueStore;

import java.net.Socket;

/*
 * A thread for handling an incoming PUT request from another node.
 */
public class HandlePutThread implements Runnable {
    KeyValueStore kv_store;
    String key;
    String value;
    Integer recvd_rand;
    Socket reply_socket;
    public HandlePutThread(KeyValueStore kv_store,String key,String value,Integer recvd_rand,Socket reply_socket){
        this.kv_store = kv_store;
        this.key = key;
        this.value = value;
        this.recvd_rand = recvd_rand;
        this.reply_socket = reply_socket;
    }
    public void run(){
        try{
            if(!kv_store.keys_random_pairs.containsKey(key)){
                if(!(kv_store.local_store.containsKey(key) || kv_store.peer_table.containsKey(key))){
                    reply_socket.getOutputStream().write(("YES\n").getBytes());
                } else{
                    reply_socket.getOutputStream().write(("NO\n").getBytes());
                }
                return;
            }
            // If the requested key is already taken by this node or some other node return NO
            if(kv_store.local_store.containsKey(key) || kv_store.peer_table.containsKey(key)){
                reply_socket.getOutputStream().write(("NO\n").getBytes());
            } else{
                // If current node is working on same key resolve clash by comparing random no.s
                if(kv_store.keys_random_pairs.containsKey(key)){
                    int self_random = kv_store.keys_random_pairs.get(key);
                    System.err.println("COMPARING RANDOMS +++++++");
                    if(recvd_rand > self_random){
                        reply_socket.getOutputStream().write(("YES\n").getBytes());
                    } else{
                        reply_socket.getOutputStream().write(("NO\n").getBytes());
                    }
                } else{
                    reply_socket.getOutputStream().write(("YES\n").getBytes());
                }
            }
        } catch(Exception e){}
    }
}
