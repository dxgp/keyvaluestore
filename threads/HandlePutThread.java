package threads;

import storage.KeyValueStore;

import java.net.Socket;

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
            if(kv_store.local_store.containsKey(key) || kv_store.peer_table.containsKey(key)){
                reply_socket.getOutputStream().write(("NO\n").getBytes());
                System.out.println("REPLIED NO");
            } else{
                if(kv_store.keys_random_pairs.containsKey(key)){
                    int self_random = kv_store.keys_random_pairs.get(key);
                    if(recvd_rand>self_random){
                        reply_socket.getOutputStream().write(("YES\n").getBytes());
                        System.out.println("REPLIED YES");
                    } else{
                        reply_socket.getOutputStream().write(("NO\n").getBytes());
                        System.out.println("REPLIED NO");
                    }
                } else{
                    reply_socket.getOutputStream().write(("YES\n").getBytes());
                    System.out.println("REPLIED YES");
                }
            }
            reply_socket.getOutputStream().flush();
        } catch(Exception e){e.printStackTrace();}
    }
}
