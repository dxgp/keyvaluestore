package threads;

import storage.KeyValueStore;

import java.io.DataOutputStream;
import java.net.Socket;

public class HandlePutThread implements Runnable {
    KeyValueStore kv_store;
    String key;
    String value;
    Integer recvd_rand;
    Socket reply_socket;
    DataOutputStream dos;
    public HandlePutThread(KeyValueStore kv_store,String key,String value,Integer recvd_rand,Socket reply_socket){
        this.kv_store = kv_store;
        this.key = key;
        this.value = value;
        this.recvd_rand = recvd_rand;
        this.reply_socket = reply_socket;
    }
    public void run(){
        try{
            dos = new DataOutputStream(reply_socket.getOutputStream());
            if(kv_store.local_store.containsKey(key) || kv_store.peer_table.containsKey(key)){
                dos.writeBytes("NO\n");
                System.out.println("REPLIED NO");
            } else{
                if(kv_store.keys_random_pairs.containsKey(key)){
                    int self_random = kv_store.keys_random_pairs.get(key);
                    if(recvd_rand>self_random){
                        dos.writeBytes("YES\n");
                        System.out.println("REPLIED YES");
                    } else{
                        dos.writeBytes("NO\n");
                        System.out.println("REPLIED NO");
                    }
                } else{
                    dos.writeBytes("YES\n");
                    System.out.println("REPLIED YES");
                }
            }
            dos.flush();
        } catch(Exception e){}
    }
}
