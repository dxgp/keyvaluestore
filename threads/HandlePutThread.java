package threads;

import storage.KeyValueStore;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class HandlePutThread implements Runnable {
    KeyValueStore kv_store;
    String key;
    String value;
    Integer recvd_rand;
    DatagramSocket socket;
    InetAddress reply_address;
    Integer reply_port;

    public HandlePutThread(KeyValueStore kv_store, String key, String value, Integer recvd_rand, DatagramSocket socket, InetAddress reply_address, Integer reply_port){
        this.kv_store = kv_store;
        this.key = key;
        this.value = value;
        this.recvd_rand = recvd_rand;
        this.socket = socket;
        this.reply_address = reply_address;
        this.reply_port = reply_port;
    }

    private void sendPacket(String response) throws IOException {
        byte[] buf = response.getBytes();
        DatagramPacket response_packet = new DatagramPacket(buf, buf.length, this.reply_address, this.reply_port);
        this.socket.send(response_packet);
    }

    public void run(){
        try{
            if(!kv_store.keys_random_pairs.containsKey(key)){
                if(!(kv_store.local_store.containsKey(key) || kv_store.peer_table.containsKey(key))){
                    this.sendPacket("YES");
                } else{
                    this.sendPacket("NO");
                }
                return;
            }
            // If the requested key is already taken by this node or some other node return NO
            if(kv_store.local_store.containsKey(key) || kv_store.peer_table.containsKey(key)){
                this.sendPacket("NO");
            } else{
                // If current node is working on same key resolve clash by comparing random no.s
                if(kv_store.keys_random_pairs.containsKey(key)){
                    int self_random = kv_store.keys_random_pairs.get(key);
                    System.err.println("COMPARING RANDOMS +++++++");
                    if(recvd_rand > self_random){
                        this.sendPacket("YES");
                    } else{
                        this.sendPacket("NO");
                    }
                } else{
                    this.sendPacket("YES");
                }
            }
        } catch(Exception e){}
    }
}
