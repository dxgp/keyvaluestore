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
            if (kv_store.host_id == 2) {
                System.out.println("SLEEPING===");
                Thread.sleep(50000);
            }

            System.out.println("AWAKE========");

            // If the requested key is already taken by this node or some other node return NO
            if(kv_store.local_store.containsKey(key) || kv_store.peer_table.containsKey(key)){
                this.sendPacket("NO");
                System.out.println("REPLIED NO");
            } else{
                System.out.println("IN ELSE fiRST");
                // If current node is working on same key resolve clash by comparing random no.s
                if(kv_store.keys_random_pairs.containsKey(key)){
                    System.out.println("COMPARING RANDOM");
                    int self_random = kv_store.keys_random_pairs.get(key);
                    if(recvd_rand > self_random){
                        System.out.println("RECVD RAND: " + recvd_rand + " SELF RAND: " + self_random);
                        System.out.println("RECVD RAND > SELF RAND; REPLYING YES");
                        this.sendPacket("YES");
                        // System.out.println("REPLIED YES");
                    } else{
                        System.out.println("RECVD RAND: " + recvd_rand + " SELF RAND: " + self_random);
                        System.out.println("RECVD RAND < SELF RAND; REPLYING NO");
                        this.sendPacket("NO");
                        // System.out.println("REPLIED NO");
                    }
                } else{
                    System.out.println("NOT WORKING ON KEY; REPLYING YES");
                    this.sendPacket("YES");
                    System.out.println("REPLIED YES");
                }
            }

        } catch(Exception e){}
    }
}
