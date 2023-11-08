package threads;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

import storage.KeyValueStore;

public class HandlePTUpdateThread implements Runnable {
    KeyValueStore kv_store;
    String key;
    Integer host_id;

    DatagramSocket socket;
    InetAddress reply_address;
    Integer reply_port;

    public HandlePTUpdateThread(KeyValueStore kv_store, String key, Integer host_id, DatagramSocket socket, InetAddress reply_address, Integer reply_port){
        this.kv_store = kv_store;
        this.key = key;
        this.host_id = host_id;
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
            if(kv_store.peer_table.containsKey(key)){
                System.out.println("Key already exists in peer table");
                this.sendPacket("ERR");
            } else{
                kv_store.peer_table.put(key, host_id);
                this.sendPacket("EXECUTED");
            }

        } catch(Exception e){}
    }
}
