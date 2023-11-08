package threads;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

import storage.KeyValueStore;

public class HandleGetThread implements Runnable {
    KeyValueStore kv_store;
    String key;

    DatagramSocket socket;
    InetAddress reply_address;
    Integer reply_port;

    public HandleGetThread(KeyValueStore kv_store, String key, DatagramSocket socket, InetAddress reply_address, Integer reply_port){
        this.kv_store = kv_store;
        this.key = key;
        this.socket = socket;
        this.reply_address = reply_address;
        this.reply_port = reply_port;
    }

    private void sendPacket(String response) throws IOException {
        System.out.println("SENDING VALUE FOR GET REQ: " + response);
        byte[] buf = response.getBytes();
        DatagramPacket response_packet = new DatagramPacket(buf, buf.length, this.reply_address, this.reply_port);
        this.socket.send(response_packet);
    }

    public void run(){
        try{

            if(this.kv_store.local_store.containsKey(this.key)){
                String value = this.kv_store.local_store.get(this.key);
                this.sendPacket(value);
            } else{
                System.out.println("DOES NOT CONTAIN KEY");
                this.sendPacket("ERROR: HOST " + kv_store.host_id + " DOES NOT CONTAIN THE KEY " + key);
            }

        } catch(Exception e){}
    }
}
