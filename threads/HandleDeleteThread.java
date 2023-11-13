package threads;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

import storage.KeyValueStore;

public class HandleDeleteThread implements Runnable{
    KeyValueStore kv_store;
    String key;

    DatagramSocket socket;
    InetAddress reply_address;
    Integer reply_port;

    public HandleDeleteThread(KeyValueStore kv_store, String key, DatagramSocket socket, InetAddress reply_address, Integer reply_port){
        this.kv_store = kv_store;
        this.key = key;
        this.socket = socket;
        this.reply_address = reply_address;
        this.reply_port = reply_port;
    }

    private void sendPacket(String response) throws IOException {
        System.out.println("SENDING VALUE FOR DEL REQ: " + response);
        byte[] buf = response.getBytes();
        DatagramPacket response_packet = new DatagramPacket(buf, buf.length, this.reply_address, this.reply_port);
        this.socket.send(response_packet);
    }

    public void run(){
        try{
            // Remove key from peer table
            kv_store.peer_table.remove(key);

            // Send success response
            this.sendPacket("EXECUTED");
        } catch(Exception e){
            System.out.println(e);
            try {
                this.sendPacket("ERROR");
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
    }
}
