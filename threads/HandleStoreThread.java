package threads;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Map;

import storage.KeyValueStore;

public class HandleStoreThread implements Runnable {
    KeyValueStore kv_store;
    DatagramSocket socket;
    InetAddress reply_address;
    Integer reply_port;

    public HandleStoreThread(KeyValueStore kv_store, DatagramSocket socket, InetAddress reply_address, Integer reply_port){
        this.kv_store = kv_store;
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
            String final_data = "";
            for (Map.Entry<String,String> entry : kv_store.local_store.entrySet()) {
                final_data = final_data + entry.getKey() + " " + entry.getValue() + "|";
            }

            System.out.println("SENDING STORE RESPONSE: " + final_data);

            this.sendPacket(final_data);
        } catch(Exception e){}
    }
}
