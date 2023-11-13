package threads;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

import storage.KeyValueStore;

public class HandleExitThread implements Runnable{
    KeyValueStore kv_store;
    String host_id;

    DatagramSocket socket;
    InetAddress reply_address;
    Integer reply_port;

    public HandleExitThread(KeyValueStore kv_store, String host_id, DatagramSocket socket, InetAddress reply_address, Integer reply_port){
        this.kv_store = kv_store;
        this.host_id = host_id;
        this.socket = socket;
        this.reply_address = reply_address;
        this.reply_port = reply_port;
    }

    private void sendPacket(String response) throws IOException {
        System.out.println("SENDING VALUE FOR EXIT REQ: " + response);
        byte[] buf = response.getBytes();
        DatagramPacket response_packet = new DatagramPacket(buf, buf.length, this.reply_address, this.reply_port);
        this.socket.send(response_packet);
    }

    public void run(){
        try{
            // Remove entries from peer table for the host id
            Integer host_to_remove = Integer.parseInt(this.host_id);
            kv_store.peer_table.entrySet().removeIf(entry -> (entry.getValue() == host_to_remove));

            // Remove the host from peers list
            kv_store.peers.remove(host_to_remove);

            // Decrement total host count
            kv_store.total_host_count -= 1;

            System.out.println("AFTER REMOVING HOST " + this.host_id + " FROM PEER TABLE:");
            System.out.println(kv_store.peer_table.toString());

            System.out.println("PEERS: ");
            System.out.println(kv_store.peers.toString());

            // Send success response
            this.sendPacket("OK");
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
