package threads;

import storage.KeyValueStore;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Arrays;

public class RequestListenThread implements Runnable{
    int host_id;
    KeyValueStore kv_store;

    // UDP
    private DatagramSocket socket;
    public int server_port;
    private byte[] buf = new byte[256];

    public RequestListenThread(int host_id,KeyValueStore kv_store){
        try{
            this.host_id = host_id;
            this.kv_store = kv_store;
            // UDP
            this.server_port = 10000 + this.host_id;
            this.socket = new DatagramSocket(this.server_port);
        } catch(Exception e){}
    }
    public void run(){
        System.out.println("Now listening for incoming connections");
        try{
            
            while(true){
                // Receive data
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);

                // Get src ip, port of the received packet to send a reply
                InetAddress reply_address = packet.getAddress();
                Integer reply_port = Integer.valueOf(packet.getPort());

                // Retreive the query from the packet into string
                String query = new String(packet.getData(), 0, packet.getLength());

                // Split the query into query terms
                String[] query_terms = query.trim().split("\\ ");
                System.out.println("QUERY RECEIVED:"+Arrays.toString(query_terms));

                if(query_terms[0].equals("PUT")){
                    String key = query_terms[1];
                    String value = query_terms[2];
                    int recvd_rand = Integer.parseInt(query_terms[3]);
                    kv_store.handle_put(key, value, recvd_rand, socket, reply_address, reply_port);
                
                // TODO: Handle other query types
                
                // } else if(query_terms[0].equals("GET")){
                //     String key = query_terms[1];
                //     kv_store.handle_get(key, socket);
                // } else if(query_terms[0].equals("STORE")){
                //     kv_store.handle_store(socket);
                // } else if(query_terms[0].equals("DELETE")){
                //     String key = query_terms[1];
                //     kv_store.handle_delete(key, socket);
                } else if(query_terms[0].equals("PTUPDATE")){
                    String key = query_terms[1];
                    int host_id = Integer.parseInt(query_terms[2]);
                    kv_store.handle_ptupdate(key, host_id, socket, reply_address, reply_port);
                } else{
                    System.out.println("INVALID QUERY RECEIVED");
                }
            }
        } catch(Exception e){}
    }
}
