package storage;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

import java.net.SocketException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashMap;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import threads.SendPutRequestThread;
import threads.SendPTUpdateRequestThread;
import threads.SendDeleteRequestThread;
import threads.SendStoreRequestThread;
import threads.SendExitRequestThread;

import threads.HandlePutThread;
import threads.HandleGetThread;
import threads.HandlePTUpdateThread;
import threads.HandleStoreThread;
import threads.HandleDeleteThread;
import threads.HandleExitThread;

import threads.RequestListenThread;


public class KeyValueStore{
    public ConcurrentHashMap<String,String> local_store;
    public ConcurrentHashMap<String,Integer> peer_table;
    public ConcurrentHashMap<String,Integer> keys_random_pairs;
    public HashMap<Integer, String> peers;
    public ConcurrentHashMap<String, Boolean> voted_on;

    public int host_id;
    public int total_host_count;
    

    public KeyValueStore(int host_id,int total_host_count) throws SocketException{
        this.local_store = new ConcurrentHashMap<String,String>(){};
        this.peer_table = new ConcurrentHashMap<String,Integer>();
        this.keys_random_pairs = new ConcurrentHashMap<String,Integer>();
        this.voted_on = new ConcurrentHashMap<String, Boolean>();
        this.peers = new HashMap<Integer, String>();
        this.total_host_count = total_host_count;
        this.host_id = host_id;


        (new Thread(new RequestListenThread(host_id,this))).start();
    }

    public void initialize_peers(String[] args){
        for(int i = 2; i < 1 + this.total_host_count; i++){
            // host_id:ip is being split here
            String[] host_id_ips = args[i].split(":");
            this.peers.put(Integer.parseInt(host_id_ips[0]), host_id_ips[1]);
        }
        System.out.println("Peer adresses initialised:");
        System.out.println(this.peers);
    }

    // Execute PUT locally and broadcast PTUPDATE message once PUT is done
    public void execute_put(String key,String value){
        ExecutorService broadcast_executor = Executors.newFixedThreadPool(this.total_host_count);
        if (this.voted_on.containsKey(key)) {
            System.out.println("Key is already voted on");
            return;
        } else if (this.local_store.containsKey(key)) {
            System.out.println("Key already exists in local store");
            return;
        } else if (this.peer_table.containsKey(key)) {
            System.out.println("Key exists in peer table");
            return;
        }


        final AtomicInteger count = new AtomicInteger(0);
        int self_random = ThreadLocalRandom.current().nextInt(0, 1000);

        this.keys_random_pairs.put(key, self_random);

        this.peers.forEach((host_id,address)->{
            System.out.println("Sent to host id: " + host_id);
            try {
                broadcast_executor.execute(new SendPutRequestThread(host_id, address, key, value, count, self_random));
            } catch (Exception e) {
                e.printStackTrace();
            }  
        });
        broadcast_executor.shutdown();
        try{broadcast_executor.awaitTermination(300L, TimeUnit.SECONDS);} catch(Exception e){e.printStackTrace();}
        if(count.intValue()==(this.total_host_count - 1)){
            // Request successful
            this.local_store.put(key, value);

            // TODO: CALL execute_ptupdate
            execute_ptupdate(key, this.host_id);

            this.keys_random_pairs.remove(key);
            System.out.println("PUT REQUEST SUCCESSFUL");
        } else{
            //Request unsuccessful
            System.out.println("PUT REQUEST FAILED.");
        }


    }

    // Broadcast PTUPDATE message to tell the peers to update their peer tables for this key
    public void execute_ptupdate(String key,Integer self_host_id){
        ExecutorService broadcast_executor = Executors.newFixedThreadPool(this.total_host_count);
        this.peers.forEach((peer_host_id,address)->{
            try {
                broadcast_executor.execute(new SendPTUpdateRequestThread(peer_host_id, address, key, self_host_id));
            } catch(Exception e){e.printStackTrace();}
        });
        broadcast_executor.shutdown();
        try{broadcast_executor.awaitTermination(300L, TimeUnit.SECONDS);} catch(Exception e){e.printStackTrace();}
    }

    // Retreive the value for key and display it
    public void execute_get(String key){

        // Check in local store
        if (this.local_store.containsKey(key)) {
            System.out.println("Key " + key + " found locally.");
            String value = this.local_store.get(key);
            System.out.println("GET query executed. Value is: " + value);
        } else {
            System.out.println("Looking for key " + key + " in peer table.");

            // Check in peer table
            if (!peer_table.containsKey(key)) {
                System.out.println("Key not found in peer table. Key does not exist in the system.");
            } else {
                int key_holder_host_id = peer_table.get(key);
                String request = "GET "+ key;
                try{
                    // Retreive key holder's ip and port for sending request
                    String holder_address = this.peers.get(key_holder_host_id);
                    int holder_port = 10000 + key_holder_host_id;

                    // Send GET request
                    DatagramSocket send_get_socket = new DatagramSocket();
                    byte[] buf = request.getBytes();
                    InetAddress holder_inet = InetAddress.getByName(holder_address);
                    DatagramPacket packet = new DatagramPacket(buf, buf.length, holder_inet, holder_port);
                    send_get_socket.send(packet);

                    // Receive value from holder host
                    byte[] ack_buf = new byte[500];
                    DatagramPacket ack_packet = new DatagramPacket(ack_buf, ack_buf.length);
                    send_get_socket.receive(ack_packet);
                    String value = new String(ack_packet.getData());
                    System.out.println("GET query executed. Value is: " + value);

                    send_get_socket.close();
                } catch(Exception e){
                    e.printStackTrace();
                }
            }
            
        }
        
    }

    // Show all the keys and their corresponding values in the system
    public void execute_store(){
        ExecutorService broadcast_executor = Executors.newFixedThreadPool(this.total_host_count);
        // HashMap to store combined local stores of all the nodes
        ConcurrentHashMap<String,String> total_map = new ConcurrentHashMap<String,String>();

        // Append this node's local store to total_map
        total_map.putAll(this.local_store);

        // Broadcast store request to receive peers local stores in total_map
        this.peers.forEach((peer_host_id,address)->{
            try {
                broadcast_executor.execute(new SendStoreRequestThread(peer_host_id, address, total_map));
            } catch(Exception e){e.printStackTrace();}
        });
        broadcast_executor.shutdown();
        try{broadcast_executor.awaitTermination(300L, TimeUnit.SECONDS);} catch(Exception e){e.printStackTrace();}

        // Display the table
        System.out.println("**TABLE**");
        total_map.forEach((key,value)->{
            System.out.println(key + "\t \t" +value);
        });
    }
    
    public void execute_delete(String key){
        ExecutorService broadcast_executor = Executors.newFixedThreadPool(this.total_host_count);
        if (!this.local_store.containsKey(key)) {
            System.out.println("Invalid Request: The key " + key + " does not exist at this node.");
            return;
        }

        // Remove the entry for the key from local store
        this.local_store.remove(key);

        // Broadcast delete request to all the peers to delete the entry from their peer table
        this.peers.forEach((peer_host_id,address)->{
            Thread th;
            try {
                broadcast_executor.execute(new SendDeleteRequestThread(peer_host_id, address, key));
            } catch(Exception e){e.printStackTrace();}
        });
        broadcast_executor.shutdown();
        try{broadcast_executor.awaitTermination(300L, TimeUnit.SECONDS);} catch(Exception e){e.printStackTrace();}
        
    }

    public void execute_exit(){
        // Broadcast exit request to all the peers to delete the entries for this host from their peer table
        ExecutorService broadcast_executor = Executors.newFixedThreadPool(this.total_host_count);
        this.peers.forEach((peer_host_id,address)->{
            try {
                broadcast_executor.execute(new SendExitRequestThread(peer_host_id, address, this.host_id));
            } catch (Exception e) {e.printStackTrace();} 
        });
        broadcast_executor.shutdown();
        try{broadcast_executor.awaitTermination(300L, TimeUnit.SECONDS);} catch(Exception e){e.printStackTrace();}
        // Exit program
        System.exit(0);
        
    }

    // Receiving methods. These methods perform the tasks required after a request is received
    public void handle_put(String key, String value, Integer recvd_rand, DatagramSocket socket, InetAddress reply_address, Integer reply_port){
        HandlePutThread hp_thread = new HandlePutThread(this, key, value, recvd_rand, socket, reply_address, reply_port);
        (new Thread(hp_thread)).start();
    }

    public void handle_ptupdate(String key, Integer host_id, DatagramSocket socket, InetAddress reply_address, Integer reply_port){
        HandlePTUpdateThread hpt_thread = new HandlePTUpdateThread(this, key, host_id, socket, reply_address, reply_port);
        (new Thread(hpt_thread)).start();
    }

    public void handle_get(String key, DatagramSocket socket, InetAddress reply_address, Integer reply_port){
        HandleGetThread hg_thread = new HandleGetThread(this, key, socket, reply_address, reply_port);
        (new Thread(hg_thread)).start();
    }
    
    public void handle_store(DatagramSocket socket, InetAddress reply_address, Integer reply_port){
        HandleStoreThread hs_thread = new HandleStoreThread(this, socket, reply_address, reply_port);
        (new Thread(hs_thread)).start();
    }

    public void handle_delete(String key, DatagramSocket socket, InetAddress reply_address, Integer reply_port){
        HandleDeleteThread hs_thread = new HandleDeleteThread(this, key, socket, reply_address, reply_port);
        (new Thread(hs_thread)).start();
    }

    public void handle_exit(String host_id, DatagramSocket socket, InetAddress reply_address, Integer reply_port){
        HandleExitThread he_thread = new HandleExitThread(this, host_id, socket, reply_address, reply_port);
        (new Thread(he_thread)).start();
    }
    
}