package storage;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import threads.SendPutRequestThread;
import threads.SendPTUpdateRequestThread;
import threads.SendDeleteRequestThread;
import threads.SendStoreRequestThread;
import threads.HandlePutThread;
import threads.HandleGetThread;
import threads.HandlePTUpdateThread;
import threads.HandleStoreThread;
import threads.HandleDeleteThread;
import threads.RequestListenThread;
import threads.SendExitRequestThread;
import threads.HandleExitThread;


public class KeyValueStore{
    public ConcurrentHashMap<String,String> local_store;
    public ConcurrentHashMap<String,Integer> peer_table;
    public ConcurrentHashMap<String,Integer> keys_random_pairs;
    public ConcurrentHashMap<Integer,Socket> peers;

    public ConcurrentHashMap<String,Boolean> voted_on;

    ExecutorService query_executor;

    public int host_id;
    public int total_host_count;

    /*
     * Basic class that represents the state of the DB at one node.
     */
    public KeyValueStore(int host_id,int total_host_count){
        this.local_store = new ConcurrentHashMap<String,String>(){}; // the local store of the node
        this.peer_table = new ConcurrentHashMap<String,Integer>(); // the peer table that each node must maintain
        this.keys_random_pairs = new ConcurrentHashMap<String,Integer>(); // random number generated for each key for conflict resolution
        this.peers = new ConcurrentHashMap<Integer,Socket>(); // integer-socket pairs for connection
        this.voted_on = new ConcurrentHashMap<String,Boolean>(); // key-boolean pairs that stores if a node has voted yes/no for a particular key
        this.total_host_count = total_host_count; // total host count
        this.host_id = host_id; // host id of this node given as a command line argument
        this.query_executor = Executors.newFixedThreadPool(10); // a thread pool to make requests concurrently executable
        (new Thread(new RequestListenThread(host_id,this))).start(); // start listening for requests
    }
    
    /*
     * A function to create the hostid-socket pairs for sending/receiving data later on.
     */
    public void initialize_peers(String[] ip_list){
        for(int i=0;i < this.total_host_count;i++){
            if(i!=this.host_id){
                boolean connected = false;
                while(connected!=true){
                    try{
                        Socket out_socket = new Socket(ip_list[i],10000+i);
                        this.peers.put(i,out_socket);
                        connected = true;
                    } catch(Exception e){}
                }
            }
        }
        System.out.println("Conn established.");
    }

    /*
     * Below are the execute functions. They dictate how a particular function is executed on the node.
     * So, if a node wants to execute put request, it calls execute_put.
     */


    public void execute_put(String key,String value){
        if(voted_on.containsKey(key)==true){
            System.out.println("Key exists in voted_on set. PUT not allowed.");
            return;
        } else if(local_store.containsKey(key) || peer_table.containsKey(key)){
            System.out.println("Key already exists.");
            return;
        }
        final AtomicInteger count = new AtomicInteger(0);
        ExecutorService broadcast_executor = Executors.newFixedThreadPool(this.total_host_count);
        int self_random = ThreadLocalRandom.current().nextInt(0, 1000); //generate a random number for resolving conflicts
        keys_random_pairs.put(key, self_random);
        //broadcasting the request. (Multithreaded)
        this.peers.forEach((host_id,sock)->{
            System.out.println("Sent to host id:"+host_id);
            broadcast_executor.execute(new SendPutRequestThread(sock, key, value, count,self_random));
        });
        broadcast_executor.shutdown();
        try{
            broadcast_executor.awaitTermination(300L, TimeUnit.SECONDS);
        } catch(Exception e){e.printStackTrace();}
        System.out.println("COUNT VAL:"+count.intValue());

        // Checkiing if all nodes agree
        if(count.intValue()==(this.total_host_count - 1)){
            // Request successful
            this.local_store.put(key, value);
            execute_ptupdate(key, this.host_id);
            keys_random_pairs.remove(key);
            System.out.println("PUT REQUEST SUCCESSFUL");
        } else{
            //Request unsuccessful
            System.out.println("PUT REQUEST FAILED.");
        }
    }
    public void execute_get(String key){
        System.out.println("Executing GET "+key);
        //check if the key is in its own local store.
        if(local_store.containsKey(key)){
            System.out.println("GET query executed. Returned "+local_store.get(key));
            return;
        }
        // if not, get the key holder from the peer table
        int key_holder = peer_table.get(key);
        Socket sock = this.peers.get(key_holder);
        String request = "GET "+key + "\n";
        // send the get request to the key holder
        try{
            sock.getOutputStream().write(request.getBytes());
            char buf = '\0';
            String response = "";
            while(!(buf == '\n')){
                buf = (char) sock.getInputStream().read();
                response += buf;
            }
            System.out.println("GET query executed. Returned "+response);
        } catch(Exception e){
            System.out.println("Exception occured when writing query to output stream in SendPutRequestThread");
        }
    }
    public void execute_store(){
        ConcurrentHashMap<String,String> total_map = new ConcurrentHashMap<String,String>(); // map to hold the total output of STORE
        ExecutorService broadcast_executor = Executors.newFixedThreadPool(this.total_host_count);
        //broadcast the request
        this.peers.forEach((h_id,sock)->{
            broadcast_executor.execute(new SendStoreRequestThread(sock,h_id,total_map));
        });
        broadcast_executor.shutdown();
        try{
            broadcast_executor.awaitTermination(300L, TimeUnit.SECONDS);
        } catch(Exception e){e.printStackTrace();}
        //printing
        System.out.println("**TABLE**");
        total_map.forEach((key,value)->{
            System.out.println(key + "\t \t" +value);
        });
    }
    public void execute_ptupdate(String key,Integer host_id){
        System.out.println("NOW SENDING PTUPDATE");
        ExecutorService broadcast_executor = Executors.newFixedThreadPool(this.total_host_count);
        this.peers.forEach((h_id,sock)->{            
            broadcast_executor.execute(new SendPTUpdateRequestThread(sock,key,host_id));
        });
        broadcast_executor.shutdown();
        try{
            broadcast_executor.awaitTermination(300L, TimeUnit.SECONDS);
            System.out.println("PTUPDATE EXECUTED");
            voted_on.remove(key);
        } catch(Exception e){e.printStackTrace();}
    }
    public void execute_delete(String key){
        System.out.println("Executing DELETE "+key);
        ExecutorService broadcast_executor = Executors.newFixedThreadPool(this.total_host_count);
        this.local_store.remove(key);
        // broadcast the request
        this.peers.forEach((h_id,sock)->{
            broadcast_executor.execute(new SendDeleteRequestThread(sock,key));
        });
        broadcast_executor.shutdown();
        try{
            broadcast_executor.awaitTermination(300L, TimeUnit.SECONDS);
        } catch(Exception e){e.printStackTrace();}
        System.out.println("DELETE EXECUTED");
    }
    public void execute_exit(){
        System.out.println("EXECUTING EXIT");
        //broadcast the request
        ExecutorService broadcast_executor = Executors.newFixedThreadPool(this.total_host_count);
        this.peers.forEach((h_id,sock)->{
            broadcast_executor.execute(new SendExitRequestThread(sock,this.host_id));
        });
        broadcast_executor.shutdown();
        try{
            broadcast_executor.awaitTermination(300L, TimeUnit.SECONDS);
        } catch(Exception e){e.printStackTrace();}
        System.exit(0); //node exit
    }

    /*
     * Functions that dictate how incoming requests are handled. New threads are created for their handling.
     */
    
    public void handle_put(String key,String value,Integer recvd_rand,Socket socket){
        query_executor.execute(new HandlePutThread(this,key, value,recvd_rand,socket));
    }
    public void handle_get(String key,Socket socket){
        query_executor.execute(new HandleGetThread(this,key,socket));
    }
    public void handle_store(Socket socket){
        query_executor.execute(new HandleStoreThread(this,socket));
    }
    public void handle_delete(String key,Socket socket){
        query_executor.execute(new HandleDeleteThread(this,key,socket));
    }
    public void handle_ptupdate(String key,Integer host_id,Socket socket){
        query_executor.execute(new HandlePTUpdateThread(this,key,host_id,socket));
    }
    public void handle_exit(int host_id,Socket socket){
        query_executor.execute(new HandleExitThread(this,host_id,socket));
    }
}