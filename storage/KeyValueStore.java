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

    public KeyValueStore(int host_id,int total_host_count){
        this.local_store = new ConcurrentHashMap<String,String>(){};
        this.peer_table = new ConcurrentHashMap<String,Integer>();
        this.keys_random_pairs = new ConcurrentHashMap<String,Integer>();
        this.peers = new ConcurrentHashMap<Integer,Socket>();
        this.voted_on = new ConcurrentHashMap<String,Boolean>();
        this.total_host_count = total_host_count;
        this.host_id = host_id;
        this.query_executor = Executors.newFixedThreadPool(10);
        (new Thread(new RequestListenThread(host_id,this))).start();
    }
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
        int self_random = ThreadLocalRandom.current().nextInt(0, 1000);
        this.peers.forEach((host_id,sock)->{
            System.out.println("Sent to host id:"+host_id);
            broadcast_executor.execute(new SendPutRequestThread(sock, key, value, count,self_random));
        });
        broadcast_executor.shutdown();
        try{
            broadcast_executor.awaitTermination(300L, TimeUnit.SECONDS);
        } catch(Exception e){e.printStackTrace();}
        System.out.println("COUNT VAL:"+count.intValue());
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
        int key_holder = peer_table.get(key);
        Socket sock = this.peers.get(key_holder);
        String request = "GET "+key + "\n";
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
        ConcurrentHashMap<String,String> total_map = new ConcurrentHashMap<String,String>();
        ExecutorService broadcast_executor = Executors.newFixedThreadPool(this.total_host_count);
        this.peers.forEach((h_id,sock)->{
            broadcast_executor.execute(new SendStoreRequestThread(sock,h_id,total_map));
        });
        broadcast_executor.shutdown();
        try{
            broadcast_executor.awaitTermination(300L, TimeUnit.SECONDS);
        } catch(Exception e){e.printStackTrace();}
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
        ExecutorService broadcast_executor = Executors.newFixedThreadPool(this.total_host_count);
        this.peers.forEach((h_id,sock)->{
            broadcast_executor.execute(new SendExitRequestThread(sock,this.host_id));
        });
        broadcast_executor.shutdown();
        try{
            broadcast_executor.awaitTermination(300L, TimeUnit.SECONDS);
        } catch(Exception e){e.printStackTrace();}
        System.exit(0);
    }
    
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