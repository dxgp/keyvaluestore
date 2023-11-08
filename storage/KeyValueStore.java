package storage;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
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


public class KeyValueStore{
    public ConcurrentHashMap<String,String> local_store;
    public ConcurrentHashMap<String,Integer> peer_table;
    public ConcurrentHashMap<String,Integer> keys_random_pairs;
    public ConcurrentHashMap<Integer,ArrayList<Object>> peers;
    ExecutorService query_executor;

    public int host_id;
    public int total_host_count;

    public KeyValueStore(int host_id,int total_host_count,int key_count){
        this.local_store = new ConcurrentHashMap<String,String>(){};
        this.peer_table = new ConcurrentHashMap<String,Integer>();
        this.keys_random_pairs = new ConcurrentHashMap<String,Integer>();
        this.peers = new ConcurrentHashMap<Integer,ArrayList<Object>>();
        this.total_host_count = total_host_count;
        this.host_id = host_id;
        this.query_executor = Executors.newFixedThreadPool(10);
        (new Thread(new RequestListenThread(host_id,this))).start();
    }
    public void initialize_peers(){
        for(int i=0;i < this.total_host_count;i++){
            if(i!=this.host_id){
                boolean connected = false;
                while(connected!=true){
                    try{
                        Socket out_socket = new Socket("localhost",10000+i);
                        System.out.println("REACHED HERE.");
                        DataOutputStream out_stream = new DataOutputStream(out_socket.getOutputStream());
                        BufferedReader in_stream = new BufferedReader(new InputStreamReader(out_socket.getInputStream()));
                        System.out.println("REACHED HERE TOO.");
                        ArrayList<Object> peer_streams = new ArrayList<Object>(Arrays.asList(out_stream,in_stream));
                        this.peers.put(i, peer_streams);
                        connected = true;
                        out_socket.setPerformancePreferences(1, 0, 2); // here latency is 0.
                        out_socket.setTcpNoDelay(true); // for client as well as server.
                    } catch(Exception e){}
                }
                System.out.println("Conn established.");
            }
        }
    }
    public void execute_put(String key,String value){
        final AtomicInteger count = new AtomicInteger(0);
        ExecutorService broadcast_executor = Executors.newFixedThreadPool(this.total_host_count);
        int self_random = ThreadLocalRandom.current().nextInt(0, 1000);
        this.peers.forEach((host_id,peer_streams)->{
            System.out.println("Sent to host id:"+host_id);
            DataOutputStream dos = (DataOutputStream)peer_streams.get(0);
            BufferedReader in = (BufferedReader)peer_streams.get(1);
            broadcast_executor.execute(new SendPutRequestThread(dos, in, key, value, count,self_random));
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
        System.out.println("KEY HOLDER IS:"+key_holder);
        DataOutputStream dos = (DataOutputStream)(peers.get(key_holder)).get(0);
        BufferedReader in = (BufferedReader)(peers.get(key_holder)).get(1);
        String request = "GET "+key + "\n";
        try{
            dos.writeBytes(request);
            System.out.println("BYTES WRITTEN");
            while(!in.ready());
            char buf = '\0';
            String response = "";
            while(!(buf == '\n')){
                buf = (char) in.read();
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
        this.peers.forEach((h_id,peer_streams)->{
            DataOutputStream dos = (DataOutputStream)peer_streams.get(0);
            BufferedReader in = (BufferedReader)peer_streams.get(1);
            broadcast_executor.execute(new SendStoreRequestThread(dos,in,h_id,total_map));
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
        ExecutorService broadcast_executor = Executors.newFixedThreadPool(this.total_host_count);
        this.peers.forEach((h_id,peer_streams)->{
            DataOutputStream dos = (DataOutputStream)peer_streams.get(0);
            BufferedReader in = (BufferedReader)peer_streams.get(1);
            broadcast_executor.execute(new SendPTUpdateRequestThread(dos,in,key,host_id));
        });
        broadcast_executor.shutdown();
        try{
            broadcast_executor.awaitTermination(300L, TimeUnit.SECONDS);
        } catch(Exception e){e.printStackTrace();}
    }
    public void execute_delete(String key){
        ExecutorService broadcast_executor = Executors.newFixedThreadPool(this.total_host_count);
        this.local_store.remove(key);
        this.peers.forEach((h_id,peer_streams)->{
            DataOutputStream dos = (DataOutputStream)peer_streams.get(0);
            BufferedReader in = (BufferedReader)peer_streams.get(1);
            broadcast_executor.execute(new SendDeleteRequestThread(dos,in,key));
        });
        broadcast_executor.shutdown();
        try{
            broadcast_executor.awaitTermination(300L, TimeUnit.SECONDS);
        } catch(Exception e){e.printStackTrace();}
    }
    public void handle_put(String key,String value,Integer recvd_rand,Socket socket){
        //HandlePutThread hp_thread = new HandlePutThread(this,key, value,recvd_rand,socket);
        query_executor.execute(new HandlePutThread(this,key, value,recvd_rand,socket));
        //(new Thread(hp_thread)).start();
    }
    public void handle_get(String key,Socket socket){
        // HandleGetThread hg_thread = new HandleGetThread(this,key,socket);
        query_executor.execute(new HandleGetThread(this,key,socket));
        // (new Thread(hg_thread)).start();
    }
    public void handle_store(Socket socket){
        // HandleStoreThread hs_thread = new HandleStoreThread(this,socket);
        query_executor.execute(new HandleStoreThread(this,socket));
        // (new Thread(hs_thread)).start();
    }
    public void handle_delete(String key,Socket socket){
        // HandleDeleteThread hs_thread = new HandleDeleteThread(this,key,socket);
        query_executor.execute(new HandleDeleteThread(this,key,socket));
        // (new Thread(hs_thread)).start();
    }
    public void handle_ptupdate(String key,Integer host_id,Socket socket){
        // HandlePTUpdateThread hpt_thread = new HandlePTUpdateThread(this,key,host_id,socket);
        query_executor.execute(new HandlePTUpdateThread(this,key,host_id,socket));
        // (new Thread(hpt_thread)).start();
    }
}