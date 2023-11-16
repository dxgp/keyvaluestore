package storage;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import service.StorageService;
import threads.SendPutRequestThread;
import threads.SendPTUpdateThread;
import java.util.concurrent.atomic.AtomicInteger;
import threads.SendStoreRequestThread;
import threads.DeleteRequestThread;
import threads.SendExitRequestThread;


public class KeyValueStore implements StorageService{
    public ConcurrentHashMap<String,String> local_store;
    public ConcurrentHashMap<String,Integer> peer_table;
    public ConcurrentHashMap<String,Integer> keys_random_pairs;
    ConcurrentHashMap<Integer,StorageService> host_stubs;
    ConcurrentHashMap<String,Boolean> voted_on;
    Registry rmi_registry;
    public int host_id;
    public int total_host_count;
    public KeyValueStore(int host_id,int total_host_count,int registry_port){
        this.local_store = new ConcurrentHashMap<>();
        this.peer_table = new ConcurrentHashMap<>();
        this.keys_random_pairs = new ConcurrentHashMap<>();
        this.host_stubs = new ConcurrentHashMap<>();
        this.voted_on = new ConcurrentHashMap<String,Boolean>();
        this.total_host_count = total_host_count;
        this.host_id = host_id;
        try{
            this.rmi_registry = LocateRegistry.getRegistry("localhost",registry_port);
            StorageService stub = (StorageService) UnicastRemoteObject.exportObject(this, 0);
            this.rmi_registry.bind("h"+this.host_id, stub);
            System.out.println("Connected to RMI Registry");
        } catch(Exception e){
            System.out.println("Cannot connect to RMI registry. Did you forget to start it?");
            e.printStackTrace();
        }
    }
    // A function to attempt connecting to all hosts before proceeding with requests...
    public void initialize_peers(){
        System.out.println("Attempting to connect to host.");
        for(int i=0;i<total_host_count;i++){
            if(i!=host_id){
                boolean connected = false;
                while(connected!=true){
                    try{
                        StorageService stub = (StorageService) rmi_registry.lookup("h"+i);
                        if(stub.test_call().equals("ONLINE")){
                            connected = true;
                            host_stubs.put(i,stub);
                        }
                    } catch(Exception e){}
                }
            }
        }
        System.out.println("Connected to all hosts.Now proceeding...");
    }

    //Implementations of local calls for the given operations
    public void execute_put(String key,String value){
        if(voted_on.containsKey(key)==true){
            System.out.println("Key exists in voted_on set. PUT not allowed.");
            return;
        } else if(local_store.containsKey(key) || peer_table.containsKey(key)){
            System.out.println("Key already exists.");
            return;
        }
        System.out.println("Executing PUT "+key+" "+value);
        int self_random = ThreadLocalRandom.current().nextInt(0, 1000);
        final AtomicInteger count = new AtomicInteger(0);
        ExecutorService broadcast_executor = Executors.newFixedThreadPool(this.total_host_count);
        keys_random_pairs.put(key,self_random);
        this.host_stubs.forEach((host_id,host_stub)->{
            broadcast_executor.execute(new SendPutRequestThread(key, value, host_id, host_stub,count,self_random));
        });
        try{
            broadcast_executor.shutdown();
            broadcast_executor.awaitTermination(300L, TimeUnit.SECONDS);
        } catch(Exception e){
            System.out.println("An exception occured in broadcast_executor.awaitTermination");
        }
        if(count.intValue()==total_host_count-1){
            local_store.put(key, value);
            execute_ptupdate(key,host_id);
            keys_random_pairs.remove(key);
        } else{
            System.out.println("PUT REQUEST FAILED SINCE SOME NODES DID NOT AGREE");
        }
    }
    public void execute_get(String key){
        System.out.println("Executing GET "+key);
        int key_holder = peer_table.get(key);
        StorageService host_stub = host_stubs.get(key_holder);
        try{
            System.out.println("GET RESULT:"+host_stub.get(key));
        } catch(Exception e){
            System.out.println("An exception occured during get call.");
        }
    }
    public void execute_store(){
        System.out.println("Executing STORE");
        ConcurrentHashMap<String,String> combined_table = new ConcurrentHashMap<String,String>();
        ExecutorService broadcast_executor = Executors.newFixedThreadPool(this.total_host_count);
        this.host_stubs.forEach((host_id,host_stub)->{
            broadcast_executor.execute(new SendStoreRequestThread(host_stub,combined_table));
        });
        try{
            broadcast_executor.shutdown();
            broadcast_executor.awaitTermination(300L, TimeUnit.SECONDS);
        } catch(Exception e){
            System.out.println("An exception occured in broadcast_executor.awaitTermination");
        }
        System.out.println("***TABLE***");
        combined_table.forEach((key,value)->{
            System.out.println(key + "\t \t"+value);
        });
    }
    public void execute_delete(String key){
        System.out.println("Executing DELETE "+key);
        this.local_store.remove(key);
        ExecutorService broadcast_executor = Executors.newFixedThreadPool(this.total_host_count);
        this.host_stubs.forEach((host_id,host_stub)->{
            broadcast_executor.execute(new DeleteRequestThread(host_stub,key));
        });
        try{
            broadcast_executor.shutdown();
            broadcast_executor.awaitTermination(300L, TimeUnit.SECONDS);
        } catch(Exception e){
            System.out.println("An exception occured in broadcast_executor.awaitTermination");
        }
    }
    public void execute_ptupdate(String key,Integer host_id){
        System.out.println("Executing PTUPDATE");
        ExecutorService broadcast_executor = Executors.newFixedThreadPool(this.total_host_count);
        this.host_stubs.forEach((h_id,host_stub)->{
            broadcast_executor.execute(new SendPTUpdateThread(key,host_id,host_stub));
        });
        try{
            broadcast_executor.shutdown();
            broadcast_executor.awaitTermination(300L, TimeUnit.SECONDS);
        } catch(Exception e){
            System.out.println("An exception occured in broadcast executor while broadcasting PTUPDATE");
        }
    }
    public void execute_exit(){
        ExecutorService broadcast_executor = Executors.newFixedThreadPool(this.total_host_count);
        this.host_stubs.forEach((h_id,host_stub)->{
            broadcast_executor.execute(new SendExitRequestThread(host_stub,host_id));
        });
        try{
            broadcast_executor.shutdown();
            broadcast_executor.awaitTermination(300L, TimeUnit.SECONDS);
            System.exit(0);
        } catch(Exception e){
            System.out.println("An exception occured in broadcast executor while broadcasting EXIT");
        }
    } 

    
    //Implementation of remote calls for these implementations
    public String put(String key,String value,int request_random){
        System.out.println("Received PUT "+key+" "+value);
        if(voted_on.containsKey(key)){
            return "NO";
        } else{
            voted_on.put(key, true);
        }
        if(local_store.containsKey(key) || peer_table.containsKey(key)){
            return "NO";
        } else{
            if(keys_random_pairs.containsKey(key)){
                int self_random = keys_random_pairs.get(key);
                if(self_random > request_random){
                    return "NO";
                } else{
                    return "YES";
                }
            } else{
                return "YES";
            }
        }
    }
    public String get(String key){
        System.out.println("Received GET "+key);
        return local_store.get(key);
    }
    public ConcurrentHashMap<String,String> store(){
        System.out.println("Received STORE");
        return local_store;
    }
    public String delete(String key){
        System.out.println("Received DELETE");
        this.peer_table.remove(key);
        return "EXECUTED";
    }
    public String test_call(){
        return "ONLINE";
    }
    public String ptupdate(String key,Integer host_id){
        System.out.println("Received PTUPDATE "+key+" "+host_id);
        peer_table.put(key, host_id);
        return "EXECUTED";
    }
    public void exit(Integer host_id){
        System.out.println("Received EXIT for host:"+host_id);
        // clear the peer table entries
        for (Map.Entry<String, Integer> entry : peer_table.entrySet()) {
            if(entry.getValue() == host_id){
                peer_table.remove(entry.getKey());
            }
        }
        total_host_count = total_host_count - 1; //decrement total host count
        host_stubs.remove(host_id); //remove the stubs
    }
}
