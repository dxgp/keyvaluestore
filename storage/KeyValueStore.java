package storage;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import threads.SendRequestThread;
import threads.TotalStore;

public class KeyValueStore {
    public ConcurrentHashMap<String,String> local_store;
    public ConcurrentHashMap<String,String> peer_table;
    public ConcurrentHashMap<String,Integer> keys_random_pairs;
    /* 
       The structure of this map is: (id,(DataOutputStream,BufferedReader))
       the DataOutputStream will be used for sending the data out while the buffered reader
       will be used for data in.
    */
    private ConcurrentHashMap<Integer,ArrayList<Object>> peers;
    public int host_id;
    public int total_host_count;
    public int key_count;
    public KeyValueStore(int host_id,int total_host_count,int key_count){
        this.local_store = new ConcurrentHashMap<>();
        this.peer_table = new ConcurrentHashMap<>();
        this.peers = new ConcurrentHashMap<>();
        this.keys_random_pairs = new ConcurrentHashMap<>();
        this.key_count = key_count;
        this.total_host_count = total_host_count;
        this.host_id = host_id;
    }
    public void initialize_peers(){
        for (int i = 0; i < this.total_host_count; i++) {
            boolean connection_established = false;
            if(i!=this.host_id){
                while(connection_established!=true){
                    try{
                        Socket out_socket = new Socket("localhost",10000+i);
                        DataOutputStream out_stream = new DataOutputStream(out_socket.getOutputStream());
                        BufferedReader in_stream = new BufferedReader(new InputStreamReader(out_socket.getInputStream()));
                        ArrayList<Object> peer_streams = new ArrayList<Object>(Arrays.asList(out_stream, in_stream));
                        this.peers.put(i,peer_streams);
                        connection_established = true;
                    } catch(Exception e){}
                }
            }
        }
        System.out.println("Connection established will all nodes. Now proceeding...");
    }
    public void broadcast_request(String req, KeyValueStore kv_store) throws IOException,InterruptedException {
        final AtomicInteger count = new AtomicInteger(0);
        ExecutorService broadcast_executor = Executors.newFixedThreadPool(kv_store.total_host_count);
        TotalStore total_store = new TotalStore();
        this.peers.forEach((host_id,peer_streams)->{
            DataOutputStream dos = (DataOutputStream)peer_streams.get(0);
            BufferedReader in = (BufferedReader)peer_streams.get(1);
            if(parseRequest(req)[0].equals("STORE")){
                broadcast_executor.execute(new SendRequestThread(dos, in, req,total_store));
            }
            else{
                broadcast_executor.execute(new SendRequestThread(dos, in, req, count));
            }
        });
        broadcast_executor.awaitTermination(2L, TimeUnit.SECONDS);
        broadcast_executor.shutdownNow();
        if(parseRequest(req)[0].trim().equals("STORE")){
            System.out.println(total_store.total);
        }
        else if(count.intValue()==kv_store.total_host_count-1){
            System.out.println("REQ SUCCESSFUL");
            // the request was successful the key can now be put into the local store and
            // the peer table changes need to be broadcast
            String[] query_terms = this.parseRequest(req);
            String key = query_terms[1];
            String value = query_terms[2];
            this.local_store.put(key, value);
            this.broadcastPeerTableChange(key, kv_store);
        } else{
            // the request was unsuccessful, this key is already in use.
            // maybe the change has not been propagated or someone else generated
            // the same key at the same time with a higher random integer.
            System.out.println("REQUEST UNSUCCESSFUL");
        }
    }

    public void broadcastPeerTableChange(String key, KeyValueStore kv_store) {
        // PTUPDATE|host_id|key
        String req = "PTUPDATE|" + this.host_id + "|" + key + "\n";
        ExecutorService broadcast_executor = Executors.newFixedThreadPool(kv_store.total_host_count);
        this.peers.forEach((host_id,peer_streams)->{
            DataOutputStream dos = (DataOutputStream)peer_streams.get(0);
            BufferedReader in = (BufferedReader)peer_streams.get(1);
            broadcast_executor.execute(new SendRequestThread(dos, in, req));
        });
        try{
            broadcast_executor.awaitTermination(2L, TimeUnit.SECONDS);
        } catch(Exception e){}
    }
    public String process_query(String[] query_terms,KeyValueStore kv_store){
        String query = query_terms[0].trim();
        if(query.equals("GET")){
            String key = query_terms[1];
            return kv_store.local_store.get(key);
        }
        else if(query.equals("PUT")){
            String key = query_terms[1];
            int rand = Integer.parseInt(query_terms[3].trim());
            /*
             * The steps will be:
             * 1. Check if it already exists in the local content store.
             * 2. If it does, reply NO.
             * 3. If not, reply YES.
             * 
             * In the case when two hosts would like to put the same key,
             * the host will send the put request with a random integer.
             * The receiver will also keep a random integer generated.
             * If the receiver's random integer is greater, it will reply NO, meaning that
             * the put function cannot be executed. If not, it must reply with YES.
             */
            

            /*
             * This scheme will be problematic when three hosts generate the same key at the same time.
             * At that time, since between every two nodes, the keys contesting will be different, it's possible
             * that none of them will win. (Maybe add a HashMap to store the random integer contesting for each key?)
             */
            
            // peer table does not contain the entries for the node at which it is stored at.
            if(kv_store.local_store.containsKey(key) || kv_store.peer_table.containsKey(key)){
                return "NO";
            } else{
                if(kv_store.keys_random_pairs.containsKey(key)){
                    int self_random = kv_store.keys_random_pairs.get(key);
                    if(rand>self_random){
                        return "YES";
                    } else{
                        return "NO";
                    }
                    
                } else{
                    return "YES";
                }
            }
        }
        else if(query.equals("DEL")){
            String key = query_terms[1];
            kv_store.local_store.remove(key);
            return "AB\n";
        }
        else if(query.equals("STORE")){
            String ls_string = "";
            for (Map.Entry<String, String> entry : kv_store.local_store.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                ls_string+= key+"\t"+value+"\t"+"\n";
            }
            return ls_string;
        }
        else if(query.equals("PTUPDATE")) {
            String host_id = query_terms[1];
            String key = query_terms[2];
            // Update peer table
            if (kv_store.peer_table.containsKey(key)) {
                System.out.println("ERROR: Key " + key + " already exists in peer table\n");
                return "ERR";
            } else {
                kv_store.peer_table.put(key, host_id);
                return "AB";
            }
        }
        else{
            System.out.println("Invalid query provided");
            return "AB";
        }
    }

    public String[] parseRequest(String req) {
        String[] queryTerms = req.split("\\|");
        return queryTerms;
    }

    public String generate_random_request(KeyValueStore kv_store) {
        String[] operations = {"PUT", "GET", "DEL", "STORE"};
        int op_index = ThreadLocalRandom.current().nextInt(0, operations.length);
        String random_op = operations[op_index];
        
        if (random_op == "PUT" || random_op == "GET" || random_op == "DEL") {
            String random_key = "";
            do {
                // Generate random key
                int r = ThreadLocalRandom.current().nextInt(0, kv_store.key_count);
                random_key = Integer.toString(r);
            }
            while(kv_store.local_store.containsKey(random_key) && kv_store.peer_table.containsKey(random_key));
            if (random_op == "PUT") {
                // Generate random value
                String random_value = Integer.toString(ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE));
                String encoded_random_value = Base64.getEncoder().encodeToString(random_value.getBytes());
                
                // Generate random number for clash resolution
                int rand_num = ThreadLocalRandom.current().nextInt(0, 1001);
                kv_store.keys_random_pairs.put(random_key, rand_num);
                String random_num = Integer.toString(rand_num);
                
                return random_op + "|" + random_key + "|" + encoded_random_value + "|" + random_num + "\n";
            }
            return random_op + "|" + random_key + "\n";
        }
        return random_op + "\n";
    }
}
