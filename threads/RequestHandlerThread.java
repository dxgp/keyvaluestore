package threads;
import java.net.*;
import java.io.*;
import java.util.*;
import storage.KeyValueStore;

public class RequestHandlerThread implements Runnable{
    private final Socket socket;
    BufferedReader server_in;
    DataOutputStream server_out;
    KeyValueStore kv_store;
    public RequestHandlerThread(Socket socket,KeyValueStore kv_store){
        this.socket = socket;
        this.kv_store = kv_store;
    }
    public void run(){
        try{
            server_in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            server_out = new DataOutputStream(new DataOutputStream(socket.getOutputStream()));
            while(true){ // all requests from this host will be handled by this thread from now on.
                while(!server_in.ready());
                
                char[] buf = new char[100];
                server_in.read(buf);
                String client_query = new String(buf);
                String[] queryTerms = this.kv_store.parseRequest(client_query);
                System.out.println("Request received"+Arrays.toString(queryTerms));
                String resp = process_query(queryTerms, kv_store);
                server_out.writeBytes(resp+"\n");
                server_out.flush();
            }
        } catch(IOException e){
            System.out.println(e.toString());
        }
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
}
