import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.util.*;
import threads.RequestAcceptThread;
import threads.SendGetRequestThread;
import storage.KeyValueStore;

public class Program {
    /*command line arguments in the form:
    1. the id of this host
    2. total hosts
    3. total key count to get the range of random key generation
    4. debug mode
    */
    public static void main(String[] args) {
        System.out.println("Starting...");
        KeyValueStore kv_store = new KeyValueStore(Integer.parseInt(args[0]),Integer.parseInt(args[1]),Integer.parseInt(args[2]));
        RequestAcceptThread accept_thread = new RequestAcceptThread(kv_store.host_id,kv_store);
        System.out.println("Request accept thread started...");
        kv_store.initialize_peers();
        new Thread(accept_thread).start();
        if(args.length==4){
            System.out.println("DEBUG MODE STARTED...");
            while(true){
                try{
                    Scanner sc = new Scanner(System.in);
                    System.out.println("Enter Query:");
                    String query = sc.nextLine();
                    kv_store.broadcast_request(query, kv_store);
                    if(query.equals("EXIT")){
                        return;
                    }
                } catch(Exception e){System.out.println(e);}
            }
        }
        else{
            if(kv_store.host_id==0 || kv_store.host_id==1){
                for(int i=0;i<10;i++){
                    try{
                        String request = kv_store.generate_random_request(kv_store);
                        String[] req_split = kv_store.parseRequest(request);
                        if(req_split[0].equals("GET")){
                            String key = req_split[1];
                            // Check if key exists in local store
                            if (kv_store.local_store.containsKey(key)) {
                                System.out.println("VALUE FOR KEY " + key + ": " + kv_store.local_store.get(key));
                            } else if (kv_store.peer_table.containsKey(key)) {
                                // Check for key in peer table and retrieve the value from the host that has that key
                                String host_id = kv_store.peer_table.get(key);
                                ArrayList<Object> peer_streams = kv_store.peers.get(host_id);
                                DataOutputStream dos = (DataOutputStream)peer_streams.get(0);
                                BufferedReader in = (BufferedReader)peer_streams.get(1);
                                String req = "GET|" + key + "\n";
                                SendGetRequestThread sendGetThread = new SendGetRequestThread(dos, in, req);
                                new Thread(sendGetThread).start();
                            } else {
                                System.out.println("INVALID REQUEST: KEY " + key + "does not exist.");
                            }
                        } else{
                            kv_store.broadcast_request(request, kv_store);
                        }
                    } catch(Exception e){}
                }
            }
        }
    }
}
