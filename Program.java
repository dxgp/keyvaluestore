import java.util.*;
import threads.RequestAcceptThread;
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
                        if(kv_store.parseRequest(request)[0].equals("GET")){
                            
                        } else{
                            kv_store.broadcast_request(request, kv_store);
                        }
                    } catch(Exception e){}
                }
            }
        }
    }
}
