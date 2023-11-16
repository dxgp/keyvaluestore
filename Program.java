import java.util.concurrent.TimeUnit;
import java.rmi.registry.LocateRegistry;
import java.util.Scanner;

import storage.KeyValueStore;
public class Program {
    public static void main(String[] args) {
        int host_id = Integer.parseInt(args[0]);
        KeyValueStore kv_store = new KeyValueStore(Integer.parseInt(args[0]),Integer.parseInt(args[1]),10000);
        String[] ip_list = new String[kv_store.total_host_count];
        if(args.length>2){
            for(int i=2;i<args.length;i++){
                ip_list[i-2] = args[i];
            }
        }
        else{
            for(int i=0;i<kv_store.total_host_count;i++){
                ip_list[i] = "localhost";
            }
        }
        kv_store.initialize_peers(ip_list);
        Scanner sc = new Scanner(System.in);
        while(true){
            String query = sc.nextLine();
            execute_query(query,kv_store);
        }
    }
    public static void execute_query(String query,KeyValueStore kv_store){
        String[] query_terms = query.split("\\ ");
        if(query_terms[0].equals("PUT") && query_terms.length==3){
            String key = query_terms[1];
            String value = query_terms[2];
            kv_store.execute_put(key, value);
        } else if(query_terms[0].equals("GET")  && query_terms.length==2){
            String key = query_terms[1];
            kv_store.execute_get(key);
        } else if(query_terms[0].equals("DELETE")  && query_terms.length==2){
            String key = query_terms[1];
            kv_store.execute_delete(key);
        } else if(query_terms[0].equals("STORE")){
            kv_store.execute_store();
        } else if(query_terms[0].equals("EXIT")){
            kv_store.execute_exit();
        } else {
            System.out.println("Illegal Query!");
        }
    }
}
