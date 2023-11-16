import java.util.concurrent.TimeUnit;
import java.util.Scanner;

import storage.KeyValueStore;
public class Program {
    public static void main(String[] args) {
        int host_id = Integer.parseInt(args[0]);
        KeyValueStore kv_store = new KeyValueStore(Integer.parseInt(args[0]),Integer.parseInt(args[1]),10000);
        kv_store.initialize_peers();
        if(args.length==3 && args[2].equals("--debug")){
            Scanner sc = new Scanner(System.in);
            while(true){
                String query = sc.nextLine();
                execute_query(query,kv_store);
            }
        }
        if(host_id==0){
            kv_store.execute_put("10","asdf8asdf");
            kv_store.execute_put("11","asdfa");
            kv_store.execute_put("12","sdgcxb");
            kv_store.execute_put("13","qwerui");
            kv_store.execute_put("14","sdvoaoi");
            kv_store.execute_put("15","qweiufdx");
        }
        else{
            try{
                TimeUnit.SECONDS.sleep(1);
            } catch(Exception e){}
            if(host_id==1){
                kv_store.execute_store();
                kv_store.execute_delete("12");
                kv_store.execute_store();
            }
            
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
