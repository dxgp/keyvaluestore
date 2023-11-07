import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import storage.KeyValueStore;
public class Program{
    public static void main(String[] args) {
        KeyValueStore kv_store = new KeyValueStore(Integer.parseInt(args[0]),Integer.parseInt(args[1]),Integer.parseInt(args[2]));
        kv_store.initialize_peers();
        Scanner sc = new Scanner(System.in);
        while(true){
            try{
                String query = sc.nextLine();
                query = query.trim();
                String[] query_terms = query.split("\\ ");
                System.out.println("QUERY:"+Arrays.toString(query_terms));
                if(query_terms[0].equals("PUT")){
                    String key = query_terms[1];
                    String value = query_terms[2];
                    kv_store.execute_put(key, value);
                } else if(query_terms[0].equals("GET")){
                    String key = query_terms[1];
                    kv_store.execute_get(key);
                } else if(query_terms[0].equals("STORE")){
                    kv_store.execute_store();
                } else if(query_terms[0].equals("DELETE")){
                    String key = query_terms[1];
                    kv_store.execute_delete(key);
                } else{
                    System.out.println("INVALID QUERY PROVIDED");
                }
                System.out.println("**QUERY EXECUTION COMPLETE**");
                TimeUnit.MILLISECONDS.sleep(50);
            } catch(Exception e){}
        }
    }
}