import java.util.Scanner;
import storage.KeyValueStore;
public class Program{
    public static void main(String[] args) {
        KeyValueStore kv_store = new KeyValueStore(Integer.parseInt(args[0]),Integer.parseInt(args[1]),Integer.parseInt(args[2]));
        kv_store.initialize_peers();
        while(true){
            try{
                Scanner sc = new Scanner(System.in);
                String query = sc.nextLine();
                String[] query_terms = query.split("// ");
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
            } catch(Exception e){}
        }
    }
}