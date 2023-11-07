import java.util.concurrent.TimeUnit;

import storage.KeyValueStore;
public class Program {
    public static void main(String[] args) {
        int host_id = Integer.parseInt(args[0]);
        KeyValueStore kv_store = new KeyValueStore(Integer.parseInt(args[0]),Integer.parseInt(args[1]),Integer.parseInt(args[2]),15000);
        kv_store.initialize_peers();
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
    
}
