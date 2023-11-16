import java.util.concurrent.TimeUnit;

import storage.KeyValueStore;

public class Benchmarking {
    public static void main(String[] args) {
        KeyValueStore kv_store = new KeyValueStore(Integer.parseInt(args[0]),Integer.parseInt(args[1]),10000);
        kv_store.initialize_peers();
        int num_runs = 100;
        if(kv_store.host_id==0){
            long[] put_times = new long[num_runs];
            long[] get_times = new long[num_runs];
            long[] store_times = new long[num_runs];
            long[] delete_times = new long[num_runs];
            for(int i=0;i<num_runs;i++){
                long startTime = System.currentTimeMillis();
                kv_store.execute_put(""+i, ""+i);
                long endTime = System.currentTimeMillis();
                put_times[i] = endTime - startTime;
            }
            try{TimeUnit.MILLISECONDS.sleep(50);}catch(Exception e){}
            while(!kv_store.peer_table.containsKey("TEST_KEY"+9));
            for(int i=0;i<num_runs;i++){
                long startTime = System.currentTimeMillis();
                kv_store.execute_get("TEST_KEY"+i);
                long endTime = System.currentTimeMillis();
                get_times[i] = endTime - startTime;
            }
            try{TimeUnit.MILLISECONDS.sleep(50);}catch(Exception e){}
            for(int i=0;i<num_runs;i++){
                long startTime = System.currentTimeMillis();
                kv_store.execute_store();
                long endTime = System.currentTimeMillis();
                store_times[i] = endTime - startTime;
            }
            try{TimeUnit.MILLISECONDS.sleep(50);}catch(Exception e){}
            for(int i=0;i<num_runs;i++){
                long startTime = System.currentTimeMillis();
                kv_store.execute_delete("TEST_KEY"+i);
                long endTime = System.currentTimeMillis();
                delete_times[i] = endTime - startTime;
            }
            System.out.println("***********************************************************");
            System.out.println("********************BENCHMARKING RESULTS*******************");
            System.out.println("GET QUERY:   \t--"+array_avg(get_times) + " ms");
            System.out.println("PUT QUERY:   \t--"+array_avg(put_times) + " ms");
            System.out.println("DELETE QUERY:\t--"+array_avg(delete_times) + " ms");
            System.out.println("STORE QUERY: \t--"+array_avg(store_times) + " ms");
        } else if(kv_store.host_id == 1){
            while(!kv_store.peer_table.containsKey(""+9));
            System.out.println("Host 1 put test completed. Now adding test keys and vals for get");
            for(int i=0;i<num_runs;i++){
                kv_store.execute_put("TEST_KEY"+i, "TEST_VAL");
            }
            try{TimeUnit.MILLISECONDS.sleep(50);}catch(Exception e){}
        }
    }
    static double array_avg(long[] times){
        double total_time = 0;
        for(int i=0;i<times.length;i++){
            total_time += times[i];
        }
        return total_time/times.length;
    }
}
