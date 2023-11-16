import java.net.SocketException;
import java.util.concurrent.TimeUnit;

import storage.KeyValueStore;

public class BenchmarkingUDP {
    public static void main(String[] args) throws NumberFormatException, SocketException {
        KeyValueStore kv_store = new KeyValueStore(Integer.parseInt(args[0]),Integer.parseInt(args[1]));
        int num_runs = Integer.parseInt(args[2]);
        String[] ip_list = new String[kv_store.total_host_count];
        for(int i=0;i<kv_store.total_host_count;i++){
            ip_list[i] = "localhost";
        }
        
        if(kv_store.host_id==0){
            String[] peers = new String[3];
            peers[0] = "";
            peers[1] = "";
            peers[2] = "1:localhost"; 
            kv_store.initialize_peers(peers);

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
            while(!kv_store.peer_table.containsKey("TEST_KEY"+(num_runs-1)));
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
            String[] peers = new String[3];
            peers[0] = "";
            peers[1] = "";
            peers[2] = "0:localhost";
            kv_store.initialize_peers(peers);

            while(!kv_store.peer_table.containsKey(""+(num_runs-1)));
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