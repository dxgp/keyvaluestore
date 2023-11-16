import java.io.FileWriter;
import java.util.concurrent.TimeUnit;

import storage.KeyValueStore;

public class Benchmarking {
    public static void main(String[] args) {
        KeyValueStore kv_store = new KeyValueStore(Integer.parseInt(args[0]),Integer.parseInt(args[1]));
        int num_runs = Integer.parseInt(args[2]);
        String[] ip_list = new String[kv_store.total_host_count];
        for(int i=0;i<kv_store.total_host_count;i++){
            ip_list[i] = "localhost";
        }
        kv_store.initialize_peers(ip_list);
        if(kv_store.host_id==0){
            long[] put_times = new long[num_runs];
            long[] get_times = new long[num_runs];
            long[] store_times = new long[num_runs];
            long[] delete_times = new long[num_runs];
            for(int i=0;i<num_runs;i++){
                long startTime = System.nanoTime();
                kv_store.execute_put(""+i, ""+i);
                long endTime = System.nanoTime();
                put_times[i] = endTime - startTime;
            }
            try{TimeUnit.MILLISECONDS.sleep(50);}catch(Exception e){}
            while(!kv_store.peer_table.containsKey("TEST_KEY"+9));
            for(int i=0;i<num_runs;i++){
                long startTime = System.nanoTime();
                kv_store.execute_get("TEST_KEY"+i);
                long endTime = System.nanoTime();
                get_times[i] = endTime - startTime;
            }
            try{TimeUnit.MILLISECONDS.sleep(50);}catch(Exception e){}
            for(int i=0;i<num_runs;i++){
                long startTime = System.nanoTime();
                kv_store.execute_store();
                long endTime = System.nanoTime();
                store_times[i] = endTime - startTime;
            }
            try{TimeUnit.MILLISECONDS.sleep(50);}catch(Exception e){}
            for(int i=0;i<num_runs;i++){
                long startTime = System.nanoTime();
                kv_store.execute_delete(""+i);
                long endTime = System.nanoTime();
                delete_times[i] = endTime - startTime;
            }
            System.out.println("***********************************************************");
            System.out.println("********************BENCHMARKING RESULTS*******************");
            System.out.println("GET QUERY:   \t--"+array_avg(get_times)/100000 + " ms");
            System.out.println("PUT QUERY:   \t--"+array_avg(put_times)/100000 + " ms");
            System.out.println("DELETE QUERY:\t--"+array_avg(delete_times)/100000 + " ms");
            System.out.println("STORE QUERY: \t--"+array_avg(store_times)/100000 + " ms");

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
    static void write_to_csv(long[] wasp,String file_name){
        try{
            FileWriter writer = new FileWriter(file_name+".csv");
            for (int j = 0; j < wasp.length; j++) {
                writer.append(String.valueOf(wasp[j]));
                writer.append(",");
            }
            writer.close();
        } catch(Exception e){e.printStackTrace();}
    }
}
