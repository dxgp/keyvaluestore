import java.net.*;
import java.io.*;
import java.util.*;
public class KeyValueStore {
    private Map<String,String> local_store;
    private Map<String, String> peer_table;
    private Map<Integer,Socket> peers;
    int host_id;
    int total_hosts;
    int key_count;
    KeyValueStore(int host_id,int total_hosts,int key_count){
        this.local_store = new HashMap<>();
        this.peer_table = new HashMap<>();
        this.peers = new HashMap<>();
        this.key_count = key_count;
        this.total_hosts = total_hosts;
        this.host_id = host_id;
        for (int i = 0; i < total_hosts; i++) {
            if(i!=host_id){
                try{
                    peers.put(i,new Socket("localhost",10000+i));
                } catch(Exception e){
                    System.out.println(e.toString());
                }
            }
        }
    }
    public static void main(String[] args) {
        /*command line arguments in the form:
          1. the id of this host
          2. total hosts
          3. total key count to get the range of random key generation
        */
        KeyValueStore kv_store = new KeyValueStore(Integer.parseInt(args[0]),Integer.parseInt(args[1]),Integer.parseInt(args[2]));
        
    }
}
