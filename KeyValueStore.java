import java.net.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ThreadLocalRandom;
public class KeyValueStore {
    private ConcurrentHashMap<String,String> local_store;
    private ConcurrentHashMap<String,String> peer_table;
    /* 
       The structure of this map is: (id,(DataOutputStream,BufferedReader))
       the DataOutputStream will be used for sending the data out while the buffered reader
       will be used for data in.
    */
    private ConcurrentHashMap<Integer,ArrayList<Object>> peers;
    int host_id;
    int total_host_count;
    int key_count;
    KeyValueStore(int host_id,int total_host_count,int key_count){
        this.local_store = new ConcurrentHashMap<>();
        this.peer_table = new ConcurrentHashMap<>();
        this.peers = new ConcurrentHashMap<>();
        this.key_count = key_count;
        this.total_host_count = total_host_count;
        this.host_id = host_id;
        initialize_peers();
    }

    public void initialize_peers(){
        for (int i = 0; i < this.total_host_count; i++) {
            if(i!=this.host_id){
                try{
                    Socket out_socket = new Socket("localhost",10000+i);
                    DataOutputStream out_stream = new DataOutputStream(out_socket.getOutputStream());
                    BufferedReader in_stream = new BufferedReader(new InputStreamReader(out_socket.getInputStream()));
                    ArrayList<Object> peer_streams = new ArrayList<Object>(Arrays.asList(out_stream, in_stream));
                    this.peers.put(i,peer_streams);
                } catch(Exception e){
                    System.out.println(e.toString());
                }
            }
        }
    }

    // Broadcasts current operation and key to all other peers
    private void broadcast_request(String operation, String key) throws IOException {
        String req = operation + "|" + key;
        final AtomicInteger count = new AtomicInteger(0);
        this.peers.forEach((host_id, peer_streams) -> {
            DataOutputStream dos = (DataOutputStream)peer_streams.get(0);
            BufferedReader in = (BufferedReader)peer_streams.get(1);
            SendRequestThread send_thread = new SendRequestThread(dos, in, req,count);
            new Thread(send_thread).start();
        });
    }

    private static class SendRequestThread implements Runnable {
        DataOutputStream dos;
        BufferedReader in;
        String req;
        AtomicInteger counter;
        SendRequestThread(DataOutputStream dos, BufferedReader in, String req, AtomicInteger counter) {
            this.dos = dos;
            this.in = in;
            this.req = req;
            this.counter = counter;
        }
        
        public void run() {
            try {
                dos.writeBytes(req);
                String ack = in.readLine();
                counter.incrementAndGet();
            } catch (IOException e) {
                System.err.println(e);
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
        RequestAcceptThread accept_thread = new RequestAcceptThread(kv_store.host_id,kv_store);
        new Thread(accept_thread).start();

        //write the code to send the requests
    }
    /*
     * A thread to accept incoming requests. Runs an infinite while loop and keeps accepting requests.
     * Once a request is accepted, it creates a new thread to handle it.
     */
    private static class RequestAcceptThread implements Runnable{
        ServerSocket accept_socket;
        KeyValueStore kv_store;
        public RequestAcceptThread(int host_id,KeyValueStore kv_store){
            try{
                accept_socket = new ServerSocket(10000+host_id);
                this.kv_store = kv_store;
            } catch(IOException e){
                System.out.println(e.toString());
            }
        }
        public void run(){
            while(true){
                try{
                    Socket socket = accept_socket.accept();
                    // call the request handler thread here
                    RequestHandlerThread req_handler = new RequestHandlerThread(socket,kv_store);
                    new Thread(req_handler).start();
                } catch(IOException e){
                    System.out.println(e.toString());
                }
            }
        }
    }
    /*
     * This thread will now handle all future requests from this host.
     */
    private static class RequestHandlerThread implements Runnable{
        private final Socket socket;
        BufferedReader server_in;
        DataOutputStream server_out;
        KeyValueStore kv_store;
        public RequestHandlerThread(Socket socket,KeyValueStore kv_store){
            this.socket = socket;
            this.kv_store = kv_store;
        }
        public void run(){
            try{
                server_in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                server_out = new DataOutputStream(new DataOutputStream(socket.getOutputStream()));
                while(true){ // all requests from this host will be handled by this thread from now on.
                    while(!server_in.ready());
                    // now process using the input here.
                    String client_query = server_in.readLine();
                    System.out.println(client_query);
                    String[] queryTerms = client_query.split("\\|");
                    System.out.println(Arrays.toString(queryTerms));
                    server_out.flush();
                }
            } catch(IOException e){
                System.out.println(e.toString());
            }
        }
    }
    private static String process_query(String[] query_terms,KeyValueStore kv_store){
        if(query_terms[0].equals("GET")){
            System.out.println("GET QUERY RECEIVED");
            return kv_store.local_store.get(query_terms[1]);
        }
        else if(query_terms[0].equals("PUT")){
            /*
             * The steps will be:
             * 1. Check if it already exists in the local content store.
             * 2. If it does, reply NO.
             * 3. If not, reply YES.
             * 
             * In the case when two hosts would like to put the same key,
             * the host will send the put request with a random integer.
             * The receiver will also keep a random integer generated.
             * If the receiver's random integer is greater, it will reply NO, meaning that
             * the put function cannot be executed. If not, it must reply with YES.
             */
            int random_integer = ThreadLocalRandom.current().nextInt(0, 1000 + 1);
            System.out.println("PUT QUERY RECEIVED");
            
            return "AB\n";
        }
        else if(query_terms[0].equals("DEL")){
            System.out.println("DELETE QUERY RECEIVED");
            data.remove(query_terms[1]);
            return "AB\n";
        }
        else if(queryTerms[0].equals("STORE")){
            System.out.println("STORE QUERY RECEIVED");
            return "AB\n";
        }
        else{
            System.out.println("Invalid query provided");
            return "AB";
        }
    }

}
