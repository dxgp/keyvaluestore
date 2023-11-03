import java.net.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
public class KeyValueStore {
    private ConcurrentHashMap<String,String> local_store;
    private ConcurrentHashMap<String,String> peer_table;
    private ConcurrentHashMap<String,Integer> keys_random_pairs; // HashMap to store the keys currently being generated
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
    }

    public void initialize_peers(){
        for (int i = 0; i < this.total_host_count; i++) {
            boolean connection_established = false;
            if(i!=this.host_id){
                while(connection_established!=true){
                    try{
                        Socket out_socket = new Socket("localhost",10000+i);
                        DataOutputStream out_stream = new DataOutputStream(out_socket.getOutputStream());
                        BufferedReader in_stream = new BufferedReader(new InputStreamReader(out_socket.getInputStream()));
                        ArrayList<Object> peer_streams = new ArrayList<Object>(Arrays.asList(out_stream, in_stream));
                        this.peers.put(i,peer_streams);
                        connection_established = true;
                    } catch(Exception e){System.out.println("Connection failed..."+ " with host "+i);}
                }
            }
        }
        System.out.println("Connection established will all nodes. Now proceeding...");
    }
    public static void main(String[] args) {
        /*command line arguments in the form:
          1. the id of this host
          2. total hosts
          3. total key count to get the range of random key generation
          4. debug mode
        */
        System.out.println("Starting...");
        KeyValueStore kv_store = new KeyValueStore(Integer.parseInt(args[0]),Integer.parseInt(args[1]),Integer.parseInt(args[2]));
        RequestAcceptThread accept_thread = new RequestAcceptThread(kv_store.host_id,kv_store);
        System.out.println("Request accept thread started...");
        kv_store.initialize_peers();
        new Thread(accept_thread).start();
        if(args.length==4){
            //run in debug mode
            System.out.println("DEBUG MODE STARTED...");
            while(true){
                try{
                    Scanner sc = new Scanner(System.in);
                    System.out.println("Enter Query:");
                    String query = sc.nextLine();
                    kv_store.broadcast_request(query, kv_store);
                    if(query.equals("EXIT")){
                        return;
                    }
                } catch(Exception e){}
            }
        }
        //Generate random requests
        else{
            if(kv_store.host_id==0){
                for(int i=0;i<3;i++){
                    try{
                        String request = kv_store.generate_random_request(kv_store);
                        System.out.println("GENERATED REQUEST:"+request);
                        kv_store.broadcast_request(request, kv_store);
                    } catch(Exception e){}
                }
            }
        }
    }

    // Broadcasts current operation and key to all other peers
    private void broadcast_request(String req, KeyValueStore kv_store) throws IOException,InterruptedException {
        final AtomicInteger count = new AtomicInteger(0);
        ExecutorService broadcast_executor = Executors.newFixedThreadPool(kv_store.total_host_count);
        this.peers.forEach((host_id,peer_streams)->{
            DataOutputStream dos = (DataOutputStream)peer_streams.get(0);
            BufferedReader in = (BufferedReader)peer_streams.get(1);
            broadcast_executor.execute(new SendRequestThread(dos, in, req, count));
        });
        System.out.println("Request broadcasted.");
        broadcast_executor.awaitTermination(50L, TimeUnit.SECONDS);
        if(count.intValue()==kv_store.total_host_count-1){
            // the request was successful the key can now be put into the local store and
            // the peer table changes need to be broadcast

            // TODO: Need to create another message type to broadcast the change in the peer table.
        } else{
            // the request was unsuccessful, this key is already in use.
            // maybe the change has not been propagated or someone else generated
            // the same key at the same time with a higher random integer.
        }
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
                System.out.println("QQQQ " + req);
                dos.writeBytes(req);
                while(!in.ready());
                String ack = in.readLine();
                if(ack.equals("YES")){
                    counter.incrementAndGet();
                }

            } catch (IOException e) {
                System.err.println(e);
            }
        }
    }


    private String generate_random_request(KeyValueStore kv_store) {
        // Generate random operation
        // removed exit from here. Cannot call EXIT until the end.
        String[] operations = {"PUT", "GET", "DEL", "STORE"};
        int op_index = ThreadLocalRandom.current().nextInt(0, operations.length);
        String random_op = operations[op_index];

        if (random_op == "PUT" || random_op == "GET" || random_op == "DEL") {
            String random_key = "";
            do {
                // Generate random key
                int r = ThreadLocalRandom.current().nextInt(0, kv_store.key_count);
                random_key = Integer.toString(r);
            }
            while(kv_store.local_store.containsKey(random_key) && kv_store.peer_table.containsKey(random_key));
            if (random_op == "PUT") {
                // Generate random value
                String random_value = Integer.toString(ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE));
                String encoded_random_value = Base64.getEncoder().encodeToString(random_value.getBytes());

                // Generate random number for clash resolution
                int rand_num = ThreadLocalRandom.current().nextInt(0, 1001);
                kv_store.keys_random_pairs.put(random_key, rand_num);
                String random_num = Integer.toString(rand_num);

                return random_op + "|" + random_key + "|" + encoded_random_value + "|" + random_num + "\n";
            }
            return random_op + "|" + random_key + "\n";
        }
        return random_op + "\n";
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
                    char[] buf = new char[100];
                    server_in.read(buf);
                    System.out.println("CLIENt Q: " + buf);
                    String client_query = new String(buf);
                    String[] queryTerms = client_query.split("\\|");
                    System.out.println("Request received"+Arrays.toString(queryTerms));
                    //process_query(queryTerms, kv_store);
                    server_out.writeBytes("\n");
                    server_out.flush();
                }
            } catch(IOException e){
                System.out.println(e.toString());
            }
        }
    }
    private static String process_query(String[] query_terms,KeyValueStore kv_store){
        String query = query_terms[0];
        if(query.equals("GET")){
            String key = query_terms[1];
            System.out.println("GET QUERY RECEIVED");
            return kv_store.local_store.get(key);
        }
        else if(query.equals("PUT")){
            String key = query_terms[1];
            String rand = query_terms[3];
            System.out.println("PUT QUERY RECEIVED FOR KEY:"+key);
            int recvd_random = Integer.parseInt(rand);
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
            

            /*
             * This scheme will be problematic when three hosts generate the same key at the same time.
             * At that time, since between every two nodes, the keys contesting will be different, it's possible
             * that none of them will win. (Maybe add a HashMap to store the random integer contesting for each key?)
             */
            
            System.out.println("PUT QUERY RECEIVED");
            // peer table does not contain the entries for the node at which it is stored at.
            if(kv_store.local_store.containsKey(key) || kv_store.peer_table.containsKey(key)){
                return "NO";
            } else{
                if(kv_store.keys_random_pairs.containsKey(key)){
                    int self_random = kv_store.keys_random_pairs.get(key);

                    if(recvd_random>self_random){
                        return "YES";
                    } else{
                        return "NO";
                    }
                    
                } else{
                    return "YES";
                }
            }
        }
        else if(query.equals("DEL")){
            String key = query_terms[1];
            System.out.println("DELETE QUERY RECEIVED");
            kv_store.local_store.remove(key);
            return "AB\n";
        }
        else if(query.equals("STORE")){
            // TODO: Figure out a way to send its local store
            return "AB\n";
        }
        else{
            System.out.println("Invalid query provided");
            return "AB";
        }
    }

}
