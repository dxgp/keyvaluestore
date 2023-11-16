package threads;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

public class SendStoreRequestThread implements Runnable{
    Socket sock;
    Integer host_id;
    ConcurrentHashMap<String,String> total_map;
    public SendStoreRequestThread(Socket sock,Integer host_id,ConcurrentHashMap<String,String> total_map){
        this.sock = sock;
        this.host_id = host_id;
        this.total_map = total_map;
    }
    public void run(){
        String request = "STORE" + "\n";
        try{
            sock.getOutputStream().write(request.getBytes());
            sock.getOutputStream().flush();
            char buf = '\0';
            String response = "";
            while(!(buf == '\n')){
                buf = (char) sock.getInputStream().read();
                response += buf;
            }
            response = response.trim();
            ConcurrentHashMap<String,String> recvd_hm = new ConcurrentHashMap<String,String>();
            String[] rows = response.split("\\|");
            for(int i=0;i<rows.length;i++){
                String[] kv = rows[i].split("\\ ");
                recvd_hm.put(kv[0], kv[1]);
            }
            total_map.putAll(recvd_hm);
        } catch(Exception e){}
    }
}
