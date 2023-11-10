package threads;
import java.net.Socket;

public class SendPTUpdateRequestThread implements Runnable {
    Socket sock;
    String key;
    Integer host_id;
    public SendPTUpdateRequestThread(Socket sock,String key,Integer host_id){
        this.sock = sock;
        this.key = key;
        this.host_id = host_id;
    }
    public void run(){
        String request = "PTUPDATE "+key+" "+host_id+"\n";
        try{
            // dos.writeBytes(request);
            sock.getOutputStream().write(request.getBytes());
            sock.getOutputStream().flush();
            System.out.println("WROTE PTUPDATE TO OUT STREAM");
            char buf = '\0';
            String response = "";
            while(!(buf == '\n')){
                buf = (char) sock.getInputStream().read();
                response += buf;
            }
            response = response.trim();
            if(!response.equals("EXECUTED")){
                System.out.println("FAILED TO EXECUTE PTUPDATE for host "+host_id);
            }
        } catch(Exception e){e.printStackTrace();}
    }
}
