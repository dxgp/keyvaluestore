package threads;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;



public class SendPutRequestThread implements Runnable {

    String key;
    String value;
    AtomicInteger count;
    Integer self_random;
    DatagramSocket socket;
    InetAddress address;
    int port;


    public SendPutRequestThread(Integer host_id,String address,String key,String value,AtomicInteger count,Integer self_random) throws SocketException, UnknownHostException{
        this.key = key;
        this.value = value;
        this.count = count;
        this.self_random = self_random;


        socket = new DatagramSocket();
        this.address = InetAddress.getByName(address);
        this.port = 10000 + host_id;
    }

    public String sendRequest(String req) {
        try {
            // Send PUT request
            byte[] buf = req.getBytes();
            DatagramPacket packet = new DatagramPacket(buf, buf.length, this.address, this.port);
            this.socket.send(packet);

            System.out.println("REQ Sent: " + req + " To host_id: " + (this.port-10000));
            
            // Receive ack from peers
            DatagramPacket ack_packet = new DatagramPacket(buf, buf.length);
            socket.receive(ack_packet);
            String received = new String(ack_packet.getData(), 0, ack_packet.getLength());
            System.out.println("Recvd: " + received + " from host " + (this.port-10000));
            return received;
        } catch (IOException e) {
            // TODO: handle exception
            System.out.println(e);
            return "";
        }
        
    }

    public void run(){
        String request = "PUT "+key+" "+value+" "+self_random;
        try{
            String response = this.sendRequest(request);
            if(response.equals("YES")){
                count.incrementAndGet();
            }

        } catch(Exception e){
            System.out.println("Exception occured when writing query to output stream in SendPutRequestThread");
        }
    }
    
}
