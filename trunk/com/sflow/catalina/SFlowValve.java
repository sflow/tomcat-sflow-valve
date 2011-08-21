package com.sflow.catalina;

import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.CharacterCodingException;
import java.nio.CharBuffer;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import javax.servlet.ServletException;

import org.apache.catalina.valves.ValveBase;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.Host;

public final class SFlowValve extends ValveBase {
   public SFlowValve() {
      super(true);
   }

   protected static final String info = 
     "com.sflow.catalina.SFlowValve/1.0";

   private static final String CONFIG_FILE = "config_file";
   private static final String DEFAULT_CONFIG_FILE = "/etc/hsflowd.auto";

   private static String configFile = DEFAULT_CONFIG_FILE;
   private static long lastConfigFileChange = 0L;

   private static long pollingInterval = 0L;
   private static byte[] agentAddress = null;
   private static ArrayList<InetSocketAddress> destinations;

   private static DatagramSocket socket = null;

   // update configuration
   private static synchronized void updateConfig() {
      File file = new File(configFile);
      if(!file.exists()) return;

      long modified = file.lastModified();
      if(modified == lastConfigFileChange) return;

      lastConfigFileChange = modified;
    
      String rev_start = null;
      String rev_end = null;
      String sampling = null;
      String sampling_http = null;
      String polling = null;
      String agentIP = null;
      ArrayList<String> collectors = null; 
      try {
        BufferedReader br = new BufferedReader(new FileReader(file));
        try {
          String line;
          while((line = br.readLine()) != null) {
             if(line.startsWith("#")) continue;
             int idx = line.indexOf('=');
             if(idx < 0) continue;
             String key = line.substring(0,idx).trim();
             String value = line.substring(idx + 1).trim();
             if("rev_start".equals(key)) rev_start = value;
             else if("sampling".equals(key)) sampling = value;
             else if("sampling.http".equals(key)) sampling_http = value;
             else if("polling".equals(key)) polling = value;
             else if("agentIP".equals(key)) agentIP = value;
             else if("collector".equals(key)) {
               if(collectors == null) collectors = new ArrayList();
               collectors.add(value); 
             }
             else if("rev_end".equals(key)) rev_end = value; 
          }
        } finally { br.close(); }
      } catch (IOException e) {}

      if(rev_start != null && rev_start.equals(rev_end)) {
        lastConfigFileChange = modified;

        // set polling interval
        if(polling != null) {
           try {
             long seconds = Long.parseLong(polling);
              pollingInterval = seconds * 1000L;
           } catch(NumberFormatException e) {
              pollingInterval = 0L;
           };
        }
        else pollingInterval = 0L;

        // set agent address
        if(agentIP != null) agentAddress = addressToBytes(agentIP);
        else agentAddress = null;

        // set sampling rate
        int rate = 0;
        if(sampling_http == null) sampling_http = sampling;
        if(sampling_http != null) {
           try { rate = Integer.parseInt(sampling_http); }
           catch(NumberFormatException e) { rate = 0; }
         }
         else rate = 0;
         setSamplingRate(rate);
      }
 
      // set collectors
      if(collectors != null) {
         ArrayList<InetSocketAddress> newSockets = new ArrayList();
         for(String socketStr : collectors) {
           String[] parts = socketStr.split(" ");
           InetAddress addr = null;
           try { addr = InetAddress.getByName(parts[0]); }
           catch(UnknownHostException e) {}
           if(addr != null) {
              int port = 6343;
              if(parts.length == 2) {
                try { port = Integer.parseInt(parts[1]); }
                catch(NumberFormatException e) {};
              }
              newSockets.add(new InetSocketAddress(addr,port)); 
           }
         }
         destinations = newSockets;
      }
      else destinations = null;
   }

   // random number generator
   private static final ThreadLocal<Long> seed =
       new ThreadLocal<Long> () {
          protected Long initialValue() {
             return new Long(System.nanoTime());
          }
       };

   private static int nextRandom(int nbits) {
     long x = seed.get();
     x ^= (x << 21);
     x ^= (x >>> 35);
     x ^= (x << 4);
     seed.set(x);
     x &= ((1L << nbits) -1);
     return (int) x;
   }

   private static final int dsClass = 3;
   private int dsIndex = -1;
   private int agentSequenceNo = 0;
   private int counterSequenceNo = 0;
   private int flowSequenceNo = 0;
   private long agentStartTime = 0L;


   // sFlow state
   AtomicInteger sample_pool      = new AtomicInteger();
   AtomicInteger sample_count     = new AtomicInteger();
   AtomicInteger skip_count       = new AtomicInteger();

   // sampling
   static int sampling_rate      = 0;
   static int sampling_threshold = 0;
   static int sampling_nbits     = 0;

   private static void setSamplingRate(int rate) {
      if(rate <= 0) {
         sampling_rate      = 0;
         sampling_threshold = 0;
         sampling_nbits     = 0;
      } else {
         sampling_nbits = 16;
         sampling_rate = Math.min(rate, 1<<(sampling_nbits - 1));
         sampling_threshold = (1 << sampling_nbits) / sampling_rate;
      }
   }

   // sFlow HTTP counters
   AtomicInteger method_option_count  = new AtomicInteger();
   AtomicInteger method_get_count     = new AtomicInteger();
   AtomicInteger method_head_count    = new AtomicInteger();
   AtomicInteger method_post_count    = new AtomicInteger();
   AtomicInteger method_put_count     = new AtomicInteger();
   AtomicInteger method_delete_count  = new AtomicInteger();
   AtomicInteger method_trace_count   = new AtomicInteger();
   AtomicInteger method_connect_count = new AtomicInteger();
   AtomicInteger method_other_count   = new AtomicInteger();
   AtomicInteger status_1XX_count     = new AtomicInteger();
   AtomicInteger status_2XX_count     = new AtomicInteger();
   AtomicInteger status_3XX_count     = new AtomicInteger();
   AtomicInteger status_4XX_count     = new AtomicInteger();
   AtomicInteger status_5XX_count     = new AtomicInteger();
   AtomicInteger status_other_count   = new AtomicInteger();

   public String getInfo() {
      return (info);
   }

   public void invoke(Request request, Response response) throws IOException, ServletException {
     long start = System.nanoTime();
     getNext().invoke(request,response);
     long end = System.nanoTime();

     // update counters
     int status = response.getStatus();
     if(status < 100) status_other_count.getAndIncrement();
     else if(status < 200) status_1XX_count.getAndIncrement();
     else if(status < 300) status_2XX_count.getAndIncrement();
     else if(status < 400) status_3XX_count.getAndIncrement();
     else if(status < 500) status_4XX_count.getAndIncrement();
     else if(status < 600) status_5XX_count.getAndIncrement();
     else status_other_count.getAndIncrement();

     String method = request.getMethod();
     if("OPTION".equals(method)) method_option_count.getAndIncrement();
     else if("GET".equals(method)) method_get_count.getAndIncrement();
     else if("HEAD".equals(method)) method_head_count.getAndIncrement();
     else if("POST".equals(method)) method_post_count.getAndIncrement();
     else if("PUT".equals(method)) method_put_count.getAndIncrement();
     else if("DELETE".equals(method)) method_delete_count.getAndIncrement();
     else if("TRACE".equals(method)) method_trace_count.getAndIncrement();
     else if("CONNECT".equals(method)) method_connect_count.getAndIncrement();
     else method_other_count.getAndIncrement();

     sample_pool.getAndIncrement();
     if(sampling_rate != 0
        && nextRandom(sampling_nbits) < sampling_threshold) {
        sample_count.getAndIncrement();
        sampleRequest(request,response,method,status,(int)((end - start) / 1000));
     }
   }

   private static final byte[] EMPTY = {};

   private void sampleRequest(Request request, Response response, String method, int status,int duration) {
     if(agentAddress == null) return;

      byte[] local_addr = addressToBytes(request.getLocalAddr());
      byte[] remote_addr =addressToBytes(request.getRemoteAddr());
      int local_port = request.getLocalPort();
      int remote_port = request.getRemotePort(); 
      String protocol = request.getProtocol();

      if(dsIndex == -1) dsIndex = local_port;

      int socketType = 0;
      int socketLen = 0;
      if(local_addr.length == 4 && remote_addr.length == 4) {
         socketType = 4;
         socketLen = 20;
      }
      else if(local_addr.length == 16 && remote_addr.length == 16) {
         socketType = 6;
         socketLen = 44;
      }

      long bytes = response.getBytesWritten(true);

      String uri = request.getRequestURI();
      byte[] uri_bytes = uri != null ? stringToBytes(uri,255) : EMPTY;
      int uri_pad = pad(uri_bytes.length); 
 
      Host host = request.getHost();
      String hostName = host != null ? host.getName() : "";
      byte[] host_bytes = stringToBytes(hostName,32);
      int host_pad = pad(host_bytes.length);

       String referrer = request.getHeader("referer");
       byte[] referrer_bytes = referrer != null ? stringToBytes(referrer,255) : EMPTY;
       int referrer_pad = pad(referrer_bytes.length);

       String agent = request.getHeader("user-agent");
       byte[] agent_bytes = agent != null ? stringToBytes(agent,64) : EMPTY;
       int agent_pad = pad(agent_bytes.length);

       String user = request.getRemoteUser();
       byte[] user_bytes = user != null ? stringToBytes(user,32) : EMPTY;
       int user_pad = pad(user_bytes.length);

       String mimeType = null;
       byte[] mime_bytes = mimeType != null ? stringToBytes(mimeType,32) : EMPTY;
       int mime_pad = pad(mime_bytes.length);

      int sample_length = 4 + 4 + uri_bytes.length + uri_pad + host_bytes.length + host_pad + referrer_bytes.length + referrer_pad + agent_bytes.length + agent_pad + user_bytes.length + user_pad + mime_bytes.length + mime_pad + 24 + 8 + 4 + 4;

      byte[] buf = new byte[1400];

      int i = 0;

      i = xdrSFlowHeader(buf,i,System.currentTimeMillis());
      i = xdrInt(buf,i,1); // one flow sample
      i = xdrInt(buf,i,1);  // sample_type = flow_sample
      i = xdrInt(buf,i, (4 * 10) + 4 + sample_length + (socketLen == 0 ? 0 : 4 + socketLen));
      i = xdrInt(buf,i,flowSequenceNo++);
      i = xdrDatasource(buf,i,dsClass,dsIndex);
      i = xdrInt(buf,i,sampling_rate);
      i = xdrInt(buf,i,sample_pool.get());
      i = xdrInt(buf,i,0);          // drops
      i = xdrInt(buf,i,0);          // input interface
      i = xdrInt(buf,i,0x3FFFFFFF); // output interface
      i = xdrInt(buf,i,socketType == 0 ? 1 : 2);  // number of flow records
      i = xdrInt(buf,i,2201);       // data format
      i = xdrInt(buf,i,sample_length);

      int method_val = 0;
      if("OPTIONS".equals(method))      method_val = 1;
      else if("GET".equals(method))     method_val = 2;
      else if("HEAD".equals(method))    method_val = 3;
      else if("POST".equals(method))    method_val = 4;
      else if("PUT".equals(method))     method_val = 5;
      else if("DELETE".equals(method))  method_val = 6;
      else if("TRACE".equals(method))   method_val = 7;
      else if("CONNECT".equals(method)) method_val = 8;

      int protocol_val = 0;
      if("HTTP/1.1".equals(protocol)) protocol_val = 1001;
      else if("HTTP/1.0".equals(protocol)) protocol_val = 1000;
      else if("HTTP/0.9".equals(protocol)) protocol_val = 9;

      i = xdrInt(buf,i,method_val);
      i = xdrInt(buf,i,protocol_val); 
      i = xdrBytes(buf,i,uri_bytes,uri_pad,true);
      i = xdrBytes(buf,i,host_bytes,host_pad,true);
      i = xdrBytes(buf,i,referrer_bytes,referrer_pad,true);
      i = xdrBytes(buf,i,agent_bytes,agent_pad,true);
      i = xdrBytes(buf,i,user_bytes,user_pad,true);
      i = xdrBytes(buf,i,mime_bytes,mime_pad,true);
      i = xdrLong(buf,i,bytes);
      i = xdrInt(buf,i,duration);
      i = xdrInt(buf,i,status);

      // socket struct
      if(socketType != 0) {
         i = xdrInt(buf,i,socketType == 4 ? 2100 : 2101);
         i = xdrInt(buf,i,socketLen);
         i = xdrInt(buf,i,6);
         i = xdrBytes(buf,i,local_addr);
         i = xdrBytes(buf,i,remote_addr);
         i = xdrInt(buf,i,local_port); 
         i = xdrInt(buf,i,remote_port);
      }
         
      sendDatagram(buf,i);
   }

   private int pad(int len) { return (4 - len) & 3; }

   private int xdrInt(byte[] buf,int offset,int val) {
      int i = offset;
      buf[i++] = (byte)(val >>> 24);
      buf[i++] = (byte)(val >>> 16);
      buf[i++] = (byte)(val >>> 8);
      buf[i++] = (byte)val;
      return i;
   }

   private int xdrLong(byte[] buf, int offset, long val) {
      int i = offset;
      buf[i++] = (byte)(val >>> 56);
      buf[i++] = (byte)(val >>> 48);
      buf[i++] = (byte)(val >>> 40);
      buf[i++] = (byte)(val >>> 32);
      buf[i++] = (byte)(val >>> 24);
      buf[i++] = (byte)(val >>> 16);
      buf[i++] = (byte)(val >>> 8);
      buf[i++] = (byte)val;
      return i;
   }

   private int xdrBytes(byte[] buf, int offset, byte[] val, int pad, boolean varLen) {
      int i = offset;
      if(varLen) i = xdrInt(buf,i,val.length);
      System.arraycopy(val,0,buf,i,val.length);
      i+=val.length;
      for(int j = 0; j < pad; j++) buf[i++] = 0;
      return i;
   }

   private int xdrBytes(byte[] buf, int offset, byte[] val, int pad) {
     return xdrBytes(buf,offset,val,pad,false);
   }

   private int xdrBytes(byte[] buf, int offset, byte[] val) {
     return xdrBytes(buf,offset,val,0);
   }

   private static byte[] addressToBytes(String address) {
     if(address == null) return null;

     byte[] bytes = null;
     try {
       InetAddress addr = InetAddress.getByName(address);
       if(addr != null) bytes = addr.getAddress();
     } catch(UnknownHostException e) {
       bytes = null;
     }
     return bytes;
   }
   

   private byte[] stringToBytes(String string, int maxLen) {
     CharsetEncoder enc = Charset.forName("UTF8").newEncoder();
     enc.onMalformedInput(CodingErrorAction.REPORT);
     enc.onUnmappableCharacter(CodingErrorAction.REPLACE);
     byte[] bytes = null;
     try { bytes = enc.encode(CharBuffer.wrap(string)).array();}
     catch(CharacterCodingException e) { ; }

     if(bytes != null && maxLen > 0 && bytes.length > maxLen) {
        byte[] original = bytes;
        bytes = new byte[maxLen];
        System.arraycopy(original,0,bytes,0,maxLen);  
     }
     return bytes;
   }

   // maximum length needed to accomodate IPv6 agent address
   static final int header_len = (4 * 5) + 16;
   private int xdrSFlowHeader(byte[] buf, int offset, long now) {
      int i = offset;

      int addrType = agentAddress.length == 4 ? 1 : 2;
      i = xdrInt(buf,i,5);
      i = xdrInt(buf,i,addrType);
      i = xdrBytes(buf,i,agentAddress,pad(agentAddress.length)); 
      i = xdrInt(buf,i,dsIndex);
      i = xdrInt(buf,i,agentSequenceNo++);
      i = xdrInt(buf,i,(int)(now - agentStartTime));

      return i;
   }

   private int xdrDatasource(byte[] buf, int offset, int ds_class, int ds_index) {
      int i = offset;

      buf[i++] = (byte)ds_class;
      buf[i++] = (byte)(ds_index >>> 16);
      buf[i++] = (byte)(ds_index >>> 8);
      buf[i++] = (byte)ds_index;

      return i;
  }

   static final int counter_data_len = 4 * 22;
   // opaque = counter_data; enterprise = 0; format = 2201
   private int xdrCounterSample(byte[] buf, int offset) {
      int i = offset;

      // sample_type = counters_sample
      i = xdrInt(buf,i,2);
      i = xdrInt(buf,i, 4 * 20);
      i = xdrInt(buf,i,counterSequenceNo++);
      i = xdrDatasource(buf,i,dsClass,dsIndex);
      i = xdrInt(buf,i,1);  // counter records
      i = xdrInt(buf,i,2201); // data format
      i = xdrInt(buf,i,(4 * 15)); // opaque length
      i = xdrInt(buf,i,method_option_count.get());
      i = xdrInt(buf,i,method_get_count.get());
      i = xdrInt(buf,i,method_head_count.get());
      i = xdrInt(buf,i,method_post_count.get());
      i = xdrInt(buf,i,method_put_count.get());
      i = xdrInt(buf,i,method_delete_count.get());
      i = xdrInt(buf,i,method_trace_count.get());
      i = xdrInt(buf,i,method_connect_count.get());
      i = xdrInt(buf,i,method_other_count.get());
      i = xdrInt(buf,i,status_1XX_count.get());
      i = xdrInt(buf,i,status_2XX_count.get());
      i = xdrInt(buf,i,status_3XX_count.get());
      i = xdrInt(buf,i,status_4XX_count.get());
      i = xdrInt(buf,i,status_5XX_count.get());
      i = xdrInt(buf,i,status_other_count.get());
 
      return i;
   }

   private void pollCounters(long now) {
      if(agentAddress == null) return;
      if(dsIndex == -1) return;

      byte[] buf = new byte[header_len + 1 + counter_data_len];

      int i = 0;

      i = xdrSFlowHeader(buf,i,now);
      i = xdrInt(buf,i,1);
      i = xdrCounterSample(buf,i);

      sendDatagram(buf,i);
   }

   private void sendDatagram(byte[] datagram, int len) {
       if(socket == null) return;
      
       for (InetSocketAddress dest : destinations) {
         try {
           DatagramPacket packet = new DatagramPacket(datagram,len,dest);
           socket.send(packet);
         } catch(IOException e) {}
       }
   }

   private long lastPollCounters = 0L;
   public synchronized void backgroundProcess() {
     updateConfig();

     if(pollingInterval > 0L) {
       long now = System.currentTimeMillis();
       if((now - lastPollCounters) > pollingInterval) {
         lastPollCounters = now;
         pollCounters(now);
       }
     }
   }

   public synchronized void startInternal() throws LifecycleException {
      agentStartTime = System.currentTimeMillis();

      try { socket = new DatagramSocket(); }
      catch(SocketException e) {}

      setState(LifecycleState.STARTING);
   }

   public synchronized void stopInternal() throws LifecycleException {
      setState(LifecycleState.STOPPING);
      if(socket != null)  socket.close();
   }
}
