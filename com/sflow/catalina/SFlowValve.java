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
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.Set;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.MemoryManagerMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.lang.management.MemoryMXBean;
import java.lang.management.ClassLoadingMXBean;
import java.lang.management.CompilationMXBean;
import java.lang.management.MemoryUsage;
import java.lang.instrument.Instrumentation;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
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

   private static final byte[] EMPTY = {}; 
 
   private static final int DEFAULT_SFLOW_PORT      = 6343; 
   private static final int DS_CLASS_PHYSICAL       = 2;
   private static final int DS_CLASS_LOGICAL        = 3;
   private static final int OS_NAME_JAVA            = 13;
   private static final int MACHINE_TYPE_UNKNOWN    = 0;
   private static final int VIR_DOMAIN_RUNNING      = 1;

   public static UUID myUUID        = null;
   public static String myHostname  = null;

   private static long pollingInterval = 0L;
   private static byte[] agentAddress = null;
   private static ArrayList<InetSocketAddress> destinations;
   private static int parentDsIndex = -1;

   private static DatagramSocket socket = null;

   public static int dsIndex() {
      int dsIndex = -1;
      String indexStr = System.getProperty("sflow.dsindex");
      if(indexStr != null) {
        try { dsIndex = Integer.parseInt(indexStr); }
        catch(NumberFormatException e) { ; }
      }
      return dsIndex;
   }

   public static void uuid(UUID uuid) { myUUID = uuid; };
   public static UUID uuid() {
      if(myUUID != null) return myUUID;

      String uuidStr = System.getProperty("sflow.uuid");
      if(uuidStr != null) {
        try { myUUID = UUID.fromString(uuidStr); }
        catch(IllegalArgumentException e) { ; }
      }
      if(myUUID != null) return myUUID;

      myUUID = new UUID(0L,0L); 
      return myUUID;
   }

   public static void hostname(String hostname) { myHostname = hostname; }
   public static String hostname() {
      if(myHostname != null) return myHostname;

      myHostname = System.getProperty("sflow.hostname");
      if(myHostname != null) return myHostname;

      //RuntimeMXBean runtimeMX = ManagementFactory.getRuntimeMXBean();
      //myHostname = runtimeMX.getName();
      myHostname = "apache-tomcat";
      return myHostname;
   }

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
      String parent = null;
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
             else if("ds_index".equals(key)) parent = value;
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

        // set parent
        if(parent != null) {
          try { parentDsIndex = Integer.parseInt(parent); }
          catch(NumberFormatException e) {
            parentDsIndex = -1;
          }
        }
        else parentDsIndex = -1;

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
              int port = DEFAULT_SFLOW_PORT;
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

   private int dsIndex = dsIndex();
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

   int max_flow_data_len = 1024;
   private int xdrFlowSample(byte[] buf, int offset, Request request, Response response, String method, int status,int duration) {

      byte[] local_addr = addressToBytes(request.getLocalAddr());
      byte[] remote_addr =addressToBytes(request.getRemoteAddr());
      int local_port = request.getLocalPort();
      int remote_port = request.getRemotePort(); 
      String protocol = request.getProtocol();

      int socketType = 0;
      if(local_addr.length == 4 && remote_addr.length == 4) {
         socketType = 4;
      }
      else if(local_addr.length == 16 && remote_addr.length == 16) {
         socketType = 6;
      }

      long bytes_read = (request.getCoyoteRequest()).getBytesRead();
      long bytes_written = response.getBytesWritten(true);

      String uri = request.getRequestURI();
 
      Host host = request.getHost();
      String hostName = host != null ? host.getName() : "";

      String referrer = request.getHeader("referer");
      String agent = request.getHeader("user-agent");
      String user = request.getRemoteUser();
      String xff = request.getHeader("x-forwarded-for");

      String mimeType = null;

      int i = offset;

      i = xdrInt(buf,i,1);  // sample_type = flow_sample
      int sample_len_idx = i;
      i += 4;
      i = xdrInt(buf,i,flowSequenceNo++);
      i = xdrDatasource(buf,i,DS_CLASS_LOGICAL,dsIndex);
      i = xdrInt(buf,i,sampling_rate);
      i = xdrInt(buf,i,sample_pool.get());
      i = xdrInt(buf,i,0);          // drops
      i = xdrInt(buf,i,0);          // input interface
      i = xdrInt(buf,i,0x3FFFFFFF); // output interface
      int sample_nrecs_idx = i;
      int sample_nrecs = 0;
      i += 4;

      i = xdrInt(buf,i,2206);      
      int opaque_len_idx = i;
      i += 4;

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
      i = xdrString(buf,i,uri,255);
      i = xdrString(buf,i,hostName,64);
      i = xdrString(buf,i,referrer,255);
      i = xdrString(buf,i,agent,128);
      i = xdrString(buf,i,xff,64);
      i = xdrString(buf,i,user,32);
      i = xdrString(buf,i,mimeType,64);
      i = xdrLong(buf,i,bytes_read);
      i = xdrLong(buf,i,bytes_written);
      i = xdrInt(buf,i,duration);
      i = xdrInt(buf,i,status);
      xdrInt(buf,opaque_len_idx, i - opaque_len_idx - 4);
      sample_nrecs ++;

      // socket struct
      if(socketType != 0) {
         i = xdrInt(buf,i,socketType == 4 ? 2100 : 2101);
         opaque_len_idx = i;
         i += 4;
         i = xdrInt(buf,i,6);  // protocol = TCP
         i = xdrBytes(buf,i,local_addr);
         i = xdrBytes(buf,i,remote_addr);
         i = xdrInt(buf,i,local_port); 
         i = xdrInt(buf,i,remote_port);
         xdrInt(buf,opaque_len_idx, i - opaque_len_idx - 4);
         sample_nrecs++;
      }

      // fill in sample length and record count
      xdrInt(buf,sample_len_idx, i - sample_len_idx - 4);
      xdrInt(buf,sample_nrecs_idx, sample_nrecs);

      return i;       
   }

   private void sampleRequest(Request request, Response response, String method, int status,int duration) {
     if(agentAddress == null) return;
     if(dsIndex == -1) dsIndex = request.getLocalPort();

     long now = System.currentTimeMillis();
     byte[] buf = new byte[max_header_len + 4 + max_flow_data_len];
     int i = 0;

      i = xdrSFlowHeader(buf,i,now);
      i = xdrInt(buf,i,1);
      i = xdrFlowSample(buf,i, request,response,method,status,duration);

      sendDatagram(buf,i); 
   }

   private static int pad(int len) { return (4 - len) & 3; }

   private static int xdrInt(byte[] buf,int offset,int val) {
      int i = offset;
      buf[i++] = (byte)(val >>> 24);
      buf[i++] = (byte)(val >>> 16);
      buf[i++] = (byte)(val >>> 8);
      buf[i++] = (byte)val;
      return i;
   }

   private static int xdrLong(byte[] buf, int offset, long val) {
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

   private static int xdrBytes(byte[] buf, int offset, byte[] val, int pad, boolean varLen) {
      int i = offset;
      if(varLen) i = xdrInt(buf,i,val.length);
      System.arraycopy(val,0,buf,i,val.length);
      i+=val.length;
      for(int j = 0; j < pad; j++) buf[i++] = 0;
      return i;
   }

   private static int xdrBytes(byte[] buf, int offset, byte[] val, int pad) {
     return xdrBytes(buf,offset,val,pad,false);
   }

   private static int xdrBytes(byte[] buf, int offset, byte[] val) {
     return xdrBytes(buf,offset,val,0);
   }

   public static int xdrString(byte[] buf, int offset, String str, int maxLen) {
     byte[] bytes = str != null ? stringToBytes(str,maxLen) : EMPTY;
     int pad = pad(bytes.length);
     return xdrBytes(buf,offset,bytes,pad,true);
   }

    public static int xdrUUID(byte[] buf, int offset, UUID uuid) {
	int i = offset;

	i = xdrLong(buf,i,uuid.getMostSignificantBits());
	i = xdrLong(buf,i,uuid.getLeastSignificantBits());  

	return i;
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

   private static byte[] stringToBytes(String string, int maxLen) {
     CharsetEncoder enc = Charset.forName("US-ASCII").newEncoder();
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
   static final int max_header_len = 36;
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

    private long totalThreadTime = 0L;
    private HashMap<Long,Long> prevThreadCpuTime = null;
   static final int max_counter_data_len = 512;
   // opaque = counter_data; enterprise = 0; format = 2201
   private int xdrCounterSample(byte[] buf, int offset) {
      int i = offset;

      // sample_type = counters_sample
      i = xdrInt(buf,i,2);
      int sample_len_idx = i;
      i += 4;
      i = xdrInt(buf,i,counterSequenceNo++);
      i = xdrDatasource(buf,i,DS_CLASS_LOGICAL,dsIndex);
      int sample_nrecs_idx = i;
      int sample_nrecs = 0;
      i += 4;

      // JVM Counters
      RuntimeMXBean runtimeMX = ManagementFactory.getRuntimeMXBean();

      String hostname = hostname();

      UUID uuid = uuid();

      String os_release = System.getProperty("java.version");
      String vm_name = runtimeMX.getVmName();
      String vm_vendor = runtimeMX.getVmVendor();
      String vm_version = runtimeMX.getVmVersion();

      List<GarbageCollectorMXBean> gcMXList = ManagementFactory.getGarbageCollectorMXBeans();
      long gcCount = 0;
      long gcTime = 0;
      for(GarbageCollectorMXBean gcMX : gcMXList)  {
        gcCount += gcMX.getCollectionCount();
        gcTime += gcMX.getCollectionTime();
      }

      CompilationMXBean compilationMX = ManagementFactory.getCompilationMXBean();
      long compilationTime = 0L;
      if(compilationMX.isCompilationTimeMonitoringSupported()) {
        compilationTime = compilationMX.getTotalCompilationTime();
      }

      long cpuTime = 0L;
      ThreadMXBean threadMX = ManagementFactory.getThreadMXBean();
      OperatingSystemMXBean osMX = ManagementFactory.getOperatingSystemMXBean();
      String className = osMX.getClass().getName();
      if ("com.sun.management.OperatingSystem".equals(className)
           || "com.sun.management.UnixOperatingSystem".equals(className)) {
	  cpuTime = ((com.sun.management.OperatingSystemMXBean)osMX).getProcessCpuTime();
	  cpuTime /= 1000000L;
      } else {
	  if(threadMX.isThreadCpuTimeEnabled()) {
	      long[] ids = threadMX.getAllThreadIds();
	      HashMap<Long,Long> threadCpuTime = new HashMap<Long,Long>();
	      for(int t = 0; t < ids.length; t++) {
		  long id = ids[t];
		  if(id >= 0) {
		      long threadtime = threadMX.getThreadCpuTime(id);
		      if(threadtime >= 0) {
			  threadCpuTime.put(id,threadtime);
			  long prev = 0L;
			  if(prevThreadCpuTime != null) {
			      Long prevl = prevThreadCpuTime.get(id);
			      if(prevl != null) prev = prevl.longValue();
			  }
			  if(prev <= threadtime) totalThreadTime += (threadtime - prev);
			  else totalThreadTime += threadtime;
		      }
		  }
	      }
	      cpuTime = (totalThreadTime / 1000000L) + gcTime + compilationTime;
              prevThreadCpuTime = threadCpuTime;
	  }
      }

      MemoryMXBean memoryMX = ManagementFactory.getMemoryMXBean();
      MemoryUsage heapMemory =  memoryMX.getHeapMemoryUsage();
      MemoryUsage nonHeapMemory = memoryMX.getNonHeapMemoryUsage();
   
      int nrVirtCpu = osMX.getAvailableProcessors();

      long memory = heapMemory.getCommitted() + nonHeapMemory.getCommitted();
      long maxMemory = heapMemory.getMax() + nonHeapMemory.getCommitted(); 

      ClassLoadingMXBean classLoadingMX = ManagementFactory.getClassLoadingMXBean();

      long fd_open_count = 0L;
      long fd_max_count = 0L;
      if("com.sun.management.UnixOperatingSystem".equals(className)) {
         fd_open_count = ((com.sun.management.UnixOperatingSystemMXBean)osMX).getOpenFileDescriptorCount();
         fd_max_count = ((com.sun.management.UnixOperatingSystemMXBean)osMX).getMaxFileDescriptorCount();
      }

      // host_descr
      i = xdrInt(buf,i,2000);
      int opaque_len_idx = i;
      i += 4;
      i = xdrString(buf,i,hostname,64);
      i = xdrUUID(buf,i,uuid);
      i = xdrInt(buf,i,MACHINE_TYPE_UNKNOWN);
      i = xdrInt(buf,i,OS_NAME_JAVA);
      i = xdrString(buf,i,os_release,32);
      xdrInt(buf,opaque_len_idx, i - opaque_len_idx - 4);
      sample_nrecs++;

      // host_adapters
      // i = xdrInt(buf,i,2001)
      // Java does not create virtual network interfaces
      // Note: sFlow sub-agent on host OS reports host network adapters

      if(parentDsIndex > 0) {
	 // host_parent
	 i = xdrInt(buf,i,2002);
         opaque_len_idx = i;
         i += 4;
	 i = xdrInt(buf,i,DS_CLASS_PHYSICAL);
	 i = xdrInt(buf,i,parentDsIndex);
         xdrInt(buf,opaque_len_idx, i - opaque_len_idx - 4);
         sample_nrecs++;
      }

      // virt_cpu
      i = xdrInt(buf,i,2101);
      opaque_len_idx = i;
      i += 4; 
      i = xdrInt(buf,i,VIR_DOMAIN_RUNNING);
      i = xdrInt(buf,i,(int)cpuTime);
      i = xdrInt(buf,i,nrVirtCpu);
      xdrInt(buf,opaque_len_idx, i - opaque_len_idx - 4);
      sample_nrecs++;
 
      // virt_memory
      i = xdrInt(buf,i,2102);
      opaque_len_idx = i;
      i += 4;
      i = xdrLong(buf,i,memory);
      i = xdrLong(buf,i,maxMemory);
      xdrInt(buf,opaque_len_idx, i - opaque_len_idx - 4);
      sample_nrecs++;

      // virt_disk_io
      // i = xdrInt(buf,i,2103);
      // Currently no JMX bean providing JVM disk I/O stats
      // Note: sFlow sub-agent on host OS provides overall disk I/O stats

      // virt_net_io
      // i = xdrInt(buf,i,2104);
      // Currently no JMX bena providing JVM network I/O stats
      // Note: sFlow sub-agent on host OS provides overall network I/O stats

      // jvm_runtime
      i = xdrInt(buf,i,2105);
      opaque_len_idx = i;
      i += 4;
      i = xdrString(buf,i,vm_name,64);
      i = xdrString(buf,i,vm_vendor,32);
      i = xdrString(buf,i,vm_version,32);
      xdrInt(buf,opaque_len_idx, i - opaque_len_idx - 4);
      sample_nrecs++;

      // jvm_statistics
      i = xdrInt(buf,i,2106);
      opaque_len_idx = i;
      i += 4;
      i = xdrLong(buf,i,heapMemory.getInit());
      i = xdrLong(buf,i,heapMemory.getUsed());
      i = xdrLong(buf,i,heapMemory.getCommitted());
      i = xdrLong(buf,i,heapMemory.getMax());
      i = xdrLong(buf,i,nonHeapMemory.getInit());
      i = xdrLong(buf,i,nonHeapMemory.getUsed());
      i = xdrLong(buf,i,nonHeapMemory.getCommitted());
      i = xdrLong(buf,i,nonHeapMemory.getMax());
      i = xdrInt(buf,i,(int)gcCount);
      i = xdrInt(buf,i,(int)gcTime);
      i = xdrInt(buf,i,(int)classLoadingMX.getLoadedClassCount());
      i = xdrInt(buf,i,(int)classLoadingMX.getTotalLoadedClassCount());
      i = xdrInt(buf,i,(int)classLoadingMX.getUnloadedClassCount());
      i = xdrInt(buf,i,(int)compilationTime);
      i = xdrInt(buf,i,(int)threadMX.getThreadCount());
      i = xdrInt(buf,i,(int)threadMX.getDaemonThreadCount());
      i = xdrInt(buf,i,(int)threadMX.getTotalStartedThreadCount());
      i = xdrInt(buf,i,(int)fd_open_count);
      i = xdrInt(buf,i,(int)fd_max_count);
      xdrInt(buf,opaque_len_idx, i - opaque_len_idx - 4);
      sample_nrecs++;

      // HTTP counters
      i = xdrInt(buf,i,2201);
      opaque_len_idx = i;
      i += 4;
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
      xdrInt(buf,opaque_len_idx, i - opaque_len_idx - 4);
      sample_nrecs++;

      // Worker pool
      MBeanServer mbserver = ManagementFactory.getPlatformMBeanServer();
      ObjectName oname;
      try {
	  oname = ObjectName.getInstance("Catalina:type=ThreadPool,*");
          Set<ObjectInstance> mbeans = mbserver.queryMBeans(oname,null);

	  // assume that only one connector is being used - pick pool with largest currentThreadCount
          int workers_active = 0;
          int workers_idle = 0;
          int workers_max = 0;
          int maxCurrentThreadCount = 0;
          for(ObjectInstance oi : mbeans) {
              oname = oi.getObjectName();
	      Integer maxThreads = (Integer)mbserver.getAttribute(oname,"maxThreads");
              Integer currentThreadCount = (Integer)mbserver.getAttribute(oname,"currentThreadCount");
              Integer currentThreadsBusy = (Integer)mbserver.getAttribute(oname,"currentThreadsBusy");
            
              if(currentThreadCount != null && currentThreadCount.intValue() > maxCurrentThreadCount) {
                  maxCurrentThreadCount = currentThreadCount.intValue();

		  workers_active = currentThreadsBusy.intValue();
                  workers_idle = currentThreadCount.intValue() - workers_active;
                  workers_max = maxThreads.intValue();
              }
          }
          if(maxCurrentThreadCount > 0) {
	      i = xdrInt(buf,i,2206);
              opaque_len_idx = i;
              i += 4;
              i = xdrInt(buf,i,workers_active);
              i = xdrInt(buf,i,workers_idle);
              i = xdrInt(buf,i,workers_max);
              i = xdrInt(buf,i,-1); // req_delayed
              i = xdrInt(buf,i,-1); // req_dropped
	      xdrInt(buf,opaque_len_idx, i - opaque_len_idx - 4);
	      sample_nrecs++;
          }
      } catch(Exception e) {;}

      // fill in sample length and number of records
      xdrInt(buf,sample_len_idx, i - sample_len_idx - 4);
      xdrInt(buf,sample_nrecs_idx, sample_nrecs);
 
      return i;
   }

   private void pollCounters(long now) {
      if(agentAddress == null) return;
      if(dsIndex == -1) return;

      byte[] buf = new byte[max_header_len + 4 + max_counter_data_len];

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
