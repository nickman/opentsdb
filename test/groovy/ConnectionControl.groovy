import com.heliosapm.utils.jmx.JMXHelper;

def connector = null;
def tsdServer = JMXHelper.objectName("net.opentsdb:service=TSDServer");
def mbeanServer = null
def counters = new TreeSet();
def host = "localhost";
def port = 4242;
def sockets = [];

getCounters = {
    return JMXHelper.getAttributes(tsdServer, mbeanServer, counters);
}

resetCounters = {
    JMXHelper.invoke(tsdServer, mbeanServer, "resetCounters");
}


closeSockets = {
    int socks = sockets.size();
    sockets.each() {
        try { it.close(); } catch (x) {}
    }
    sockets.clear();
}

connect = {
    Socket socket = new Socket(host, port);
    socket.setSoTimeout(10000);
    sockets.add(socket);
    Thread.sleep(200);
    assert socket.isConnected();
    return socket;
}

close = { sock ->
    sock.close();
    Thread.sleep(100);
    assert sock.isClosed();
    
}


ping = { sock ->
    msg = null;
    sock.withStreams({ een, out ->
        out << "ping\n";
        out.flush();
        byte[] b = new byte[4];
        een.read(b);
        msg = new String(b);
    });
    assert "pong".equals(msg);
    return msg;
}

try {
    connector = JMXHelper.getJMXConnection("service:jmx:attach:///[.*OpenTSDBMain.*]");
    mbeanServer = connector.getMBeanServerConnection();
    println "Connected.";
    allAttrNames = JMXHelper.getAttributeNames(tsdServer, mbeanServer);
    allAttrNames.each() {
        if(it.contains("Connections") || it.contains("Exceptions")) counters.add(it);
    }
    resetCounters();
    
    
    
    sock = connect(); 
    println getCounters();
    assert getCounters()["ActiveConnections"] == 1;
    close(sock);
    assert getCounters()["ActiveConnections"] == 0;

//    sock = connect();
//    assert getCounters()["ActiveConnections"] == 1;
//    Thread.sleep(6000);
//    assert getCounters()["ActiveConnections"] == 0;
//
    
    println getCounters();
    
    sock = connect();
    ping(sock);
    
    for(i in 1..10) {
        try {
            sock = connect();
            //ping(sock);
            println "Connected #$i.  Active: ${getCounters()['ActiveConnections']}";
        } catch (x) {
            println "Connect Failed on #$i";
        }
    }
 
    closeSockets();
    //sock = connect();
    println getCounters();
} finally {
    try { connector.close(); println "JMX Connector Closed"; } catch (x) {}
    int socks = sockets.size();
    sockets.each() {
        try { it.close(); } catch (x) {}
    }
    sockets.clear();
    println "$socks Sockets Closed";
}

return null;