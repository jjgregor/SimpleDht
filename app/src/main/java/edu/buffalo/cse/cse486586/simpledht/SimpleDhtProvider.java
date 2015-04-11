package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;

public class SimpleDhtProvider extends ContentProvider {

    public static final String TAG = SimpleDhtProvider.class.getSimpleName();;
    static final String REMOTE_PORT0 = "5554";
    static final String REMOTE_PORT1 = "5556";
    static final String REMOTE_PORT2 = "5558";
    static final String REMOTE_PORT3 = "5560";
    static final String REMOTE_PORT4 = "5562";
    String [] avds = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    static final int SERVER_PORT = 10000;
    private static String portStr;
    public HashMap<String, String> hashed = new HashMap<>();
    private HashMap<String ,String> mPredecessors = new HashMap<>();
    private HashMap<String ,String> mSuccessors = new HashMap<>();
    public ArrayList<String> nodes = new ArrayList<>();
    private String SUCCESSOR = null;
    private String PREDECESSOR = null;
    private String NODE_MIN = null;
    private String NODE_MAX = null;
    private Object myLock = new Object();
    private HashMap<String, String> files = new HashMap<>();
    private HashMap<String, String> responses = new HashMap<>();


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        if(selection.equals("\"@\"")){

            files.clear();
        }

        else if(selection.equals("\"*\"")){

            files.clear();

            Message out = new Message();
            out.message = "DELETE_ALL";
            out.key = "\"*\"";
            out.sendPort = portStr;
            out.recPort = SUCCESSOR;
            new ClientTask().execute(out);
        }else if(helper(selection) == 1){
            files.remove(selection);
            Log.v(TAG, " In delete just delteting once");
        }else{
            Message out = new Message();
            out.message = "DELETE";
            out.key = "\"*\"";
            out.sendPort = portStr;
            out.recPort = SUCCESSOR;
            new ClientTask().execute(out);
        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        String key = values.getAsString("key");
        String val = values.getAsString("value");

        if(helper(key) == 1) {
            files.put(key, val);
            try {
                Log.v(TAG, "Just inserted key " + key + " ( " + genHash(key) + " ) \n" +
                        key + " ---> " + val);
            }catch(NoSuchAlgorithmException e){
                e.printStackTrace();
            }
            return uri;
        }else {
            Message out = new Message();
            out.recPort = SUCCESSOR;
            out.sendPort = portStr;
            out.key = key;
            out.val = val;
            out.message = "INSERT";
            new ClientTask().execute(out);
            Log.v(TAG, "Insert forwarding to successor.");

            return uri;
        }

    }

    @Override
    public boolean onCreate() {

        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.v(TAG, "Created a ServerSocket.");
        } catch (IOException e) {
            Log.v(TAG, "Can't create a ServerSocket");
        }

        if(!portStr.equals(REMOTE_PORT0)){

            Log.v(TAG, " In onCreate() calling a ClientTask() on :" + portStr);
            Message msg = new Message();
            msg.sendPort = portStr;
            msg.recPort = REMOTE_PORT0;
            msg.message = "JOIN_REQUEST";

            new ClientTask().execute(msg);
        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {

        MatrixCursor mc = new MatrixCursor(new String[]{"key","value"});//
        Message outgoing = new Message();

        Log.v(TAG, "Entered query: query=" + selection + " @ == sel? " + selection.equals("@")
        + " \"@\" == sel? " + selection.equals("\"@\""));

        Log.v(TAG, "begin if statement, files.size() = " + files.keySet().size());

        if(selection.equals("\"@\"") || selection.equals("@")){

            Log.v(TAG, "Entered if for @");

            for(String s : files.keySet()){

                String[] row = new String[2];
                row[0] = s;
                row[1]= files.get(s);
                mc.addRow(row);
            }
            Log.v(TAG, "In @ exited for returning mc = "+ mc.getCount());
            return mc;
        }

        else if (selection.equals("\"*\"") || selection.equals("*")){
            if(SUCCESSOR != null && !SUCCESSOR.equals(portStr)) {
                outgoing.message = "GLOBAL_QUERY";
                outgoing.sendPort = portStr;
                outgoing.recPort = SUCCESSOR;

                Log.v(TAG, "In query making global query ****");
                Log.v(TAG, "Sending global to " + SUCCESSOR + " from " + portStr);

                synchronized (myLock){
                    new ClientTask().execute(outgoing);
                    Log.v(TAG, "Waiting for response in global query ****. FROM: " + SUCCESSOR);
                    try {
                        myLock.wait();
                        Log.v(TAG, "wait lock in global.");
                    }catch(InterruptedException e){
                        e.printStackTrace();
                    }
                }
                Log.v(TAG, "Exited wait lock, inserting into table.");
                Log.v(TAG, "response size = " + responses.size());
                for(HashMap.Entry<String,String> entry : responses.entrySet()){
                    String[] row = new String[2];
                    row[0] = entry.getKey();
                    row[1] = responses.get(entry.getKey());
                    //responses.remove(entry.getKey());
                    mc.addRow(row);
                    Log.i(TAG,"IN ********* Found: " + row[0] + " --> " + row[1]);
                }
                Log.v(TAG, "Got a response in global query ****.");
                Log.v(TAG, "End of ****** query mc count = " + mc.getCount());

                //return mc;
            }else {
                Log.v(TAG, "*** query ending for loop");
            }
            for (String s : files.keySet()) {
                Log.v(TAG, "Entered for loop s = " + s);
                String[] row = new String[2];
                row[0] = s;
                row[1] = files.get(s);
                mc.addRow(row);
            }
            Log.v(TAG, "End of ****** query ending for loop mc count = " + mc.getCount());
            return mc;
        }else{
            Log.v(TAG, "In regular query.");
            if(helper(selection) == 0){
                Log.v(TAG, " In regular query forwarding to successor.");
                outgoing.sendPort = portStr;
                outgoing.recPort = SUCCESSOR;
                Log.v(TAG,"in regular query, recPort = " + outgoing.recPort);
                outgoing.key = selection;
                outgoing.message = "QUERY";
                //new ClientTask().execute(outgoing);

                synchronized (myLock){
                    new ClientTask().execute(outgoing);

                    Log.v(TAG, "Waiting for response in regular query ****. FROM: " + SUCCESSOR);
                    try {
                        myLock.wait();
                    }catch(InterruptedException e){
                        e.printStackTrace();
                    }
                }
                Log.v(TAG, "exiting wait");

                String[] row = new String[2];
                row[0] = selection;
                row[1] = responses.get(selection);
                //responses.remove(selection);
                Log.i(TAG,"In if else Found: " + row[0] + " --> " + row[1]);
                mc.addRow(row);
                Log.v(TAG, "End of regular query mc count = " + mc.getCount());

            }else{
                String[] row = new String[2];
                row[0] = selection;
                row[1] = files.get(selection);
                mc.addRow(row);
                Log.i(TAG," In else Found: " + row[0] + " --> " + row[1]);
            }
            Log.i(TAG, "Returning responses. Nb. rows =  " + mc.getCount());
            return mc;
        }
//        Log.i(TAG, "Returning responses. Nb. rows =  " + mc.getCount());
//        return mc;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            Log.v(TAG, "Entering ServerTask as :" + portStr);
            Message incoming;
            Message outgoing;
            SUCCESSOR = portStr;
            PREDECESSOR = portStr;

            try {

                if(portStr.equals(REMOTE_PORT0)){

                    NODE_MIN = portStr;
                    NODE_MAX = portStr;
                    nodes.add(genHash(portStr));

                    Log.v(TAG, "In onCreate() as 5554.");

                    for (String string : avds) {
                        hashed.put(genHash(string), string);
                    }
                }

                ServerSocket serverSocket = sockets[0];
                Log.v(TAG, "ServerSocket = " + serverSocket);

                while(true) {

                    Socket client = serverSocket.accept();
                    ObjectInputStream reader = new ObjectInputStream(client.getInputStream());
                    incoming = (Message) reader.readObject();
                    String type = incoming.message;
                    Log.v(TAG, "incoming.message = " + incoming.message);
                    String key = incoming.key;

                    if(type.equals("JOIN_REQUEST")){
                        Log.v(TAG, "Going to join().");

                        join(incoming);

                        Log.v(TAG, "exited join().");

                    }

                    if(type.equals("JOIN_REPLY")){
                        SUCCESSOR = incoming.succ;
                        PREDECESSOR = incoming.pred;
                        NODE_MAX = incoming.maxNode;
                        NODE_MIN = incoming.minNode;

                        Log.v(TAG, "SUCCESSOR = " + SUCCESSOR);
                        Log.v(TAG, "PREDECESSOR = " + PREDECESSOR);
                        Log.v(TAG, "NODE_MIN = " + NODE_MIN);
                        Log.v(TAG, "NODE_MAX = " + NODE_MAX);

                    }

                    else if (type.equals("INSERT")){
                        if(helper(incoming.key) == 1) {
                            Log.v(TAG, "In ServerTask with type as insert, going to insertHelper().");
                            files.put(incoming.key, incoming.val);

                        }else if(helper(incoming.key) == 0 && !incoming.sendPort.equals(portStr)){
                            Log.v(TAG, " In ServerTask as insert and updating successor and forwarding.");
                            incoming.recPort = SUCCESSOR;
                            new ClientTask().execute(incoming);
                        }
                    }

                    else if (type.equals("DELETE")){
                        incoming.recPort = SUCCESSOR;
                        if (helper(incoming.key) == 0){
                            Log.v(TAG, " ServerTask on a delete, updating successor and forwarding.");
                            outgoing = incoming;
                            outgoing.recPort = SUCCESSOR;
                            new ClientTask().execute(outgoing);
                        }
                        else{
                            new ClientTask().execute(incoming);
                        }
                    }
                    else if(type.equals("DELETE_ALL")){
                        delete(null,"\"@\"",null);
                        incoming.recPort = SUCCESSOR;
                        if(!SUCCESSOR.equals(incoming.sendPort))
                            new ClientTask().execute(incoming);
                    }
                    else if(type.equals("QUERY")){
                        incoming.recPort = SUCCESSOR;
                        Log.v(TAG, " ServerTask QUERY, entering.");
                        if(helper(incoming.key) == 1){
                            Log.v(TAG, "ServerTAsk QUERY, updating message and forwarding.");
                            //incoming.sendPort = portStr;
                            incoming.message = "RESPONSE_SIMPLE";
                            incoming.destination = incoming.sendPort;
                            incoming.addResponse(incoming.key, files.get(incoming.key));
                            new ClientTask().execute(incoming);
                        }
                        else if (!incoming.sendPort.equals(SUCCESSOR)){
                            Log.v(TAG, " ServerTask QUERY, forwarding to successor.");
                            new ClientTask().execute(incoming);
                        }
                        else
                            Log.i(TAG, "Query back to self! Shouldn't happen");
                    }
                    else if(type.equals("RESPONSE_SIMPLE")){
                        if(incoming.sendPort.equals(portStr)) {
                            synchronized (myLock) {
                                responses.put(incoming.key, incoming.responses.get(incoming.key));
                                myLock.notify();
                            }
                        }
                        else{
                            incoming.recPort = SUCCESSOR;
                            new ClientTask().execute(incoming);
                        }
                    }
                    else if (type.equals("GLOBAL_QUERY")) {
                        Log.v(TAG, "Entered GLOBAL_QUERY");
                        Log.v(TAG, "responses.entrySet();" + responses.entrySet().size());

                        for (HashMap.Entry<String, String> entry : responses.entrySet()) {
                            incoming.responses.put(entry.getKey(), entry.getValue());
                            Log.v(TAG, "response size in server global query: " +
                                    incoming.responses.put(entry.getKey(), entry.getValue()));
                        }
                        incoming.recPort = SUCCESSOR;
                        if (incoming.sendPort.equals(SUCCESSOR)) {
                            Log.v(TAG, "GLOBAL_QUERY sending response");
                            incoming.message = "RESPONSE_GLOBAL";
                        }
                        new ClientTask().execute(incoming);
                        Log.v(TAG, "ServerTask GLOBAL_QUERY forwarding to  " + incoming.recPort);

                    }
                    else if(type.equals("RESPONSE_GLOBAL")){
                        Log.v(TAG, "got RESPONSE_GLOBAL");
                        if(incoming.sendPort.equals(portStr)) {
                            Log.v(TAG, "REPONSE_GLOBAL sendPort == portStr");
                            synchronized (myLock) {
                                Log.v(TAG, "HERERERERERE reposnses : " + incoming.responses.entrySet().size());
                                for(HashMap.Entry<String,String> entry : incoming.responses.entrySet()){
                                    Log.v(TAG, "RESPONSE_GLOBAL RETURNING for responses : " +
                                            responses.put(entry.getKey(),entry.getValue()));
                                    responses.put(entry.getKey(),entry.getValue());
                                }
                                Log.v(TAG, "notifying lock");
                                myLock.notify();
                            }
                        }
                        else{
                            incoming.recPort = SUCCESSOR;
                            new ClientTask().execute(incoming);
                        }
                    }
                }
            } catch (IOException e) {
                //e.printStackTrace();
                Log.v(TAG, "Can't get input!");
            } catch (ClassNotFoundException e) {
                Log.v(TAG, "ClassNotFoundException thrown.");
            } catch (NoSuchAlgorithmException e) {
                Log.v(TAG,"NO SUCH ALGO!!!");
            }

            return null;
        }
    }

    private int helper(String s){

        if(SUCCESSOR == null || SUCCESSOR.equals(portStr)) {
            Log.v(TAG, " SUCC = " + SUCCESSOR);
            return 1;

        }
        try {
            String key = genHash(s);
            String pre = genHash(PREDECESSOR);
            String port = genHash(portStr);
            String max = genHash(NODE_MAX);
            String min = genHash(NODE_MIN);

            if(key.compareTo(pre) > 0 && key.compareTo(port) <= 0){
                return 1;
            }else if(key.compareTo(max) > 0 && port.equals(min)){
                return 1;
            }else if(key.compareTo(min) < 0 && port.equals(min)){
                return 1;
            }else{
                return 0;
            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return 0;
        }
    }

    public void join(Message msg){
        String pred;
        String succ;

        try {

            String nodeHash = genHash(msg.sendPort);
            nodes.add(nodeHash);
            Collections.sort(nodes);

            for (int i = 0; i < nodes.size(); i++) {
                if (i == nodes.size() - 1) {
                    pred = hashed.get(nodes.get(i - 1));
                    succ = hashed.get(nodes.get(0));
                } else if (i == 0) {
                    pred = hashed.get(nodes.get(nodes.size() - 1));
                    succ = hashed.get(nodes.get(1));
                } else {
                    pred = hashed.get(nodes.get(i - 1));
                    succ = hashed.get(nodes.get(i + 1));
                }
                mPredecessors.put(hashed.get(nodes.get(i)),pred);
                mSuccessors.put(hashed.get(nodes.get(i)),succ);
                String avd = hashed.get(nodes.get(i));
//                Log.v(TAG,"AVD + " + avd + " New PRED " + mPredecessors.get(avd) + "NEW SUCC :" + mSuccessors.get(avd));

                Message outgoing = new Message();
                outgoing.message = "JOIN_REPLY";
                outgoing.sendPort = portStr;
                outgoing.minNode = hashed.get(nodes.get(0));
                outgoing.maxNode = hashed.get(nodes.get(nodes.size()-1));
                outgoing.succ = succ;
                outgoing.pred = pred;
                outgoing.recPort = hashed.get(nodes.get(i));

                new ClientTask().execute(outgoing);
            }
        }catch (NoSuchAlgorithmException e) {
            Log.v(TAG, "no such algo catch!");
        }
    }

    private class ClientTask extends AsyncTask <Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msgs) {

            Message msg = msgs[0];
            String rec = msg.recPort;
            Log.v(TAG,"IN CLIENT TASK, rec = " + rec);

            try {

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(rec) * 2);
                ObjectOutputStream writer = new ObjectOutputStream(socket.getOutputStream());
                writer.writeObject(msg);
                writer.flush();
                writer.close();
                socket.close();

            } catch (Exception e) {
                Log.v(TAG, "ClientTask UnknownHostException");
            }

            return null;
        }
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
