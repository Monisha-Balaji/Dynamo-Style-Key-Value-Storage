package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {


    private static final String TAG = SimpleDynamoProvider.class.getName();
    static final int SERVER_PORT = 10000;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    String[] PORTS= new String[]{"11108","11112","11116","11120","11124"};
    ArrayList<String> portlist = new ArrayList<String>();
    HashMap<String,String> fileMap = new HashMap<String,String>();
    HashMap<String,String> fileMap1 = new HashMap<String,String>();
    public Uri uri=null;
    String myPort="";
    String pred="";
    String succ="";
    String succ_unhash="";
    String pred_unhash = "";
    String[] ports = new String[5];
    String[] unhashed = new String[5];
    ArrayList<String> querydata = new ArrayList<String>();


    @Override
    // Reference: https://stackoverflow.com/questions/23892257/contentprovider-openfile-how-to-delete-file
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        try{
            Integer avd = Integer.parseInt(myPort);
            avd = avd / 2;
            String myporthash = genHash(avd.toString());
            if(selection.equals("@")){
                boolean flag;
                String[] filelist = getContext().fileList();
                File directory = getContext().getFilesDir();
                for(int i=0;i<filelist.length;i++){
                    File file = new File(directory,filelist[i]);
                    if (file.exists()) {
                        flag = file.delete();
                        if(flag==true) {
                            Log.i(TAG, "File" + i + "deleted");
                        }
                    }
                }

            }
            else if(selection.equals("*")) {
                if (succ.equals("") && pred.equals("")) {
                    boolean flag;
                    String[] filelist = getContext().fileList();
                    File directory = getContext().getFilesDir();
                    for (int i = 0; i < filelist.length; i++) {
                        File file = new File(directory, filelist[i]);
                        if (file.exists()) {
                            flag = file.delete();
                            if (flag == true) {
                                Log.i(TAG, "File" + i + "deleted");
                            }
                        }
                    }
                } else {
                    boolean flag;
                    String[] filelist = getContext().fileList();
                    File directory = getContext().getFilesDir();
                    for (int i = 0; i < filelist.length; i++) {
                        File file = new File(directory, filelist[i]);
                        if (file.exists()) {
                            flag = file.delete();
                            if (flag == true) {
                                Log.i(TAG, "File" + i + "deleted");
                            }
                        }
                    }
                    String msg = "Important:delete all";
                    for (int i = 0; i < ports.length; i++) {
                        String temp = fileMap1.get(ports[i]);
                        if (!myporthash.equals(ports[i])) {
                            Integer port = Integer.parseInt(temp) * 2;
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    port);
                            PrintWriter out = new PrintWriter(socket.getOutputStream(), true); // Output Stream that client uses to send messages
                            Log.i(TAG, "Message to send in the for : " + msg);
                            out.println(msg); //Sends the message
                            Log.i(TAG, "Message sent " + msg);
                            out.flush();
                            socket.close();

                        }
                    }
                }
            }
            else {
                String keyhash = genHash(selection);
                if (succ.equals("") && pred.equals("")) {
                    boolean flag;
                    File directory = getContext().getFilesDir();
                    File file = new File(directory,selection);
                    if (file.exists()) {
                        flag = file.delete();
                        if(flag==true) {
                            Log.i(TAG, "File" + "deleted");
                        }
                    }
                } else if ((pred.compareTo(myporthash) > 0) && ((keyhash.compareTo(myporthash) <= 0) || (keyhash.compareTo(pred) > 0))) {
                    boolean flag;
                    File directory = getContext().getFilesDir();
                    File file = new File(directory,selection);
                    if (file.exists()) {
                        flag = file.delete();
                        if(flag==true) {
                            Log.i(TAG, "File deleted for:" + selection + "in port:"+myPort);
                        }
                    }

                    String msg = selection+":key delete";
                    String[] send = new String[2];
                    for (int i = 0; i < ports.length; i++) {
                        if (ports[i].equals(myporthash)) {
                            if (i == 3) {
                                send[0] = fileMap1.get(ports[4]);
                                send[1] = fileMap1.get(ports[0]);

                            } else if (i == 4) {
                                send[0] = fileMap1.get(ports[0]);
                                send[1] = fileMap1.get(ports[1]);

                            } else {
                                send[0] = fileMap1.get(ports[i + 1]);
                                send[1] = fileMap1.get(ports[i + 2]);
                            }
                        }
                    }

                    Log.i("Delete","delete forward to:"+send[0]+","+send[1]+"from port:"+myPort);

                    for(int i=0;i<send.length;i++) {
                        Integer port = Integer.parseInt(send[i]) * 2;
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                port);
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true); // Output Stream that client uses to send messages
                        Log.i("Delete", "Delete message: " + msg + "to:"+port);
                        Thread.sleep(20);
                        out.println(msg); //Sends the message
                        Log.i(TAG, "Message sent " + msg);
                        out.flush();
                        socket.close();
                    }


                } else if (!(pred.compareTo(myporthash) > 0) && ((keyhash.compareTo(myporthash) <= 0) && (keyhash.compareTo(pred) > 0))) {
                    boolean flag;
                    File directory = getContext().getFilesDir();
                    File file = new File(directory,selection);
                    if (file.exists()) {
                        flag = file.delete();
                        if(flag==true) {
                            Log.i(TAG, "File deleted for:" + selection + "in port:"+myPort);
                        }
                    }

                    String msg = selection+":key delete";
                    String[] send = new String[2];
                    for (int i = 0; i < ports.length; i++) {
                        if (ports[i].equals(myporthash)) {
                            if (i == 3) {
                                send[0] = fileMap1.get(ports[4]);
                                send[1] = fileMap1.get(ports[0]);

                            } else if (i == 4) {
                                send[0] = fileMap1.get(ports[0]);
                                send[1] = fileMap1.get(ports[1]);

                            } else {
                                send[0] = fileMap1.get(ports[i + 1]);
                                send[1] = fileMap1.get(ports[i + 2]);
                            }
                        }
                    }

                    Log.i("Delete","delete forward to:"+send[0]+","+send[1]+"from port:"+myPort);
                    for(int i=0;i<send.length;i++) {
                        Integer port = Integer.parseInt(send[i]) * 2;
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                port);
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true); // Output Stream that client uses to send messages
                        Log.i("Delete", "Delete message: " + msg + "to:"+port);
                        Thread.sleep(20);
                        out.println(msg); //Sends the message
                        Log.i("Delete", "Message sent " + msg);
                        out.flush();
                        socket.close();
                    }

                }
                else{
                    int size = ports.length;
                    String diff = selection + ":" + "key delete";
                    String test="";
                    String[] send=new String[3];

                    for(int i=0;i<ports.length;i++){
                        if((i!=(size-1) && ((keyhash.compareTo(ports[i])>0) && (keyhash.compareTo(ports[i+1])<=0)))){
                            send[0] = fileMap1.get(ports[i+1]);
                            test=ports[i+1];

                        }
                        else if((keyhash.compareTo(ports[size-1])>0) || (keyhash.compareTo(ports[0])<=0)){
                            send[0] =fileMap1.get(ports[0]);
                            test=ports[0];
                        }
                    }
                    for (int i = 0; i < ports.length; i++) {
                        if (ports[i].equals(test)) {
                            if (i == 3) {
                                send[1] = fileMap1.get(ports[4]);
                                send[2] = fileMap1.get(ports[0]);

                            } else if (i == 4) {
                                send[1] = fileMap1.get(ports[0]);
                                send[2] = fileMap1.get(ports[1]);

                            } else {
                                send[1] = fileMap1.get(ports[i + 1]);
                                send[2] = fileMap1.get(ports[i + 2]);
                            }
                        }
                    }
                    Log.i("Delete","delete forward to:"+send[0]+","+send[1]+","+send[2]+"from port:"+myPort);

                    for(int i=0;i<send.length;i++) {
                        Integer portTosend = Integer.parseInt(send[i]) * 2;
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                portTosend);
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true); // Output Stream that client uses to send messages
                        Log.i("Delete", "Delete message: " + diff + "to:"+portTosend);
                        Thread.sleep(20);
                        out.println(diff); //Sends the message
                        Log.i(TAG, "Message sent " + diff);
                        out.flush();
                        socket.close();
                    }

                }

                }


        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
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
        // TODO Auto-generated method stub
        // Files are used to store the <key,value> pairs

        String filename = values.get(KEY_FIELD).toString();
        String value = values.get(VALUE_FIELD).toString();
        Log.i("Query", "ports - " + Arrays.toString(ports));
        Log.i(TAG, "key received in insert methond: " + filename);
        Log.i(TAG, "value received in insert methond : " + value);
        Log.i(TAG, "Pred for " + myPort + " : " + pred);
        Log.i(TAG, "Succ for " + myPort + " : "  + succ);
        Log.i(TAG, "Portlist " + Arrays.toString(ports));


        Integer avd = Integer.parseInt(myPort);
        avd = avd / 2;
        int size = ports.length;
        FileOutputStream outputStream;
        try {

            String myporthash = genHash(avd.toString());
            String headnode = ports[0];
            String keyhash = genHash(filename);
            // Checking if there is only a single node in the list

            if(succ.equals("") && pred.equals("")){
                outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.flush();
                outputStream.close();
                Log.d("replica", "Inserted in my avd - " + filename + " - " + value);
                //Log.i("Inserted SinNode  ", "filename : " + filename + " value :" + values.get(VALUE_FIELD).toString());
                Log.i(TAG,"Key inserted");


            }
            // Checking if the current node is the first node in the list i.e header node
            if((pred.compareTo(myporthash)>0) && ((keyhash.compareTo(myporthash)<=0) || (keyhash.compareTo(pred)>0))){
                // Checking if key lies behind the current node or after the pred
                    String[] insert = new String[3];
                    String msg = filename + ";" + value + ":" + "insert";
                    Integer portTosend =0;
                    insert[0] = myporthash;
                    insert[1] = succ;
                    for(int i=0;i<ports.length;i++){
                        if(ports[i].equals(succ)){
                            if(i==4){
                                insert[2] = ports[0];
                            }
                            else{
                                insert[2] = ports[i+1];
                            }
                        }
                    }

                for(int i=0;i<insert.length;i++) {
                    String temp =fileMap1.get(insert[i]);
                    portTosend = Integer.parseInt(temp)*2;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            portTosend);
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true); // Output Stream that client uses to send messages
                    Log.i(TAG, "Message to send : " + msg + " sent to " + portTosend + "insert in current");
                    Thread.sleep(20);
                    out.println(msg); //Sends the message
                    Log.i(TAG, "Message sent " + msg);
                    out.flush();
                }
                }
                else if(!(pred.compareTo(myporthash)>0) && ((keyhash.compareTo(myporthash)<=0) && (keyhash.compareTo(pred)>0) )){
                String[] insert = new String[3];
                String msg = filename + ";" + value + ":" + "insert";
                Integer portTosend =0;
                insert[0] = myporthash;
                insert[1] = succ;
                for(int i=0;i<ports.length;i++){
                    if(ports[i].equals(succ)){
                        if(i==4){
                            insert[2] = ports[0];
                        }
                        else{
                            insert[2] = ports[i+1];
                        }
                    }
                }

                for(int i=0;i<insert.length;i++) {
                    String temp =fileMap1.get(insert[i]);
                    portTosend = Integer.parseInt(temp)*2;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            portTosend);
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true); // Output Stream that client uses to send messages
                    Log.i(TAG, "Message to send : " + msg + " sent to " + portTosend + "insert in current");
                    Thread.sleep(20);
                    out.println(msg); //Sends the message
                    Log.i(TAG, "Message sent " + msg);
                    out.flush();
                }
                }
                else{
                    Log.i("replica", "checking bucketing, filename : " + filename + " value :" + values.get(VALUE_FIELD).toString());
                    String msg = filename + ";" + value + ":" + "insert";
                    Integer portTosend =0;
                    String[] insert = new String[3];

                    for(int i=0;i<ports.length;i++){
                        if((i!=(size-1) && ((keyhash.compareTo(ports[i])>0) && (keyhash.compareTo(ports[i+1])<=0)))){
                            if(i==2) {
                                insert[0] = ports[i + 1];
                                insert[1] = ports[i + 2];
                                insert[2] = ports[0];
                            }
                            else if(i==3){
                                    insert[0] = ports[i+1];
                                    insert[1] = ports[0];
                                    insert[2] = ports[1];
                                }
                                else{
                                insert[0] = ports[i+1];
                                insert[1] = ports[i+2];
                                insert[2] = ports[i+3];
                            }
                        }
                        else if((keyhash.compareTo(ports[size-1])>0) || (keyhash.compareTo(ports[0])<=0)){
                            insert[0] = ports[0];
                            insert[1] = ports[1];
                            insert[2] = ports[2];

                        }
                    }

                    for(int i=0;i<insert.length;i++) {
                        Log.i("Insert","insert block port value to send " + insert[i] + "for i=" + i);
                        String temp =fileMap1.get(insert[i]);
                        Log.i("Insert","insert block avd value to send " + temp+ "for i=" + i);
                        portTosend = Integer.parseInt(temp)*2;
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                portTosend);
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true); // Output Stream that client uses to send messages
                        Log.i(TAG, "Message to send : " + msg + " sent to " + portTosend + " buketting");
                        Thread.sleep(20);
                        out.println(msg); //Sends the message
                        Log.i(TAG, "Message sent " + msg);
                        out.flush();
                    }
                }
        }
        catch (NullPointerException e) {
            Log.e(TAG, "File write failed (insert) : " + e.getMessage());
            e.printStackTrace();
        }
        catch (Exception e) {
            Log.e(TAG, "File write failed (insert) : " + e.getMessage());
            e.printStackTrace();
        }
        Log.v("insert", values.toString());
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        ArrayList<String> flag = new ArrayList<String>();

        Log.i(TAG, "Inside Oncreate");
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }

        flag.add("5562");
        flag.add("5556");
        flag.add("5554");
        flag.add("5558");
        flag.add("5560");

        Log.i(TAG, "flag list : " + flag.toString());
        for(int i=0;i<flag.size();i++){
            try {
                ports[i] = genHash(flag.get(i));
                fileMap1.put(genHash(flag.get(i)),flag.get(i));
                unhashed[i] = flag.get(i);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }

        int a = ports.length;
        Log.i(TAG, "Portlist size: " + a);
        Log.i(TAG, "Portlist : " + Arrays.toString(ports));
        Log.i(TAG, "Portlist unhashed : " + Arrays.toString(unhashed));
        String hash = "";
        try {
            hash = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < a; i++) {
            if ((ports[i]).equals(hash)) {
                Log.i(TAG, "Server side : computing succ and pred");
                if (a == 1) {
                    pred = "";
                    succ = "";
                } else if (i == 0) {
                    pred = ports[a - 1];
                    pred_unhash = unhashed[a - 1];
                    succ = ports[i + 1];
                    succ_unhash = unhashed[i + 1];
                } else if (i == (a - 1)) {
                    //check = check + "," + "Pred:" + ports[i - 1] + "," + "Succ:" + ports[0];
                    pred = ports[i - 1];
                    pred_unhash = unhashed[i - 1];
                    succ = ports[0];
                    succ_unhash = unhashed[0];
                    Log.i(TAG, "inside 4");
                } else {
                    //check = check + "," + "Pred:" + ports[i - 1] + "," + "Succ:" + ports[i + 1];
                    pred = ports[i - 1];
                    pred_unhash = unhashed[i - 1];
                    succ = ports[i + 1];
                    succ_unhash = unhashed[i + 1];
                    Log.i(TAG, "inside 5");
                }
            }
        }
        Log.i(TAG, "Successor for " + portStr + ":" + succ);
        Log.i(TAG, "Pred for " + portStr + ":" + pred);
        Log.i(TAG, "Successor unhahsed for " + portStr + ":" + succ_unhash);
        Log.i(TAG, "Pred unhashed for " + portStr + ":" + pred_unhash);

        String send="recover";

            for(int i=0;i<ports.length;i++) {
                if(hash.equals(ports[i])) {
                    AsyncTask<String, Void, String> rec = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, portStr, send);
                    try {
                        Log.i("oncreate", "Oncreate : Recover " + rec.get());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            }

        return false;
    }

    //http://developer.android.com/reference/android/database/MatrixCursor.html
    // Reference:
    // 1. https://www.androidinterview.com/android-internal-storage-read-and-write-text-file-example/
    // 2. https://docs.oracle.com/javase/8/docs/api/?java/io/FileInputStream.html
    // Reads string from the file and stores the data as rows into the Matrixcursor
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        ArrayList<String> matrixdata = new ArrayList<String>();
        Integer avdno = Integer.parseInt(myPort)/2;

        if(selection.equals("@")) {
            String[] filelist = getContext().fileList();
            //File directory = getContext().getFilesDir();t
            MatrixCursor matrixCursor = new MatrixCursor( new String[]{"key", "value"});
            try {
                for(int i=0;i<filelist.length;i++) {
                    //File file = new File(directory, filelist[i]);
                    FileInputStream filestream = getContext().openFileInput(filelist[i]);
                    BufferedReader buf = new BufferedReader(new InputStreamReader(filestream));
                    String value = buf.readLine();
                    if(value!=null) {
                        String[] rowdata = new String[]{filelist[i], value};
                        Log.i(TAG, "query key we got : " + filelist[i]);
                        Log.i(TAG, "query- value we got : " + value);
                        matrixCursor.addRow(rowdata);
                        buf.close();
                        Log.i(TAG, "Query @: ");
                    }
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

            Log.v("query", selection);
            return matrixCursor;
        }

        else if(selection.equals("*")) {
            if( (succ.equals("") && pred.equals(""))){
                String msg = "query all";
                String sendmdata;
                String[] filelist = getContext().fileList();
                //File directory = getContext().getFilesDir();
                MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
                try {
                    for (int i = 0; i < filelist.length; i++) {
                        //File file = new File(directory, filelist[i]);
                        FileInputStream filestream = getContext().openFileInput(filelist[i]);
                        BufferedReader buf = new BufferedReader(new InputStreamReader(filestream));
                        String value = buf.readLine();
                        if (value != null) {
                            String[] rowdata = new String[]{filelist[i], value};
                            Log.i("Recover", "Query *:query key we got : " + filelist[i]);
                            Log.i(TAG, "Recover" + value);
                            matrixCursor.addRow(rowdata);
                            String temp = filelist[i] + "=" + value;
                            matrixdata.add(temp);
                            buf.close();
                        }
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                Log.v("query", selection);
                return matrixCursor;
                //return  null;
            }
            else{
                Log.i("Query","Inside query *");
                String sendmdata="";
                String total="";
                String[] filelist = getContext().fileList();
                Integer size = filelist.length;
                Log.i("Query","Inside query * printing flist length" + size.toString());
                //File directory = getContext().getFilesDir();
                MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
                try {
                    String myporthash = genHash(avdno.toString());
                    for (int i = 0; i < filelist.length; i++) {
                        //File file = new File(directory, filelist[i]);
                        FileInputStream filestream = getContext().openFileInput(filelist[i]);
                        BufferedReader buf = new BufferedReader(new InputStreamReader(filestream));
                        String value = buf.readLine();
                        if (value != null) {
                            //String[] rowdata = new String[]{filelist[i], value};
                            Log.i("Recover", "Query *:query key we got : " + filelist[i]);
                            Log.i("Recover", "query- value we got : " + value);
                            //matrixCursor.addRow(rowdata);
                            String temp = filelist[i] + "=" + value;
                            matrixdata.add(temp);
                            buf.close();
                        }
                    }
                    //sendmdata = matrixdata.get(0);
                    for(int i=0;i<matrixdata.size();i++){
                        sendmdata = sendmdata + matrixdata.get(i) + "," ;
                    }
                    Log.i(TAG, "Appended list : " + sendmdata);
                    Log.i(TAG, "Alive nodes list: " + ports.length);
                    for(int i=0;i<ports.length;i++){
                        String temp = fileMap1.get(ports[i]);
                        if(!myporthash.equals(ports[i])){
                            String msg = "Important:query all";
                            Integer port = Integer.parseInt(temp)*2;
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    port);
                            PrintWriter out = new PrintWriter(socket.getOutputStream(), true); // Output Stream that client uses to send messages
                            Log.i(TAG, "Message to send in the for : " + msg);
                            Thread.sleep(20);
                            out.println(msg); //Sends the message
                            Log.i(TAG, "Message sent " + msg);
                            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String ack = in.readLine();
                            if(ack!=null) {
                                total = total + ack;
                            }
                            Log.i("Query", "list from other avds : " + total);
                            in.close();
                            out.flush();
                            socket.close();

                        }
                    }
                    sendmdata = sendmdata + total;
                    Log.i("Query", "FInal data " + sendmdata);
                    String[] pairs = sendmdata.split(",");
                    for (int i=0;i<pairs.length;i++){
                        String[] keyvalue = pairs[i].split("=");
                        String[] rowdata = new String[]{keyvalue[0], keyvalue[1]};
                        matrixCursor.addRow(rowdata);
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                Log.v("query", selection);
                return matrixCursor;
            }
        }
        else {
            Log.i("Query","Query for key");
            int size = ports.length;
            Integer avd = Integer.parseInt(myPort);
            avd = avd / 2;
            //File directory = getContext().getFilesDir();
            MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
            try {
                String keyhash = genHash(selection);
                String myporthash = genHash(avd.toString());
                if (succ.equals("") && pred.equals("")) {
                    Log.i("Query","Query for key : when single node in list");
                    FileInputStream filestream = getContext().openFileInput(selection);
                    BufferedReader buf = new BufferedReader(new InputStreamReader(filestream));
                    String value = buf.readLine();
                    if (value != null) {
                        String[] rowdata = new String[]{selection, value};
                        Log.i(TAG, "query key we got : " + selection);
                        Log.i(TAG, "query- value we got : " + value);
                        matrixCursor.addRow(rowdata);
                        buf.close();
                    }
                }
                else if((pred.compareTo(myporthash)>0) && ((keyhash.compareTo(myporthash)<=0) || (keyhash.compareTo(pred)>0))){
                    // Checking if key lies behind the current node or after the pred
                    //File file = new File(directory, filelist[i]);
                    Log.i("Query","Query for key : when current node is head");
                    FileInputStream filestream = getContext().openFileInput(selection);
                    BufferedReader buf = new BufferedReader(new InputStreamReader(filestream));
                    String value = buf.readLine();
                    if (value != null) {
                        String[] rowdata = new String[]{selection, value};
                        Log.i("Recover", "Query Key:query key we got : " + selection);
                        Log.i(TAG, "query- value we got : " + value);
                        matrixCursor.addRow(rowdata);
                        //String temp = selection + "=" + value;
                        buf.close();
                    }
                }
                else if(!(pred.compareTo(myporthash)>0) && ((keyhash.compareTo(myporthash)<=0) && (keyhash.compareTo(pred)>0) )){
                    Log.i("Query","Query for key : when current is not head");
                    FileInputStream filestream = getContext().openFileInput(selection);
                    BufferedReader buf = new BufferedReader(new InputStreamReader(filestream));
                    String value = buf.readLine();
                    if (value != null) {
                        String[] rowdata = new String[]{selection, value};
                        Log.i(TAG, "query key we got : " + selection);
                        Log.i(TAG, "query- value we got : " + value);
                        matrixCursor.addRow(rowdata);
                        //String temp = selection + "=" + value;
                        buf.close();
                    }
                }
                else {
                    Log.i("Query", "Query for key : finding the avd to send");
                    String diff = selection + ":" + "query";
                    Integer portTosend = 0;
                    Integer port_1 = 0;
                    Integer port_2 = 0;
                    String ack = "";
                    String ack1 = "";
                    String ack2 = "";
                    String test="";
                    String[] send = new String[2];

                    for (int i = 0; i < ports.length; i++) {
                        if ((i != (size - 1) && ((keyhash.compareTo(ports[i]) > 0) && (keyhash.compareTo(ports[i + 1]) <= 0)))) {
                            String temp = fileMap1.get(ports[i + 1]);
                            test = ports[i+1];
                            portTosend = Integer.parseInt(temp) * 2;
                        } else if ((keyhash.compareTo(ports[size - 1]) > 0) || (keyhash.compareTo(ports[0]) <= 0)) {
                            String temp = fileMap1.get(ports[0]);
                            test=ports[0];
                            portTosend = Integer.parseInt(temp) * 2;
                        }
                    }

                    for (int i = 0; i < ports.length; i++) {
                        if (ports[i].equals(test)) {
                            if (i == 3) {
                                send[0] = fileMap1.get(ports[4]);
                                send[1] = fileMap1.get(ports[0]);
                                port_1 = Integer.parseInt(send[0]) * 2;
                                port_2 = Integer.parseInt(send[1]) * 2;
                            } else if (i == 4) {
                                send[0] = fileMap1.get(ports[0]);
                                send[1] = fileMap1.get(ports[1]);
                                port_1 = Integer.parseInt(send[0]) * 2;
                                port_2 = Integer.parseInt(send[1]) * 2;
                            } else {
                                send[0] = fileMap1.get(ports[i + 1]);
                                send[1] = fileMap1.get(ports[i + 2]);
                                port_1 = Integer.parseInt(send[0]) * 2;
                                port_2 = Integer.parseInt(send[1]) * 2;
                            }
                        }
                    }

                    Log.i("Recover","Querying:"+selection+ "ports to search:"+portTosend+","+send[0]+","+send[1]);

                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                portTosend);
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true); // Output Stream that client uses to send messages
                        Thread.sleep(20);
                        Log.i(TAG, "Query loop: Message to send (0) : " + diff + " sent to " + portTosend);
                        out.println(diff); //Sends the message
                        Log.i(TAG, "Query loop: Message sent " + diff);
                        Log.i("Recover","Before in");
                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        ack = in.readLine();
                        Log.i("Recover","(0) Ack for "+ selection+ " is:" +ack+ "from port:"+portTosend);
                        if (ack == null) {
                            Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    port_1);
                            PrintWriter out1 = new PrintWriter(socket1.getOutputStream(), true); // Output Stream that client uses to send messages
                            Log.i(TAG, "Query loop: Message to send (1): " + diff + " sent to " + port_1);
                            out1.println(diff); //Sends the message
                            Log.i(TAG, "Query loop: Message sent " + diff);
                            out1.flush();
                            BufferedReader in1 = new BufferedReader(new InputStreamReader(socket1.getInputStream()));
                            ack1 = in1.readLine();
                            Log.i("Recover","(1) Ack for "+ selection+ " is:" +ack1+ "from port:"+port_1);
                            if (ack1 == null) {
                                Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        port_2);
                                PrintWriter out2 = new PrintWriter(socket2.getOutputStream(), true); // Output Stream that client uses to send messages
                                Log.i(TAG, "Query loop: Message to send (2) : " + diff + " sent to " + port_2);
                                out2.println(diff); //Sends the message
                                Log.i(TAG, "Query loop: Message sent " + diff);
                                out2.flush();
                                BufferedReader in2 = new BufferedReader(new InputStreamReader(socket2.getInputStream()));
                                ack2 = in2.readLine();
                                Log.i("Recover","(2) Ack for "+ selection+ " is:" +ack2 + "from port:"+port_2);
                                String[] rowdata = new String[]{selection, ack2};
                                matrixCursor.addRow(rowdata);
                                in2.close();
                                socket2.close();
                            }
                            else {
                                String[] rowdata = new String[]{selection, ack1};
                                Log.i("Recover", "Inserted into matrix:" + ack1);
                                matrixCursor.addRow(rowdata);
                                in1.close();
                                socket1.close();
                            }

                        }
                        else {
                            String[] rowdata = new String[]{selection, ack};
                            matrixCursor.addRow(rowdata);
                        }

                        Log.i("Recover", "value got " + ack + "in " + myPort);
                        in.close();
                        out.flush();
                        socket.close();


                    }catch (Exception e){
                        Log.e("Recover","Query error " + e.getMessage());
                    }
                }

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (Exception e){
                Log.e("Recover","Error msg " + e.getMessage());
            }

            Log.v("query", selection);
            return matrixCursor;
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
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

    // Reference: https://docs.oracle.com/javase/tutorial/networking/sockets/definition.html
    private class ClientTask extends AsyncTask<String, Void, String> {

        @Override
        protected String doInBackground(String... msgs) {
            try {
                 if(msgs[1].trim().equals("recover")){
                    try {
                        String total = "";
                        Integer size = ports.length;
                        String[] tosend = new String[2];
                        String[] send = new String[2];
                        FileOutputStream outputStream;
                        String msg = myPort + ":recover mine";
                        String msg1 = "";
                        String myporthash = genHash(msgs[0]);

                        for (int i = 0; i < ports.length; i++) {
                            if (ports[i].equals(myporthash)) {
                                if (i == 3) {
                                    send[0] = fileMap1.get(ports[4]);
                                    send[1] = fileMap1.get(ports[0]);
                                } else if (i == 4) {
                                    send[0] = fileMap1.get(ports[0]);
                                    send[1] = fileMap1.get(ports[1]);
                                } else {
                                    send[0] = fileMap1.get(ports[i + 1]);
                                    send[1] = fileMap1.get(ports[i + 2]);
                                }
                            }
                        }

                        for(int j=0;j<send.length;j++) {
                            Integer port = Integer.parseInt(send[j]) * 2;
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    port);
                            PrintWriter out = new PrintWriter(socket.getOutputStream(), true); // Output Stream that client uses to send messages
                            out.println(msg);
                            out.flush();

                            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String ack = in.readLine();
                            if (ack != null) {
                                String[] pairs = ack.split(",");
                                for (int i = 0; i < pairs.length; i++) {
                                    String[] keyvalue = pairs[i].split("=");
                                    String keyhash = genHash(keyvalue[0]);
                                    if ((pred.compareTo(myporthash) > 0) && ((keyhash.compareTo(myporthash) <= 0) || (keyhash.compareTo(pred) > 0))) {
                                        outputStream = getContext().openFileOutput(keyvalue[0], Context.MODE_PRIVATE);
                                        outputStream.write(keyvalue[1].getBytes());
                                        outputStream.flush();
                                        outputStream.close();
                                    } else if (!(pred.compareTo(myporthash) > 0) && ((keyhash.compareTo(myporthash) <= 0) && (keyhash.compareTo(pred) > 0))) {
                                        outputStream = getContext().openFileOutput(keyvalue[0], Context.MODE_PRIVATE);
                                        outputStream.write(keyvalue[1].getBytes());
                                        outputStream.flush();
                                        outputStream.close();
                                    }
                                }
                            }
                        }
                                for (int i = 0; i < ports.length; i++) {
                                    if (ports[i].equals(myporthash)) {
                                        if (i == 0) {
                                            tosend[0] = fileMap1.get(ports[size - 1]);
                                            tosend[1] = fileMap1.get(ports[size - 2]);
                                        } else if (i == 1) {
                                            tosend[0] = fileMap1.get(ports[0]);
                                            tosend[1] = fileMap1.get(ports[size - 1]);
                                        } else {
                                            tosend[0] = fileMap1.get(ports[i - 1]);
                                            tosend[1] = fileMap1.get(ports[i - 2]);
                                        }
                                    }
                                }

                                for (int i = 0; i < tosend.length; i++) {
                                    msg1 = tosend[i] + ":" + "recover dups";
                                    Integer pport = Integer.parseInt(tosend[i]) * 2;
                                    Socket ssocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            pport);
                                    PrintWriter pout = new PrintWriter(ssocket.getOutputStream(), true); // Output Stream that client uses to send messages
                                    pout.println(msg1);

                                    BufferedReader iin = new BufferedReader(new InputStreamReader(ssocket.getInputStream()));
                                    String sack = iin.readLine();
                                    if (sack != null) {
                                        total = total + sack;
                                    }
                                }
                                Log.i(TAG, "Printing:" + total);
                                String[] pair = total.split(",");
                                if(pair.length!=0){
                                    for (int i = 0; i < pair.length; i++) {
                                        String[] keyvalue = pair[i].split("=");
                                        outputStream = getContext().openFileOutput(keyvalue[0], Context.MODE_PRIVATE);
                                        outputStream.write(keyvalue[1].getBytes());
                                        outputStream.flush();
                                        outputStream.close();
                                    }
                                }

                    }
                    catch(NullPointerException e){
                        Log.e(TAG, "initial node entry");
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                    catch (Exception e){
                        Log.e(TAG, "Client : recover loop:" + e.getMessage());
                    }
                    String don= "yes";
                    return don;

                }
            }catch (Exception e) {
                Log.e(TAG, "ClientTask UnknownHostException" + e.getMessage());
            }
            return null;
        }

    }

    //Publishes updates on the UI. Invokes onProgressUpdate() every time publishProgress() is called.
    // Reference: https://developer.android.com/reference/android/os/AsyncTask
    // Reference: https://docs.oracle.com/javase/tutorial/networking/sockets/definition.html
    //Server sends an ACK message after successfully reading the client message
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept(); // Accepts the client's connection request
                    Log.i(TAG, "Server accepted connection");
                    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream())); // Input stream that helps server read data from socket
                    String data = in.readLine();
                    Log.i(TAG, "Server side : Reading data " + data);
                    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                    if ((data) != null) { // Receive data when not null
                        Log.i(TAG, data + " message received");
                        String[] sp = data.split(":");
                           if (sp[1].equals("delete all")) {
                            boolean flag;
                            String[] filelist = getContext().fileList();
                            File directory = getContext().getFilesDir();
                            for (int i = 0; i < filelist.length; i++) {
                                File file = new File(directory, filelist[i]);
                                if (file.exists()) {
                                    flag = file.delete();
                                    if (flag == true) {
                                        Log.i(TAG, "File" + i + "deleted");
                                    }
                                }
                            }
                        } else if (sp[1].equals("insert")) {
                            Log.i(TAG, "Server side : Insert");
                            String[] cvalues = sp[0].split(";");
                            FileOutputStream outputStream;
                            outputStream = getContext().openFileOutput(cvalues[0], Context.MODE_PRIVATE);
                            outputStream.write(cvalues[1].getBytes());
                            outputStream.flush();
                            outputStream.close();
                            Log.i(TAG,"Inserted in my avd:" + sp[0]);
                        } else if (sp[1].equals("query all")) {
                            ArrayList<String> matrixdata = new ArrayList<String>();
                            String sendmdata="";
                            String[] filelist = getContext().fileList();
                            try {
                                for (int i = 0; i < filelist.length; i++) {
                                    //File file = new File(directory, filelist[i]);
                                    FileInputStream filestream = getContext().openFileInput(filelist[i]);
                                    BufferedReader buf = new BufferedReader(new InputStreamReader(filestream));
                                    String value = buf.readLine();
                                    if (value != null) {
                                        Log.i(TAG, "Server side:query key we got : " + filelist[i]);
                                        Log.i(TAG, "Server side:query- value we got : " + value);
                                        String temp = filelist[i] + "=" + value;
                                        matrixdata.add(temp);
                                        buf.close();
                                    }
                                }
                                //sendmdata = matrixdata.get(0);
                                for(int i=0;i<matrixdata.size();i++){
                                    sendmdata = sendmdata + matrixdata.get(i) + ",";
                                }
                                out.println(sendmdata);
                            }catch (Exception e){
                                Log.e(TAG, e.toString());
                            }
                        }
                        else if(sp[1].equals("query")) {
                            Log.i("Recover","Server Side Query" + sp[0]);
                            FileInputStream filestream = getContext().openFileInput(sp[0]);
                            BufferedReader buf = new BufferedReader(new InputStreamReader(filestream));
                            String value = buf.readLine();
                            if (value != null) {
                                Log.i("Recover"," Server Side:the value for " + sp[0]  + " is " + value);
                                out.println(value);
                                buf.close();
                            }
                            Log.i("recover","Queried:" + sp[0]);
                        }
                        else if(sp[1].equals("key delete")){
                            boolean flag;
                            Log.i("Delete","Server Side: Deleting "+sp[0]);
                            File directory = getContext().getFilesDir();
                            File file = new File(directory,sp[0]);
                            if (file.exists()) {
                                flag = file.delete();
                                if(flag==true) {
                                    Log.i(TAG, "File deleted:" + sp[0]);
                                }
                            }
                        }
                        else if(sp[1].contains("recover mine")){
                            ArrayList<String> matrixdata = new ArrayList<String>();
                            String sendmdata="";
                            String[] filelist = getContext().fileList();
                            try {
                                for (int i = 0; i < filelist.length; i++) {
                                    FileInputStream filestream = getContext().openFileInput(filelist[i]);
                                    BufferedReader buf = new BufferedReader(new InputStreamReader(filestream));
                                    String value = buf.readLine();
                                    if (value != null) {
                                        Log.i(TAG, "query key we got : " + filelist[i]);
                                        Log.i(TAG, "query- value we got : " + value);
                                        String temp = filelist[i] + "=" + value;
                                        matrixdata.add(temp);
                                        buf.close();
                                    }
                                }
                                for(int i=0;i<matrixdata.size();i++){
                                    sendmdata = sendmdata + matrixdata.get(i) + ",";
                                }
                                out.println(sendmdata);
                            }catch (Exception e){
                                Log.e(TAG, e.toString());
                            }
                        }
                        else if(sp[1].contains("recover dups")){
                            ArrayList<String> matrixdata = new ArrayList<String>();
                            String sendmdata="";
                            String[] filelist = getContext().fileList();
                            try {
                                String myporthash = genHash(sp[0]);
                                for (int i = 0; i < filelist.length; i++) {
                                    String keyhash=genHash(filelist[i]);
                                    if((pred.compareTo(myporthash)>0) && ((keyhash.compareTo(myporthash)<=0) || (keyhash.compareTo(pred)>0))){
                                        FileInputStream filestream = getContext().openFileInput(filelist[i]);
                                        BufferedReader buf = new BufferedReader(new InputStreamReader(filestream));
                                        String value = buf.readLine();
                                        if (value != null) {
                                            String temp = filelist[i] + "=" + value;
                                            matrixdata.add(temp);
                                            buf.close();
                                        }
                                    }
                                    else if(!(pred.compareTo(myporthash)>0) && ((keyhash.compareTo(myporthash)<=0) && (keyhash.compareTo(pred)>0) )){
                                        FileInputStream filestream = getContext().openFileInput(filelist[i]);
                                        BufferedReader buf = new BufferedReader(new InputStreamReader(filestream));
                                        String value = buf.readLine();
                                        if (value != null) {
                                            String temp = filelist[i] + "=" + value;
                                            matrixdata.add(temp);
                                            buf.close();
                                        }
                                    }
                                }
                                for(int i=0;i<matrixdata.size();i++){
                                    sendmdata = sendmdata + matrixdata.get(i) + ",";
                                }
                                out.println(sendmdata);
                            }
                            catch(Exception e){
                                Log.e(TAG, e.toString());
                            }
                        }

                    }
                 clientSocket.close();
                } catch (IOException e) {
                    Log.e(TAG, e.toString());
                }
            }
        }

    }
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
}