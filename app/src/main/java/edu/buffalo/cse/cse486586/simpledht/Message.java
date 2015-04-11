package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by Jason on 3/25/15.
 */
public class Message implements Serializable{

    String sendPort;
    String recPort;
    String destination;
    String message;
    String succ;
    String pred;
    String key;
    String val;
    String minNode;
    String maxNode;
    String queryResponse;
    HashMap<String,String> responses = new HashMap<>();

    public void addResponse(String key, String value){
        responses.put(key,value);
    }

    public Message(){

    }
}
