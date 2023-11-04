package org.testflink.debezium;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown=true)
public class DebeziumSchema implements Serializable {
    private static final long serialVersionUID = 1L;
    //private Long scn;

    @JsonProperty("before")
    private HashMap<String,Object> before;

    @JsonProperty("after")
    private HashMap<String,Object> after;

    @JsonProperty("ts_ms")
    private String ts_ms;

    @JsonProperty("transaction")
    private String transaction;

    @JsonProperty("source")
    private HashMap<String,Object> source;
    @JsonProperty("op")
    private String op;

    public DebeziumSchema(HashMap<String, Object> before, HashMap<String, Object> after, HashMap<String, Object> source, String op, String ts_ms, String transaction) {
        this.before = before;
        this.after = after;
        this.source = source;
        this.op = op;
        this.ts_ms = ts_ms;
        this.transaction = transaction;
    }

    public HashMap<String, Object> getBefore() {
        return before;
    }

    public void setBefore(HashMap<String, Object> before) {
        this.before = before;
    }

    public HashMap<String, Object> getAfter() {
        return after;
    }

    public void setAfter(HashMap<String, Object> after) {
        this.after = after;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public String getTs_ms() {
        return ts_ms;
    }

    public void setTs_ms(String ts_ms) {
        this.ts_ms = ts_ms;
    }

    public String getTransaction() {
        return transaction;
    }

    public void setTransaction(String transaction) {
        this.transaction = transaction;
    }

    public DebeziumSchema() {
    }

    public void setSource(HashMap<String, Object> source) {
        this.source = source;
    }



}
