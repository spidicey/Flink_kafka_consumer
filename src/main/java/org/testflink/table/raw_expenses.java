package org.testflink.table;

import java.sql.Timestamp;

public class raw_expenses {
    private int expense_id;
    private String raw_store_code;
    private String expense_type;
    private Double total_amount;
    private Timestamp payment_time;

    public int getExpense_id() {
        return expense_id;
    }

    public void setExpense_id(int expense_id) {
        this.expense_id = expense_id;
    }

    public String getRaw_store_code() {
        return raw_store_code;
    }

    public void setRaw_store_code(String raw_store_code) {
        this.raw_store_code = raw_store_code;
    }

    public String getExpense_type() {
        return expense_type;
    }

    public void setExpense_type(String expense_type) {
        this.expense_type = expense_type;
    }

    public Double getTotal_amount() {
        return total_amount;
    }

    public void setTotal_amount(Double total_amount) {
        this.total_amount = total_amount;
    }

    public Timestamp getPayment_time() {
        return payment_time;
    }

    public void setPayment_time(Timestamp payment_time) {
        this.payment_time = payment_time;
    }

    public raw_expenses(int expense_id, String raw_store_code, String expense_type, Double total_amount, Timestamp payment_time) {
        this.expense_id = expense_id;
        this.raw_store_code = raw_store_code;
        this.expense_type = expense_type;
        this.total_amount = total_amount;
        this.payment_time = payment_time;
    }

    public raw_expenses() {
    }
}
