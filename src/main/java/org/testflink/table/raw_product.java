package org.testflink.table;

public class raw_product {
    private String product_code ;
    private String product_name;
    private int category_id;
    private double original_price;

    public String getProduct_code() {
        return product_code;
    }

    public void setProduct_code(String product_code) {
        this.product_code = product_code;
    }

    public String getProduct_name() {
        return product_name;
    }

    public void setProduct_name(String product_name) {
        this.product_name = product_name;
    }

    public int getCategory_id() {
        return category_id;
    }

    public void setCategory_id(int category_id) {
        this.category_id = category_id;
    }

    public double getOriginal_price() {
        return original_price;
    }

    public void setOriginal_price(double original_price) {
        this.original_price = original_price;
    }

    public raw_product() {
    }

    public raw_product(String product_code, String product_name, int category_id, double original_price) {
        this.product_code = product_code;
        this.product_name = product_name;
        this.category_id = category_id;
        this.original_price = original_price;
    }
}
