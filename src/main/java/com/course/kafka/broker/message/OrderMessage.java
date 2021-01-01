package com.course.kafka.broker.message;

import com.course.kafka.util.LocalDateTimeDeserializer;
import com.course.kafka.util.LocalDateTimeSerializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;


import java.time.LocalDateTime;

public class OrderMessage {

    private String creditCardNumber;
    private String itemName;

    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    private LocalDateTime orderDateTime;
    private String orderLocation;
    private String orderNumber;
    private int price;
    private int quantity;

    public OrderMessage copy22() {
        var copy33= new OrderMessage();
        copy33.setCreditCardNumber(this.creditCardNumber);
        copy33.setItemName(this.itemName);
        copy33.setOrderDateTime(this.orderDateTime);
        copy33.setOrderLocation(this.orderLocation);
        copy33.setOrderNumber(this.orderNumber);
        copy33.setPrice(this.price);
        copy33.setQuantity(this.quantity);

        return copy33;
    }


    public String getCreditCardNumber() {
        return creditCardNumber;
    }

    public String getItemName() {
        return itemName;
    }

    public LocalDateTime getOrderDateTime() {
        return orderDateTime;
    }

    public String getOrderLocation() {
        return orderLocation;
    }

    public String getOrderNumber() {
        return orderNumber;
    }

    public int getPrice() {
        return price;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setCreditCardNumber(String creditCardNumber) {
        this.creditCardNumber = creditCardNumber;
    }

    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    public void setOrderDateTime(LocalDateTime orderDateTime) {
        this.orderDateTime = orderDateTime;
    }

    public void setOrderLocation(String orderLocation) {
        this.orderLocation = orderLocation;
    }

    public void setOrderNumber(String orderNumber) {
        this.orderNumber = orderNumber;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    @Override
    public String toString() {
        return "OrderMessage [orderLocation=" + orderLocation + ", orderNumber=" + orderNumber + ", creditCardNumber="
                + creditCardNumber + ", orderDateTime=" + orderDateTime + ", itemName=" + itemName + ", price=" + price
                + ", quantity=" + quantity + "]";
    }
}
