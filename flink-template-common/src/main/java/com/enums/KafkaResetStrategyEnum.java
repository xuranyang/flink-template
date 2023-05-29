package com.enums;

import lombok.Data;

public enum KafkaResetStrategyEnum {
    LATEST("latest"),
    EARLIEST("earliest"),
    OFFSET("offset"),
    TIMESTAMP("timestamp");

    private String strategy;

    private KafkaResetStrategyEnum() {
    }

    private KafkaResetStrategyEnum(String strategy) {
        this.strategy = strategy;
    }

    public String getStrategy() {
        return strategy;
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }


}
