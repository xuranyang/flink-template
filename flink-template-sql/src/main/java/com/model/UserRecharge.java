package com.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UserRecharge {
    private String userId;
    private Integer rechargeType;
    private Double rechargeAmount;
    private Long rechargeTs;
}
