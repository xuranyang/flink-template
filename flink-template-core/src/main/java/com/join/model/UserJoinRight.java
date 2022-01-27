package com.join.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserJoinRight {
    private String userId;
    private String position;
    private Long timestamp;
}
