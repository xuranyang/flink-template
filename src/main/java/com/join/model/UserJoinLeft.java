package com.join.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserJoinLeft {
    private String club;
    private String userId;
    private Long timestamp;
}
