package com.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WindowPlayerInfo {
    private String club;
    private String userId;
    private String pos;
    private Integer age;
    private Long timestamp;
}
