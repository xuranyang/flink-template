package drools;

import lombok.Data;

/**
 * 订单
 */
@Data
public class Order {
    private Double originalPrice;   // 订单原始价格，即优惠前的价格
    private Double realPrice;       // 订单真实价格，即优惠后的价格
}