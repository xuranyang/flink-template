package drools;

import org.drools.core.base.RuleNameEqualsAgendaFilter;
import org.drools.core.base.RuleNameMatchesAgendaFilter;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.QueryResults;
import org.kie.api.runtime.rule.QueryResultsRow;


/**
 * drools API开发步骤:
 * 1.获取KieServices
 * 2.获取KieContainer
 * 3.获取KieSession
 * 4.Insert fact
 * 5.触发规则
 * 6.关闭KieSession
 */
public class DroolsTest {
    /**
     * 规则1  所购图书总价在100元以下的没有优惠
     * 规则2  所购图书总价在100~200元的优惠20元
     * 规则3  所购图书总价在200~300元的优惠50元
     * 规则4  所购图书总价在300元以上的优惠100元
     */
    public static void main(String[] args) {
        // 获取KieServices
        KieServices kieServices = KieServices.Factory.get();
        // 获取Kie容器对象（默认容器对象
        KieContainer kieContainer = kieServices.newKieClasspathContainer();
        // 从Kie容器对象中获取会话对象（默认session对象
        KieSession kieSession = kieContainer.newKieSession();

        Order order = new Order();
        // 假设所购图书总价为160元
        order.setOriginalPrice(160d);

        // 将order对象插入工作内存
        kieSession.insert(order);

        System.out.println("匹配规则前-优惠前价格：" + order.getOriginalPrice());
        System.out.println("匹配规则前-优惠后价格：" + order.getRealPrice());

        // 匹配对象
        // 激活规则，由drools框架自动进行规则匹配。若匹配成功，则执行
        kieSession.fireAllRules();
        // 通过规则过滤器实现只执行指定规则; 只有 book_discount_4 即超过300元时,才会被触发
        // kieSession.fireAllRules(new RuleNameEqualsAgendaFilter("book_discount_4"));
        // kieSession.fireAllRules(new RuleNameMatchesAgendaFilter("book.*?")); // 通过正则匹配满足要求的规则
        // kieSession.fireAllRules(new RuleNameStartsWithAgendaFilter("book_discount")); // 匹配以指定名称开头的规则
        // fireAllRules(new RuleNameEndsWithAgendaFilter("_4")); // 匹配以指定名称结尾的规则

        // 关闭会话
        kieSession.dispose();

        System.out.println("优惠前价格：" + order.getOriginalPrice());
        System.out.println("优惠后价格：" + order.getRealPrice());

    }
}
