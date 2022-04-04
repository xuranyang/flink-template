package com.cep;

/**
 * API	                含义	                        示例	                                            含义
 * where()	            指定匹配条件	                pattern.where(_ = 1)	                        匹配为1的数据
 * or()	                匹配条件，或者关系	            pattern.or(_ = 2)	                            匹配为2的数据
 * times()	            模式发生的次数	                pattern.times(2,4)	                            模式发生2-4次
 * oneOrMore()	        模式发生的次数             	pattern.oneOrMore()                     	    发生1次或多次
 * timesOrMore()	    模式发生的次数             	pattern.timesOrMore(2)	                        发生2次或多次
 * optional()	        要么触发要么不触发	            pattern.times(2).optional()             	    发生0次或2次
 * greedy()	            贪心匹配	                    pattern.times(2,4).greedy()             	    触发2、3、4次，尽可能重复执行(应用在多个Pattern中)
 * until()	            停止条件                    	pattern.oneOrMore().until(_ = 0)        	    遇都0结束
 * subtype()	        定义子类型条件             	pattern.subtype(Event.class)	                只与Event事件匹配
 * within() 	        事件限制                    	pattern.within(Time.seconds(5))         	    匹配在5s内发生
 * begin()	            定义规则开始                  	Pattern.begin("start")	                        定义规则开始，事件类型为Event
 * next()	            严格邻近	                    start.next("next").where(_=1)	                严格匹配下一个必需是1
 * followedBy()	        宽松近邻	                    start.followdBy("middle").where()	            会忽略没有 成功匹配的模式条件
 * followedByAny()	    非确定宽松近邻	                start.followdByAny("middle").where()	        可以忽略已经匹配的条件
 * notNext()	        不让某个事件严格紧邻前一个事件发生	start.notNext("not").where(_=1)	                下一个不能为1
 * notFollowedBy()	    不让某个事件在两个事件之间发生	start.notFollowedBy("not").where()...	        不希望某个事件在某两个事件之间
 * consecutive()	    严格匹配	                    start.where(_=1).times(3).consecutive()     	必需连续三个1才能匹配成功
 * allowCombinations()	不确定连续	                start.where(_=1).times(2).allowCombinations()	只要满足两个1就可以匹配成功
 */
public class CepApiUsage {
}
