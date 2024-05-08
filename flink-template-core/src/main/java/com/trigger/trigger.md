Trigger接口方法:
- onElement()： 每个被添加到窗口中的元素都会被调用
- onEventTime()：当事件时间定时器触发时会被调用，比如watermark到达
- onProcessingTime()：当处理时间定时器触发时会被调用，比如时间周期触发
- onMerge()：当两个窗口合并时两个窗口的触发器状态将会被调动并合并
- clear()：执行需要清除相关窗口的事件

TriggerResult:
- CONTINUE：什么都不做
- FIRE：触发计算
- PURGE：清除窗口中的数据
- FIRE_AND_PURGE：触发计算后清除窗口中的数据