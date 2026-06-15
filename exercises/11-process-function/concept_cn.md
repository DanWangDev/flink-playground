# 练习 11 — ProcessFunction

## 你将学到
- KeyedProcessFunction — 最灵活的流运算符
- 定时器 — 为每个键调度未来操作
- 侧输出 — 发送数据到多个输出流
- Context API — 访问时间戳、水位线和状态

## 注意事项
- 定时器在水位线通过触发时间时触发
- 侧输出声明为带类型信息的 OutputTag
- ProcessFunction 状态会被检查点保存
