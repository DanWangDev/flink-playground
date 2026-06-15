# 练习 15 — 异步 I/O

## 你将学到

- AsyncDataStream.unorderedWait 非阻塞外部调用
- CompletableFuture 异步结果处理
- 超时和容量配置
- 有序与无序处理

---

## 为什么这很重要

生产环境 Flink 作业需要调用外部服务（数据库、API）。阻塞调用会降低吞吐量。异步 I/O 让你在不阻塞流处理线程的情况下调用外部服务。

---

## 核心概念

AsyncDataStream.unorderedWait 接受四个参数：
- 输入流
- 执行异步调用的 AsyncFunction
- 超时时间（每个请求的最大等待时间）
- 容量（最大并发请求数）

unorderedWait 结果到达即发出（更快）。orderedWait 保持输入顺序。

## 注意事项

- 异步函数在单独的线程池上运行 — 不要阻塞主操作符线程
- 排序保证是每个输入分区的，不是全局的
- 始终设置合理的超时 — 挂起的外部调用会阻塞容量槽
