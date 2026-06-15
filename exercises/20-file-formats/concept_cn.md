# 练习 20 — 文件格式（JSON/CSV）

## 你将学到

- 使用 TextLineInputFormat 读取 CSV
- 结构化输出的 JSON 格式化
- 带滚动策略的 FileSink

---

## 核心概念

- TextLineInputFormat 将文件行读取为字符串
- 使用 String.split() 手动解析 CSV 简单但有效
- 使用 String.format() 构建 JSON 产生结构化输出
- FileSink 使用可配置的滚动策略写入格式化输出

## 注意事项

- 文件路径必须从所有任务管理器可访问
- 使用 data/ 目录进行 Docker 兼容的文件 I/O
- flink-csv 和 flink-json 依赖可用于高级格式处理
