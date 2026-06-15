# 练习 08 — 文件连接器

## 你将学到

- FileSource — 将文件作为有界或连续流读取
- TextLineInputFormat — 逐行文本读取
- FileSink — 将流式结果写入文件
- Docker 兼容的文件路径

---

## 核心概念

- FileSource.forRecordStreamFormat 逐行读取文件
- FileSink.forRowFormat 将文本行写入输出文件
- 滚动策略控制文件何时完成（基于大小或检查点）

## 注意事项

- 文件路径必须从所有 Flink 任务管理器可访问
- 在 Docker 部署中使用共享卷（如 data/）
- File.createTempFile() 仅创建本地文件
