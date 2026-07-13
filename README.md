# KafkaForwarder

最小可运行的 .NET Worker：使用 Confluent.Kafka 从 Kafka 消费消息并将它们转发到动态 HTTP endpoint（支持并发控制与重试）。

快速开始
1. 克隆或复制本仓库到本地目录。
2. 修改 `appsettings.json` 中的 Kafka 和 Forwarding 配置（BootstrapServers、Topic、EndPoints 等）。
3. 恢复依赖并运行：
   - dotnet restore
   - dotnet run

将项目打包为 ZIP（示例）：
- zip -r KafkaForwarder.zip . -x 'bin/*' 'obj/*' '.git/*'

可选 — 推送到 GitHub（在本地运行）：
- git init
- git add .
- git commit -m "Initial commit"
- git branch -M main
- git remote add origin https://github.com/<owner>/<repo>.git
- git push -u origin main

扩展建议
- 失败策略改为写 dead-letter topic
- 添加 OpenTelemetry / Prometheus 指标
- 增加消息幂等性支持
