# 数据约定

无偏移日期拒绝；known与valid的空终点为无穷，区间半开。相同cutoff下多个维度版本均可见时，匹配超过一条为冲突。维度链为vehicle->operator->region，软件维度按vehicle额外关联；任一环断档或冲突不得产生正常多行。样本默认获知截止为2026-11-10T12:00:00Z。双时间连接的固定截止、latest解析、事实去重与影响分析约定见association.md。

样本为合成资料，包含正常、缺失、冲突等记录；这些现象用于保留业务数据原貌，读取成功不代表领域结果有效。所有计算原语位于datapack/domain.py；datapack/association.py在不可变快照上组合这些约束，调用者负责最终验证。
