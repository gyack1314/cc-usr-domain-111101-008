# 跨城市运营事实的双时间维度关联数据包

运营事实与历史维度资料库，提供严格UTC解析、区间判断、固定时点可见性检查，
以及按固定获知截止连接事实的双时间关联引擎。数据文件位于samples，字段和
边界见docs/domain.md，双时间语义与API见docs/association.md，持久目录接口
见docs/storage.md。

## 本地运行

Python 3.11以上，仅依赖标准库，无需真实账号或外部数据服务。

```bash
python -m datapack.cli inspect samples
python -m datapack.cli join samples --cutoff 2026-11-10T12:00:00Z
python -m datapack.cli impact samples \
  --from-cutoff 2026-11-07T12:00:00Z \
  --to-cutoff 2026-11-10T12:00:00Z
python -m unittest discover -s tests -v
python -m compileall -q datapack tests
```

第一条命令检查已有资料；第二条在固定获知截止下把成功事实写入
`state/fact-joins.jsonl`，把断档/冲突写入 `state/fact-errors.jsonl`；第三条
输出两个截止之间确实改变归属的事实与分区；第四条覆盖读取、存储、领域原语、
双时间边界和影响分析；第五条构建Python字节码。

## 容器

```bash
docker build -t local-datapack .
docker run --rm --network none local-datapack
docker run --rm --network none local-datapack python -m unittest discover -s tests -v
```

应用没有必须常驻的服务，入口和库可在同一容器离线运行。state和output是本地工作目录，已排除出Git。样本文件只读，修订目录使用独立SQLite文件，旧版本可继续查询。

## 目录

datapack/storage.py提供摘要、原子文件与修订目录；datapack/records.py提供文件读取及重复主键分类；datapack/domain.py提供基础领域原语；datapack/association.py提供不可变双时间快照、固定截止连接、错误旁路和修订影响分析；tests验证已有行为与新增关联语义。数据加工结果不由资料检查命令自动生成。
