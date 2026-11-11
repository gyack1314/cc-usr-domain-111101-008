# 已有领域原语

from datetime import datetime,timezone

源码中的函数是可独立调用的纯计算或内容对象操作；tests/domain_checks.py包含输入示例。它们不隐式读取整个样本包，也不自动完成跨来源业务加工。需要开箱可用的双时间组合时使用datapack/association.py（见association.md）；其余场景调用者可在自己的应用层组合这些原语。输入字段约定以domain.md为准。
