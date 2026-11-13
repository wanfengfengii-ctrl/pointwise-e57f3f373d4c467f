# 离线两路对账
两家采集代理导出的站点读数以业务键全外关联。命令只是管道调用入口。

## 已有入口
Python 调用：
`joiner.join(left, right, budget, *, fanout=4, metrics=None)`
返回生成器。metrics 省略或传入新的 spill.Metrics。left/right 为仅能消费一次的记录迭代器；两侧各自的 id 唯一，由上游保证。
记录恰含 id、key、payload：id 为非空字符串，key 为字符串或 null，payload 为字符串。
键及 ID 均逐字比较，不作大小写或 Unicode 归一化；空字符串键合法。

每条输出为 [左id, 左payload, 右id, 右payload]，缺侧两个值均为 null。
相等的非 null 键产生全部左右组合，不能按 id 或相同载荷去重。
null 键不匹配任何行，包括另一个 null；没有任何匹配的输入行恰输出一条缺侧行。
输入空侧、两侧全空均合法；输出顺序任意，按多重集核账。不得修改输入。

文件入口 `python3 run.py example.json`：请求对象含 left、right 数组、budget 整数，
可选 fanout；stdout 每行一个输出数组，stderr 给出 peak_payload。
example.json 的三条输出（顺序不限）：
["L1","reading=12","R1","reading=13"]
["L2","unlabelled",null,null]
[null,null,"R2","unlabelled"]

## 已有资源口径
budget >= 512，fanout 为 2 至 8 的整数。每条记录由 spill.encode 编码后的字节数不得超过 budget//4；
否则沿用 INVALID_BUDGET、INVALID_FANOUT、INVALID_ROW、ROW_TOO_LARGE 的 ValueError。
非法输入不要求撤回此前流式输出，不新增批次事务或进程恢复。

spill.Partition 是可反复顺扫的临时长度帧文件，blocks(cap) 在退出或关闭时释放块。
Metrics 的 live/peak 是暂存解码记录的逻辑载荷字节数，不是 Python RSS：
同一记录引用不重复计费，调用者保留的输入/已交付输出不计，单记录序列化缓冲不计。
所有由关联器保留的解码记录必须经过 acquire/release；live 不得超过 budget，结束或 close 后为零。
匹配标记、键索引等内存辅助状态只能随当前块容量增长，不能随整个分区行数、键数或输出数增长；
磁盘辅助状态允许随输入行数增长。不能把未计费的记录、键集合或序列化载荷藏在内存中。
已有 Metrics.reads/sizes 记录各临时分区的实际解码行数与行数，不能关闭或伪造计量。

保留递归分区和热点分块回退；同键无法继续细分时须有界终止，不能提高预算、丢弃行或整分区装载。
热点回退可选择任一侧为外侧。外分区只顺扫一次，每个外块最多顺扫内分区一次，
另允许一次内分区收尾扫描；不得逐输入行重扫对侧、重新哈希逃避回退或写出匹配组合再回读。
内存内分支和分区策略可兼容调整，不要求输出排序。不能以全库 SQLite 关联或第三方关联库替代实现。
生成器正常结束、异常或显式 close 均须关闭自己创建的临时文件，不能要求耗尽输出才能清理。
不要求进程被杀后的文件恢复。保留固定最大分区深度，不开发通用查询执行器。

## 样例与回归
samples.normal 是内存内全外关联；samples.incident 的两个真实键碰到同一首层桶，两侧块序相反。
事故应有 61 条匹配输出，不能出现缺侧行；旧实现错误产生 83 条。
`python3 checks.py normal` 应退出 0，包含 NORMAL_REGRESSION_OK。
`python3 checks.py repair` 旧基线退出 1，包含 JOIN_SPILL_MISMATCH；修好后应退出 0。
`python3 checks.py verify` 重跑上述业务断言，不能删除或削弱原断言。
`python3 checks.py demo` 打印真实事故输入、实际输出与期望行数，不能只打印成功。
可补独立小输入核账、块边界、分区数变化和单热点压力验证；补测须调用同一公开接口。
