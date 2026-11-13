# 可选公开小样例
从项目根目录分别运行：
- `python3 check_public.py duplicates`：2×3 重复键，旧基线退出 0，PUBLIC_DUPLICATES_OK。
- `python3 check_public.py nulls`：两条 null 键各自缺侧，旧基线退出 0，PUBLIC_NULLS_OK。
- `python3 check_public.py spill`：两侧块序相反，期望 61 条匹配；旧基线退出 1，PUBLIC_SPILL_MISMATCH。

三个样例仅用少量记录，通常总计不到一秒，无依赖安装；独立于必须通过的基线正常回归。
spill 的预期失败是已有正确匹配之外多出缺侧行，属于真实业务断言失败。
通过这些样例只代表有限覆盖，不证明所有块边界、内存、I/O、清理或偏斜输入正确。
接口导入或运行异常应保留原异常，不能当作上述预期业务失败；接口被删属于尚未实现。
