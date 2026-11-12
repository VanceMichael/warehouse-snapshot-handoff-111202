# snapshot-handoff

读取资产完整快照，按部门状态统计并保留注销资产。

## 当前数据组织

- `assets`：`asset TEXT PRIMARY KEY, department TEXT NOT NULL, status TEXT NOT NULL`

`examples/source.json` 为本地合成输入，`examples/expected.json` 是当前查询的公开样例结果。`schema.json` 描述结构，运行时定义在 `warehouse/definition.py`。业务计算位于 `warehouse/domain.py`，事务装载位于 `warehouse/catalog.py`，内容寻址 JSONL 对象位于 `warehouse/store.py`，全量与增量交接协议位于 `warehouse/handoff.py`。

输入批次以内容 SHA-256 固定；相同批次重投不重复写入，相同批次内容不同则报错。装载以完整输入包为事务，主键冲突不留下半批记录。关系数据保存在 SQLite，精确数量用十进制文本；导出对象按内容哈希命名，读取会重新校验，来源不在运行时修改。

## 全量与增量交接协议

首次接仓（或重新全量）时，导出期间来源仍在变更：先读全量再开增量会漏掉中途注销，先开增量又让旧分页覆盖新状态。协议以"起始水位 + 结束水位 + 同步代次"交接：

1. `begin_handoff`：来源给出起始水位、结束水位和页清单（页号、摘要、续读游标，页号从 1 连续）。同表进行中的旧代次被替换（superseded），代次严格递增。
2. `accept_page`：乱序页允许；同内容重复页幂等跳过；同页号不同内容（与清单摘要不一致校验失败）报错且不计入；旧代次迟到页、已发布代次重投页一律拒绝。来源报告错误（如游标过期）时代次进入 failed，半表不发布。页负载持久化，发布时重新计算摘要复核。
3. `buffer_log`：只缓冲起始水位之后的日志（含更新与删除），位置为可比较的正整数；同位置同内容幂等，同位置不同内容报错。
4. `publish`：仅当页清单完整、全部页摘要复核通过、且起始到结束水位日志连续无缺口时，才在单个事务里合并临时 ODS 与应补日志并原子切换发布；缺页或日志缺口返回等待状态（报告缺口），绝不把半表当成功，也可 `abort_run` 明确失败。旧表在发布前持续对外服务。
5. `apply_incremental`：发布后转常规增量，从当前水位起严格连续应用，缺口等待不跳跃，重投幂等。

分页、缓冲、合并、切换各阶段状态全部持久化在 SQLite（`handoff_runs/pages/staging/log_buffer/watermarks`），中断后 `active_handoff` 返回进行中代次的进度与缺口用于恢复。重新全量时旧代次迟到页不污染新代次，已删除资产不复活。`handoff_status` 给进度与缺口，`preview_diff` 预览发布前后差异。

## 环境与验证

Python 3.11 以上，仅使用标准库，无第三方安装。`runtime.txt` 记录验证解释器版本。时区相关开发使用系统 IANA 时区库。数据库和对象目录均由入口参数指定。源码和合成数据可以直接复制到 Linux 环境运行。

```bash
python3 -m compileall -q warehouse tests scripts
python3 -m unittest discover -s tests -v
```

## 运行

每次使用全新临时目录执行真实导入、查询、导出和内容校验：

```bash
python3 scripts/demo.py
```

交接协议演示（更新发生在分页前后、删除、空表、续读恢复、代次替换、游标过期、缺口等待、发布后旧页重投不倒退，最终与来源结束水位完整状态核对）：

```bash
python3 scripts/handoff_demo.py
```

分步调用可用：

```bash
python3 -m warehouse import --db /tmp/snapshot-handoff.sqlite3 --input examples/source.json --batch first
python3 -m warehouse inspect --db /tmp/snapshot-handoff.sqlite3
python3 -m warehouse export --db /tmp/snapshot-handoff.sqlite3 --objects /tmp/snapshot-handoff-objects
```

交接协议分步调用（plan 含水位与页清单，page/log 为对应 JSON）：

```bash
python3 -m warehouse handoff-begin --db /tmp/wh.sqlite3 --plan plan.json
python3 -m warehouse handoff-page --db /tmp/wh.sqlite3 --page page1.json
python3 -m warehouse handoff-log --db /tmp/wh.sqlite3 --generation 1 --log log.json
python3 -m warehouse handoff-status --db /tmp/wh.sqlite3
python3 -m warehouse handoff-preview --db /tmp/wh.sqlite3 --generation 1
python3 -m warehouse handoff-publish --db /tmp/wh.sqlite3 --generation 1
python3 -m warehouse handoff-incremental --db /tmp/wh.sqlite3 --log more.json
```

`scripts/demo.py` 打印实际命令和完整输出，验证首次写入、重复导入、查询和导出往返。小规模样例在数秒内结束，失败返回非零退出码，不回放预制日志。手工重复使用数据库路径时应留意已存在的业务主键。
