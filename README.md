# 发酵罐温度日志质控系统（Flask + SQLite + NDJSON）

把发酵设备持续追加的 NDJSON 温度日志，按“只增不丢、非法整轮拒绝”的规则整理成
质控记录，提供网页查看、CSV 下载，并附带可演示**半行**与**日志轮转**的本地示例。

## 一、设备日志模型

设备只维护挂载目录（默认 `./data`，容器内 `/data`）中的两个文件：

| 文件 | 含义 |
| --- | --- |
| `active.ndjson` | 当前正在追加的活动日志，每行一条 NDJSON |
| `active.ndjson.1` | 归档文件：设备把 `active.ndjson` **重命名**而来，不删除、不覆盖；随后新建同名 `active.ndjson` |

- 目录最多保留这两份；归档后旧文件仍可能被续写。
- 合法记录形如：`{"seq": 12, "ts": "2026-11-15T08:30:00Z", "temp": 26.315}`
  - `seq`：整数，全局按 **+1 连续递增**
  - `ts`：UTC 的 ISO-8601 字符串（必须带时区且偏移为 0，推荐以 `Z` 结尾）
  - `temp`：有限数值（拒绝 `NaN` / `Infinity` / 非数值）

## 二、采集正确性保证（核心需求）

1. **只处理上次成功后新增的字节**：采集进度持久化在 SQLite 的 `file_state` 表，
   每次从已确认的字节偏移继续读。
2. **只纳入以换行结束的合法记录**：读取后按最后一个 `\n` 切分，换行之后的
   **半行**保留在文件中、偏移不前移，下一轮再读。
3. **按 inode 跟踪进度，而非路径**：
   - 轮转后旧文件换了路径（变成 `.1`）但 inode 不变，仍从原偏移续写采集；
   - 新文件 inode 未见过，从头采集；
   - 因而**轮转与采集交错、服务重启、重复点击**都能接续，不漏记、不重记。
   - 另含防御：inode 被文件系统复用（路径角色变化）或文件被截断时从头读，
     由 `seq` 唯一约束与原文比对兜底，陈旧 inode 进度自动清理。
4. **一轮采集 = 一个事务**，任一完整行出现下列情况即**整轮拒绝**，
   已入库记录与采集进度均不变：
   - 非法 JSON / 不是对象 / 空行；
   - `seq` 非整数、`ts` 非 UTC 时刻、`temp` 非有限数值；
   - 序号未按 +1 连续递增；
   - 同一 `seq` 出现不同内容（批内或与库内冲突）。
   - 同序号且内容完全一致视为重复传输，跳过计数、不重复入库。
5. **并发保护**：进程内采集锁 + SQLite 唯一约束，重复/并发点击不会重复入库。
6. **演示重置的原子性**（见下节）：采集与重置交错时，重置成功返回后记录、
   采集进度、采集轮次与两份日志**同时为空**；先启动的采集不会回写旧记录。

每次采集（含被拒绝的轮次）都在 `collect_runs` 表留痕，页面“最近采集轮次”可见
状态、完整行数、新增/重复数、半行字节数与拒绝原因。

### 演示重置（原子、可恢复）

“演示重置”不是简单的先删库再删文件，而是：

1. **先移动文件**：两份日志被 `os.replace` 移入数据目录下的临时暂存目录
   （同文件系统，原子 rename）。任一份移动失败，已移走的文件立即移回，
   数据库完全不动，页面提示失败。
2. **再清库**：两份文件都移走后，才在**一个数据库事务**里清空 `records`、
   `file_state`、`collect_runs`，并把 `reset_state.epoch` 加一提交。
3. **采集防回写**：采集开始时记下 reset token（`epoch + updated_at`），读完日志后
   **在同一个写事务内复核**；如果期间发生过重置（提交，或文件移动后又回滚的
   失败尝试），本轮（含记录、进度、轮次）整体丢弃，页面提示“读取日志期间
   演示重置已生效”。因此“刚读完日志、重置成功”的旧采集不会回写。
4. **崩溃恢复**：重置过程把阶段写进 `reset_state` 日志表。进程中途重启时，
   `preparing` 阶段会把日志移回（回到重置前的完整状态），`committed` 阶段会
   清理暂存（保持空状态）；启动时与每轮采集前都会自动收尾。页面读取与重置
   互斥，因此页面只会看到“重置前完整状态”或“重置后空状态”。
5. 重复点击重置、重置后继续采集都正常；采集的半行续接与轮转规则不受影响。
6. 页面显示的快照（记录、进度、轮次、目录文件）在同一把锁内读取，不会跨在
   重置中间；若崩溃恢复暂时失败，页面顶部会给出提示并保留可读状态。

## 三、目录结构

```
.
├── app/
│   ├── __init__.py        # Flask 路由：页面 / 采集 / CSV / 演示重置 / 健康检查
│   ├── collector.py       # 采集核心：增量读取、半行切分、校验、事务、进度表
│   └── templates/index.html
├── demo/
│   └── device_sim.py      # 设备模拟器（仅标准库）：write/half/rotate/illegal/seed/auto ...
├── tests/
│   └── test_collector.py  # 32 个 unittest 用例（采集器 + 重置原子性 + Web 端到端）
├── scripts/               # 本地启动 / 测试 / 演示的快捷脚本
├── data/                  # Compose 挂载的日志目录（git 仅保留 .gitkeep）
├── db/                    # SQLite 持久化目录（容器内 /instance）
├── run.py                 # 启动入口
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## 四、用 Docker Compose 启动（推荐）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

- 网页：`http://localhost:8000/`
- CSV：`http://localhost:8000/records.csv`
- 健康检查：`http://localhost:8000/healthz`
- 卷挂载：`./data -> /data`（日志）、`./db -> /instance`（SQLite）
- 演示重置开关 `ENABLE_DEMO_RESET=1` 已在 compose 中开启，页面显示“演示重置”按钮。

另开一个终端，用模拟器对挂载目录造数据（容器与宿主机共享 `./data`）：

```bash
# 方式 A：一键完整剧本（自动调用网页的 /collect，含半行、轮转、非法拒绝、恢复）
python3 -I demo/device_sim.py --data-dir ./data auto --base-url http://localhost:8000

# 方式 B：手动逐步演示
python3 -I demo/device_sim.py --data-dir ./data seed     # 预置：.1 含半行、双文件样本
# 浏览器点“立即采集” -> 5 条入库，页面显示半行 30 字节
python3 -I demo/device_sim.py --data-dir ./data finish-half
# 再点“立即采集” -> 半行补录为 seq 6
```

> 模拟器与应用代码分离：`demo/` 是独立目录、只用标准库，可单独拷贝到宿主机运行。
> 半行辅助元数据 `.half.json` 仅模拟器使用，采集端只读 `active.ndjson*` 两个文件。

## 五、不用 Docker 的本地运行

需要 Python 3.11+ 与 Flask：

```bash
python3 -m pip install -r requirements.txt   # 或用虚拟环境
./scripts/run_local.sh                       # 默认 http://localhost:8000
# 另开终端
./scripts/demo.sh seed
./scripts/demo.sh auto
```

## 六、模拟器子命令

| 命令 | 作用 |
| --- | --- |
| `write N` | 追加 N 条完整合法记录（序号自动接续，统计时计入末尾半行占用的序号） |
| `half` | 写入一条不带换行的半行，并记录 `.half.json` 元数据 |
| `finish-half` | 在两份日志中找到半行并无损补全、加换行 |
| `rotate` | 模拟归档：`active.ndjson` 重命名为 `.1`，再新建空 `active.ndjson` |
| `illegal KIND` | 写一条完整但非法的行：`nan` / `bad-json` / `bad-ts` / `dup-conflict` |
| `seed` | 预置含半行与轮转文件的演示样本 |
| `reset` | 清空两份日志与半行元数据（数据库用网页“演示重置”清理） |
| `auto` | 一键编排 8 步完整剧本并自动调用采集接口 |

## 七、测试

```bash
# 标准库 unittest；若 Flask 在非标准 site-packages，用 TEST_SITE 指定
TEST_SITE=/path/to/site-packages ./scripts/run_tests.sh
# 或
python3 -m unittest discover -s tests -v
```

覆盖：基础增量与幂等点击、半行留待下次、轮转后旧文件续写+新文件接续、
双轮转与陈旧 inode 清理、inode 复用防御、各类非法行整轮拒绝且进度不变、
序号断档/同序号异内容/同序号同内容、重启恢复、轮转与采集交错、Web 页面与 CSV。

重置专项：成功返回后记录/进度/轮次/两份日志同时为空；采集中途重置后旧采集
不回写（同一线程内联重置、后台线程真实交错、重置失败回滚三种窗口）；第二份
日志移动失败、清库事务失败时原有记录与日志可用、可继续增量采集；`preparing`
/ `committed` 两种崩溃恢复；模拟重启后页面只看到完整旧状态；连续多次重置稳定；
重置后再次采集正常。

## 八、数据库表

- `records`：质控记录（`seq` 唯一）、原文 `raw`、来源文件与 inode、采集时间。
- `file_state`：按 inode 的采集偏移、观测大小、当前路径、更新时间。
- `collect_runs`：每轮采集的状态（`ok`/`rejected`/`error`/`superseded`）与明细、错误原因。
- `reset_state`：单行重置日志（`epoch` 重置代数、`phase` 阶段、暂存目录），
  用于采集防回写与中途重启恢复。
