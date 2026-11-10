# 可靠性实现审查

## 审查结论

本次按 `PLAN.md` 审查 H1–H4 和恢复扫描相关的 M1，补充修复一处单消费组校验的并发窗口，并增加正式 PHPUnit 回归用例。源码检查与语法检查已完成；集成测试受当前沙箱限制而无法启动，**尚不能判定测试通过，也未完成正式验收**。

当前分支为 `fix/single-group-reliability`。未执行提交、推送、合并或 Git 配置变更。

## 发现及修复

### 认领扫描期间出现第二消费组

原实现仅在整个 `pendingClaim()` 开始时检查消费组，然后连续执行多个独立的 `XAUTOCLAIM`。如果外部客户端在检查后或分页之间创建第二组，后续认领仍会修改 PEL。这与计划中关键写操作原子校验的要求不一致，既有测试只覆盖调用前已存在第二组的情况。

修复在每页认领的 Lua 脚本中先检查唯一消费组及组名，再执行 `XAUTOCLAIM`，使校验与转移处于同一个 Redis 原子操作。组错误沿用既有异常转换机制，对扩展抛异常和返回 `false` 两种情况均处理。Redis 版本不支持命令时仍给出升级提示。

新增 `testSecondGroupCreatedDuringScanStopsFurtherClaims()`：通过受保护方法扩展点，在首批完成、第二批执行前确定性地创建第二组。断言后续调用抛出 `RuntimeException`，只有首批转移，其他任务仍归原 consumer，所有任务 ID、正文和 PEL 保留。此用例尚未获得真实运行结果。

README 补充每页原子校验及部分扫描完成后失败的行为：已完成的认领不会回滚，调用方不能将扫描异常理解成没有任务发生转移。

## 对照计划的检查

- H1：使用服务端返回游标推进 `XAUTOCLAIM`，以 `0-0` 结束；不依赖成功数量结束，单条 pending 可以进入扫描，使用 ID 去重统计。既有测试覆盖空 PEL、单条、多页、零 idle、跨 consumer 和前页无超时任务。
- H2 与 M1：`pendingRestore()` 复用认领，不删除再入队，不分配新 ID。既有测试包括认领后退出、首批认领后退出、并发恢复、历史读取及缺失正文。正文缺失不计成功，并清理无效 PEL。需通过真实 Redis 验证这些断言。
- H3：产品模型为每条 Stream 仅一个消费组，同组可有多个 consumer，不支持多组广播或隔离投递。入队与确认删除使用 Lua 原子校验，本次补齐认领的原子校验。领取在进入 `XREADGROUP` 前检查，阻塞期间外部新增组的边界已在 README 说明。外部客户端必须遵守单组约束。
- 确认后 `XDEL` 的前提是不存在其他消费组需要正文，且只删除本次 `XACK` 成功的 ID；既有测试覆盖无效确认不误删。`destoryStream()` 是删除整个 Stream、全部任务正文、消费组和 PEL 的管理操作，影响所有 consumer。
- H4：入队脚本原子检查 `XLEN` 并执行不带 `MAXLEN` 的 `XADD`，容量满时抛出 `OverflowException`。容量包含未领取及 pending 任务。既有测试覆盖满容量保护、确认后释放、并发争抢及无限容量。
- 正式 PHPUnit 9.6 已加入 `require-dev`，`composer test` 调用 PHPUnit。当前套件共 24 个测试方法，默认启动隔离临时 Redis，外部模式仅清理自身键。子进程测试保留恢复故障及并发验证。

以上为实现及测试代码审查结论，不代表集成测试已经通过。未验证 Redis 6.2 实例；当前可执行的 Redis 版本为 7.0.15。

## 破坏性变更

与 PLAN.md 和 README 保持一致，后续成果报告必须继续列明：

1. 最低 Redis 版本提高到 6.2，旧版本需升级。
2. `addTask()` 满容量不再有损裁剪，而是抛出 `OverflowException`；调用方必须捕获并等待、重试或限流，不能假设任务已接收。
3. `pendingRestore()` 保留原 ID，不再重新入队；调用方需通过目标 consumer 的历史 PEL 读取，例如 `getTask(null, 10, '0')`。
4. `pendingClaim()` 扫描整个消费组，包含其他 consumer 的超时任务。
5. 已有第二消费组或组名不匹配时拒绝操作；多组部署必须迁移并清理组。扫描中出现第二组会中止后续认领，此前认领仍有效。
6. `delTask()` 仅删除本次真正确认成功的 ID，不再顺带删除未成功确认的正文。

正 idle 阈值通过 Redis 原子判断，避免按过期快照立即重复恢复；零 idle 允许立即重新认领。超时后仍可能再次投递，业务必须幂等，不承诺恰好一次。

## 实际验证结果

### 环境

本阶段处于受限沙箱，执行策略禁止申请沙箱外权限。不能以本阶段结果替代要求中的沙箱外真实本地 Redis 验收。

实际运行 `php -v`、`php --ri redis`、`composer --version`、`redis-server --version`，退出码均为 `0`，关键输出：

```text
PHP 8.2.12 (cli)
Redis Version => 5.3.7
Composer version 2.6.5 2023-10-06 10:11:52
Redis server v=7.0.15
```

### 默认测试

实际命令：

```sh
composer test
```

退出码：`1`。关键原始输出：

```text
> phpunit
PHPUnit 9.6.38 by Sebastian Bergmann and contributors.
Error in bootstrap script: RuntimeException:
cannot allocate a free local port: No error information
Script phpunit handling the test event returned with error code 1
```

测试未通过启动阶段，执行用例 `0`，通过 `0`；未产生用例失败或跳过统计，不能记为全部通过。失败原因是沙箱无法绑定测试所需的本地 TCP 端口，并非测试断言失败。

### Unix socket 补充尝试

通过 Python 的 `subprocess.Popen` 启动临时 Redis，参数为：

```sh
redis-server --port 0 --unixsocket /tmp/sq-review-bcj1be_b/redis.sock --unixsocketperm 700 --save '' --appendonly no --dir /tmp/sq-review-bcj1be_b
```

随后通过 `subprocess.run` 运行 `composer test`，设置 `REDIS_HOST=/tmp/sq-review-bcj1be_b/redis.sock` 和 `REDIS_PORT=0`。关键原始输出：

```text
cannot connect to external Redis at /tmp/sq-review-bcj1be_b/redis.sock:0 (REDIS_HOST/REDIS_PORT)
Script phpunit handling the test event returned with error code 1
COMPOSER_TEST_EXIT_CODE=1
REDIS_PROCESS_STOPPED=True
```

退出码：`1`，同样未运行用例。临时进程已在清理逻辑中停止，临时目录已清理。这次尝试也不能算正式验收。

### 静态检查

以下命令退出码均为 `0`：

```sh
php -l src/RedisQueue.php
php -l tests/RedisQueueTest.php
git diff --check
```

语法检查输出：

```text
No syntax errors detected in src/RedisQueue.php
No syntax errors detected in tests/RedisQueueTest.php
```

## 待完成验收

必须在允许本地 TCP 监听及连接的沙箱外执行 `composer test`，保留退出码、通过数量、断言数量、失败和跳过数量及关键输出，并写入后续 RESULT.md。默认模式会自行启动并关闭临时 Redis；不要依赖全局服务，也不要用本报告中的静态检查代替真实测试。

本阶段没有沙箱外执行权限，无法满足“测试全部通过”的完成条件。当前不建议标记验收完成。

建议 PR 标题：修复单消费组队列可靠性并补齐本地 Redis 集成测试。
