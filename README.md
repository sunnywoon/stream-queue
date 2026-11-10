# stream-queue

基于 Redis Stream 的小型 PHP 任务队列，接口风格参考 beanstalk。

## 版本要求

- PHP `>=7.4`
- `ext-redis` `^5.2`
- Redis `>=6.2`，恢复扫描依赖 `XAUTOCLAIM`
- 当前验证环境：PHP 8.2.12、ext-redis 5.3.7、Redis 7.0.15

Redis 6.2 以下不再支持。`XAUTOCLAIM` 不可用时会抛出明确异常，不会静默降级到旧算法。

## 单消费组模型

**每条 Redis Stream 只对应一个消费组。本库不支持在同一个 Stream 上使用多个消费组做广播或隔离投递。**

同一消费组内可以运行多个 consumer 共同分担任务，这是受支持且推荐的用法。若检测到 Stream 上存在第二个消费组，或现有组名与配置不一致，构造队列及常规操作会抛出 `RuntimeException` 并拒绝执行。

单消费组模型也是确认后可以安全删除正文的前提：确认（`XACK`）后执行 `XDEL` 不会破坏其他消费组尚未读取的数据，因为本模型下不存在其他消费组。`delTask()` 只会删除本次真正 `XACK` 成功的条目；未投递或无效确认的条目不会被误删。

本库无法阻止其他客户端绕过本库直接创建消费组；但本库在每次常规操作前都会重新检查，发现第二组后即拒绝执行。阻塞领取会在进入阻塞前检查消费组；阻塞期间若外部客户端新增第二组，本库无法中断已经发出的 `XREADGROUP`，部署方必须保证外部客户端遵守单组约束。外部直接写入（例如绕过 `addTask()` 的 `XADD`）也不会自动受到容量限制。

## 安装

```sh
composer install
```

在业务项目中使用时，请按实际发布方式引入 `StreamQueue\RedisQueue` 的 PSR-4 自动加载。

## 快速开始

```php
<?php

use StreamQueue\RedisQueue;

$config = [
    'server'   => '127.0.0.1:6379',
    'stream'   => 'jobs',
    'group'    => 'jobs-group',
    'consumer' => 'worker-1',
    'maxLength' => 1000, // 0 表示不限制
];

$queue = new RedisQueue($config);

try {
    $id = $queue->addTask(['task' => 1]);
} catch (OverflowException $e) {
    // 队列已满：等待、重试或在上游限流，绝不能假设任务已被接收
}

$tasks = $queue->getTask(1000, 1); // 阻塞 1000ms，取 1 条
if ($tasks) {
    $id = key($tasks);
    // 处理业务，成功后确认并删除
    $queue->delTask($id);
}
```

## 接口说明

### addTask(array $data, string $id = "*")

向 Stream 添加任务，返回任务 ID。

- `maxLength = 0`：不限制容量。
- `maxLength > 0`：表示 Stream 当前允许的最大条目数，包含未领取和 pending 条目。
- 达到上限时拒绝入队并抛出 `OverflowException`，**不会裁剪任何未完成任务**。
- 只有 `delTask()` 成功确认并删除后才会释放容量。
- 组校验、`XLEN` 检查和 `XADD` 在同一个 Lua 脚本中原子执行，并发入队不会突破上限。
- 无效的 `maxLength` 抛出 `InvalidArgumentException`；其他 Redis 错误保持为 `RedisException`，不会被伪装成容量不足或成功。

### getTask($block = null, $count = 1, $start = '>')

从消费组领取任务。

- 默认 `$start = '>'`，只读取新任务。
- 指定历史起点（例如 `'0'`）时，读取当前 consumer 名下大于该 ID 的未确认历史任务。
- 无数据时按 ext-redis 的原始结果返回。

恢复后的任务不会重新出现在 `>` 读取中，必须通过同一 consumer 的历史读取方式处理。

### delTask($ids)

确认并删除任务，返回 `array(确认成功数, 实际删除数)`。

- 在同一个 Lua 脚本中完成单组校验、`XACK` 和 `XDEL`。
- 只对本次真正 `XACK` 成功的 ID 执行 `XDEL`。
- 检测到第二消费组时在删除前拒绝，正文和 PEL 保持不变。

### pendingClaim($idleTime = 5000, $newConsumer = null, $perPage = 20)

把消费组中 idle 超过 `$idleTime` 毫秒的任务认领给 `$newConsumer`，返回成功认领且正文存在的任务数。

- 使用 Redis 6.2+ 的 `XAUTOCLAIM`，从 `0-0` 扫描整个消费组的 PEL，直到服务端游标回到 `0-0`。
- 扫描范围是整个消费组，不只是当前 consumer；同一组内的跨 consumer 超时任务也会被覆盖。
- 即使某一页没有成功认领，也会继续扫描，不会饿死后页的超时任务。
- 空 PEL 返回 `0`；缺少 `$newConsumer` 时保留返回 `false` 的行为。
- `$idleTime >= 0`，`$perPage > 0`，非法参数抛出 `InvalidArgumentException`。
- `$idleTime = 0` 表示可以立即重新认领，因此不提供并发独占保证。

认领由 Redis 原子完成。正 idle 阈值下，并发恢复不会根据过期快照强制夺取刚被认领的任务；但超时后仍可能再次转移，消费端应按至少一次投递处理。

### pendingRestore($idleTime = 5000, $perPage = 20)

把超时任务认领给当前队列对象配置的 consumer，返回成功恢复数量。

- 复用 `pendingClaim()` 的扫描与认领逻辑。
- **不会先删除再重新入队**，任务 ID 和正文保持不变。
- 认领后、确认前进程退出不会丢任务：任务仍留在 Stream 和目标 consumer 的 PEL 中。
- 调用方应使用同一 consumer 的历史读取方式（例如 `getTask(null, 10, '0')`）读取恢复任务，处理成功后调用 `delTask()`。
- 如果 Stream 正文已经缺失，会清理无效 PEL、通过 `error_log` 记录诊断信息，且不计入成功数量，不会触发未定义下标错误。最近一次扫描清理的数量可通过 `getLastClaimMissingBodyCount()` 读取。
- 不承诺业务恰好一次。重复投递可能发生，业务操作应具备幂等性。

### destoryStream()

显式破坏性管理操作。删除整个 Stream，包括全部任务正文、消费组、PEL 和所有 consumer 的消费状态。会影响该 Stream 上的全部使用者，请仅在明确需要清空队列时调用。

## 单消费组约束与错误

- 构造时会检查 `XINFO GROUPS`：只允许无组或唯一且名称匹配的组。
- 已存在不同名称的组，或存在第二个组时，构造抛出 `RuntimeException`。
- 入队、领取、认领、恢复和确认等常规操作都会重新检查；对象创建后由外部新增第二组也会被拒绝。
- 已有多个消费组的部署不能直接升级后继续广播使用，必须先完成数据迁移和组清理。

## 容量控制与迁移影响

本次修复包含以下破坏性变更：

1. **Redis 最低版本提高到 6.2。** 旧服务器需要先升级。
2. **`addTask()` 满容量由有损裁剪改为抛出 `OverflowException`。** 调用方必须捕获并实施等待、重试或上游限流。原先依赖 `MAXLEN` 自动截断的行为不再可用。
3. **`pendingRestore()` 不再生成新 ID，也不会让任务重新出现在 `>` 读取中。** 调用方需要读取目标 consumer 的历史 PEL（例如 `getTask(null, 10, '0')`）。
4. **`pendingClaim()` 扫描整个消费组，而非仅当前 consumer。** 认领范围扩大，业务需了解跨 consumer 的任务来源。
5. **第二消费组会导致常规操作拒绝执行。** 多组部署必须先迁移数据并清理消费组。
6. **`delTask()` 不再删除未成功确认的 ID。** 这防止误删未完成任务，但也意味着无效确认不会顺带清理正文。

## 测试

测试使用正式 PHPUnit，并会自行启动隔离的临时 Redis：

```sh
composer install
composer test
```

默认行为：

- 在 `127.0.0.1` 的空闲端口启动 `redis-server`。
- 使用临时目录，关闭 RDB/AOF 持久化。
- 测试结束（包括失败或异常退出）时关闭自行启动的实例并清理临时文件。
- 端口争用会自动重试。

也可以连接外部测试 Redis，此时不会启动或关闭外部服务：

```sh
REDIS_HOST=127.0.0.1 REDIS_PORT=6379 composer test
```

外部模式下测试使用唯一的 Stream 前缀，只删除测试自己创建的键，不执行 `FLUSHALL` 或 `FLUSHDB`。

`redis-server` 不在 PATH 时，可通过 `REDIS_SERVER_BIN` 指定可执行文件路径。缺少扩展、依赖、命令或连接失败会明确失败，不会通过跳过核心集成测试制造通过结果。

测试覆盖单条 pending、空 PEL、多批分页、跨 consumer、混合 idle、恢复中途退出、并发恢复、容量已满、并发入队、正文缺失、第二消费组拒绝、确认删除和整条 Stream 删除等场景。
