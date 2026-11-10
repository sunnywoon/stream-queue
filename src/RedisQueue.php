<?php

namespace StreamQueue;

use Redis;

/*
 * 本队列要求 Redis 6.2 及以上版本（依赖 XAUTOCLAIM）。
 *
 * 产品模型：每条 Redis Stream 只对应一个消费组。
 * 不支持在同一个 Stream 上使用多个消费组做广播或隔离投递。
 * 同一消费组内允许多个 consumer 共同分担任务。
 *
 * 使用 Redis Stream 仿照 beanstalk 封装的队列服务。
 */

class RedisQueue
{
    protected $_mRedis = null;
    protected $_mStream = '';
    protected $_mGroup = '';
    protected $_mConsumer = '';

    // 默认0 不限制长度；正整数表示 Stream 当前允许的最大条目数
    protected $_mMaxLength = 0;

    // 最近一次认领扫描中，因正文缺失而清理的无效 PEL 条目数
    protected $_mLastClaimMissingBodyCount = 0;

    /*
     * 入队脚本：组校验、容量检查和 XADD 在同一个 Redis 原子操作内完成。
     * 不再使用带 MAXLEN 的 XADD，绝不为了控制容量裁掉未完成任务。
     *
     * KEYS[1]: stream
     * ARGV[1]: 任务 ID
     * ARGV[2]: 消费组名
     * ARGV[3]: maxLength（0 表示不限制）
     * ARGV[4..]: 字段名、字段值交替排列
     */
    protected const LUA_ADD_TASK = <<<'LUA'
local groups = redis.call('XINFO', 'GROUPS', KEYS[1])
if #groups == 0 then
    return redis.error_reply('STREAMQUEUE_NOGROUP consumer group does not exist')
end
if #groups > 1 then
    return redis.error_reply('STREAMQUEUE_MULTI_GROUP stream has more than one consumer group')
end
if groups[1][2] ~= ARGV[2] then
    return redis.error_reply('STREAMQUEUE_GROUP_MISMATCH consumer group does not match configuration')
end
local maxLength = tonumber(ARGV[3])
if maxLength > 0 then
    local length = redis.call('XLEN', KEYS[1])
    if length >= maxLength then
        return redis.error_reply('STREAMQUEUE_FULL queue capacity is full')
    end
end
return redis.call('XADD', KEYS[1], ARGV[1], unpack(ARGV, 4))
LUA;

    /*
     * 确认脚本：组校验、XACK 和 XDEL 在同一个 Redis 原子操作内完成。
     * 只删除本次真正 XACK 成功的 ID，避免误删未投递或无效确认的条目。
     * 返回 {确认成功数, 实际删除数}，保持原有返回形状。
     *
     * KEYS[1]: stream
     * ARGV[1]: 消费组名
     * ARGV[2..]: 任务 ID
     */
    protected const LUA_DEL_TASK = <<<'LUA'
local groups = redis.call('XINFO', 'GROUPS', KEYS[1])
if #groups == 0 then
    return redis.error_reply('STREAMQUEUE_NOGROUP consumer group does not exist')
end
if #groups > 1 then
    return redis.error_reply('STREAMQUEUE_MULTI_GROUP stream has more than one consumer group')
end
if groups[1][2] ~= ARGV[1] then
    return redis.error_reply('STREAMQUEUE_GROUP_MISMATCH consumer group does not match configuration')
end
local acked = 0
local toDelete = {}
for i = 2, #ARGV do
    local n = redis.call('XACK', KEYS[1], ARGV[1], ARGV[i])
    if n > 0 then
        acked = acked + n
        toDelete[#toDelete + 1] = ARGV[i]
    end
end
local deleted = 0
if #toDelete > 0 then
    deleted = redis.call('XDEL', KEYS[1], unpack(toDelete))
end
return {acked, deleted}
LUA;

    /* 每页认领都原子校验消费组，拒绝扫描期间外部新增的第二组。 */
    protected const LUA_AUTO_CLAIM = <<<'LUA'
local groups = redis.call('XINFO', 'GROUPS', KEYS[1])
if #groups == 0 then
    return redis.error_reply('STREAMQUEUE_NOGROUP consumer group does not exist')
end
if #groups > 1 then
    return redis.error_reply('STREAMQUEUE_MULTI_GROUP stream has more than one consumer group')
end
if groups[1][2] ~= ARGV[1] then
    return redis.error_reply('STREAMQUEUE_GROUP_MISMATCH consumer group does not match configuration')
end
return redis.call('XAUTOCLAIM', KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4], 'COUNT', ARGV[5])
LUA;

    /*
     * 创建队列, stream+group确认唯一队列
     * $config必须包括:
     * stream: stream名
     * server: 格式ip:port[:auth]
     *
     * 可选参数：
     * maxLength:队列最大条目数, 0表示不限制. 满时addTask抛出OverflowException
     * group:分组名, 默认与stream相同. 单消费组模型下每个stream只允许一个分组
     * consumer:消费者名, 默认与stream相同.
     * */
    public function __construct(array $config, Redis $redis = null)
    {
        if (!isset($config['stream'])) {
            throw new \Exception("you must config the stream");
        }

        $this->_mStream = $config['stream'];

        if (is_null($redis)) {

            if (!isset($config['server'])) {
                throw new \Exception("you must config the server");
            }

            $tmp  = explode(':', $config['server']);
            $host = $tmp[0];
            $port = $tmp[1];
            $auth = $tmp[2] ?? null;

            if ($host && $port) {
                $this->_mRedis = new Redis();
                $this->_mRedis->connect($host, $port, 1);
                if ($auth) {
                    $this->_mRedis->auth($auth);
                }
            } else {
                throw new \Exception("can not get redis server conf");
            }
        } else {
            $this->_mRedis = $redis;
        }

        if (isset($config['maxLength'])) {
            $maxLength = $config['maxLength'];
            if (is_string($maxLength) && ctype_digit($maxLength)) {
                $maxLength = (int) $maxLength;
            }
            if (!is_int($maxLength) || $maxLength < 0) {
                throw new \InvalidArgumentException('maxLength must be a non-negative integer');
            }
            $this->_mMaxLength = $maxLength;
        }

        $this->_mGroup    = $config['group'] ?? $config['stream'];
        $this->_mConsumer = $config['consumer'] ?? $config['stream'];

        $this->creatGroup();
    }

    /*
     * 删除当前流(队列)
     *
     * 这是显式破坏性管理操作：会删除整个 Stream、全部任务正文、
     * 消费组以及所有 consumer 的 PEL，影响该 Stream 上的全部使用者。
     * */
    public function destoryStream()
    {
        $this->_mRedis->del($this->_mStream);
    }

    /*
     * 向流中添加任务
     * $data: array
     * $id: string
     *
     * 容量控制是安全的：maxLength > 0 且当前条目数已达上限时拒绝入队并抛出
     * OverflowException，不会裁剪任何未领取或未确认的任务。
     * 只有通过 delTask() 成功确认并删除后才会释放容量。
     * */
    public function addTask(array $data, string $id = "*")
    {
        $args = array($this->_mStream, $id, $this->_mGroup, (string) $this->_mMaxLength);
        foreach ($data as $field => $value) {
            if (!is_scalar($value)) {
                throw new \InvalidArgumentException('task field values must be scalar');
            }
            $args[] = (string) $field;
            $args[] = (string) $value;
        }

        try {
            $this->_mRedis->clearLastError();
            $result = $this->_mRedis->eval(self::LUA_ADD_TASK, $args, 1);

            if ($result === false) {
                // Lua 内部 XADD 失败时 phpredis 可能返回 false，而不是抛出异常
                throw new \RedisException($this->_mRedis->getLastError() ?: 'XADD failed');
            }

            return $result;
        } catch (\RedisException $e) {
            $this->throwGroupOrCapacityException($e);
        }
    }

    /*
     * 从group中获取任务
     * $block:阻塞时间，毫秒. null不阻塞
     * $count:读取条数, 只要有数据，条数不够也会立刻返回，即使设置了block
     * $start:'>'接受最新数据. 若设置id，则读取大于该id，且未被ack的历史任务
     *
     * return [
     *      'id1' => taskdata1,
     *      'id2' => taskdata2,
     *      ... ...
     * ]
     *
     * 无数据返回[]
     *
     * 阻塞读取只能在进入阻塞前检查消费组；阻塞期间外部客户端若新增第二组，
     * 本库无法中断已经发出的 XREADGROUP，部署方需保证外部客户端遵守单组约束。
     * */
    public function getTask($block = null, $count = 1, $start = '>')
    {
        $this->assertSingleGroup('read task');

        $d = $this->_mRedis->xReadGroup($this->_mGroup, $this->_mConsumer, [$this->_mStream => $start], $count, $block);

        if (is_array($d) && count($d) > 0) {
            return $d[$this->_mStream];
        }

        return $d;
    }

    /*
     * 根据id
     * ack任务--从pending中删除
     * 同时从stream中删除
     *
     * 单消费组模型下，XACK 后执行 XDEL 是安全的：不存在其他消费组还需要
     * 这份正文。脚本只删除本次真正 XACK 成功的 ID，未投递或无效确认的
     * 条目会保留。返回值保持为 array(确认成功数, 实际删除数)。
     * */
    public function delTask($ids)
    {
        if (!is_array($ids)) {
            $ids = array($ids);
        }

        $args = array($this->_mStream, $this->_mGroup);
        foreach ($ids as $id) {
            $args[] = (string) $id;
        }

        try {
            $this->_mRedis->clearLastError();
            $res = $this->_mRedis->eval(self::LUA_DEL_TASK, $args, 1);

            if ($res === false) {
                throw new \RedisException($this->_mRedis->getLastError() ?: 'XACK/XDEL failed');
            }
        } catch (\RedisException $e) {
            $this->throwGroupOrCapacityException($e);
        }

        if (!is_array($res) || count($res) < 2) {
            throw new \RuntimeException('unexpected XACK/XDEL result');
        }

        return array((int) $res[0], (int) $res[1]);
    }

    /*
     * 创建消费组；如果流上已经存在同名组则复用。
     * 单消费组模型下，出现第二个组或组名不匹配时明确拒绝。
     * */
    protected function creatGroup($startID = 0)
    {
        $groups = $this->getGroups();

        if (count($groups) === 0) {
            try {
                $created = $this->_mRedis->xGroup('CREATE', $this->_mStream, $this->_mGroup, $startID, true);
            } catch (\RedisException $e) {
                if (strpos($e->getMessage(), 'BUSYGROUP') === false) {
                    throw $e;
                }
                $created = true;
            }

            if ($created === false) {
                // 可能是并发创建时的 BUSYGROUP，也可能是真的创建失败
                if (count($this->getGroups()) === 0) {
                    throw new \RuntimeException('failed to create consumer group');
                }
            }
        }

        $this->assertSingleGroup('create group');
        return true;
    }

    /*
     * 读取 Stream 上的所有消费组。
     * Stream 不存在时 xInfo 返回 false，这里归一化为空数组。
     * */
    protected function getGroups()
    {
        $groups = $this->_mRedis->xInfo('GROUPS', $this->_mStream);
        return is_array($groups) ? $groups : array();
    }

    /*
     * 单消费组模型校验：只允许无组或唯一且名称匹配的组。
     * 该检查无法阻止其他客户端绕过本库直接创建消费组，但本库的常规操作会
     * 在每次调用时重新检查，已存在第二组时明确拒绝执行。
     * */
    protected function assertSingleGroup($operation = 'operation')
    {
        $groups = $this->getGroups();

        if (count($groups) > 1) {
            throw new \RuntimeException(sprintf(
                'cannot %s: stream "%s" has %d consumer groups; this queue supports only one consumer group per stream',
                $operation,
                $this->_mStream,
                count($groups)
            ));
        }

        if (count($groups) === 1) {
            $groupName = $groups[0]['name'] ?? ($groups[0][0] ?? null);
            if ($groupName !== $this->_mGroup) {
                throw new \RuntimeException(sprintf(
                    'cannot %s: stream "%s" belongs to consumer group "%s", not configured group "%s"',
                    $operation,
                    $this->_mStream,
                    $groupName ?? '',
                    $this->_mGroup
                ));
            }
        }

        return true;
    }

    /*
     * 将 Lua 脚本返回的组错误和容量错误转换为明确的 PHP 异常。
     * 其他 Redis 错误保持原样，避免伪装成成功或容量不足。
     * */
    protected function throwGroupOrCapacityException(\RedisException $e)
    {
        $message = $e->getMessage();

        if (strpos($message, 'STREAMQUEUE_FULL') !== false) {
            throw new \OverflowException(sprintf(
                'queue "%s" is full (maxLength=%d); acknowledge tasks before adding more',
                $this->_mStream,
                $this->_mMaxLength
            ), 0, $e);
        }

        if (strpos($message, 'STREAMQUEUE_MULTI_GROUP') !== false) {
            throw new \RuntimeException(sprintf(
                'stream "%s" has more than one consumer group; this queue supports only one consumer group per stream',
                $this->_mStream
            ), 0, $e);
        }

        if (strpos($message, 'STREAMQUEUE_GROUP_MISMATCH') !== false) {
            throw new \RuntimeException(sprintf(
                'stream "%s" does not belong to configured consumer group "%s"',
                $this->_mStream,
                $this->_mGroup
            ), 0, $e);
        }

        if (strpos($message, 'STREAMQUEUE_NOGROUP') !== false) {
            throw new \RuntimeException(sprintf(
                'consumer group "%s" does not exist on stream "%s"',
                $this->_mGroup,
                $this->_mStream
            ), 0, $e);
        }

        throw $e;
    }

    /*
     * 获取pending队列概要信息
     * return
     *
     * array(4) {
     *      [0] => int(2) //未收到ack的总条数
     *      [1] => string(15) "1604908088616-0" //startID
     *      [2] => string(15) "1604908088616-1" //endID
     *      [3] =>  //每个消费者的概要
     *      array(1) {
     *          [0] =>
     *              array(2) {
     *                  [0] => string(6) "normal"   //消费者名子
     *                  [1] => string(1) "2"    //条数
     *              }
     *      }
     * }
     */
    protected function getPendingInfo()
    {
        return $this->_mRedis->xPending($this->_mStream, $this->_mGroup);
    }

    /*
     * 获取pending数据
     * $count: 条数
     * $start: 起始id
     * $end: 终止id. - +表示全部
     * $consumer：获取特定consumer的pending数据
     *
     * return
     * array(1) {
     *      [0] =>  //单条数据
     *          array(4) {
     *              [0] => string(15) "1604908973008-0" //id
     *              [1] => string(6) "normal"    //消费者名
     *              [2] => int(0)    //被取走后经历的毫秒数
     *              [3] => int(1)    //投递次数
     *      }
     * }
     */
    protected function getPending($count = 1, $start = '-', $end = '+', $consumer = null)
    {
        if (!$consumer) {
            return $this->_mRedis->xPending($this->_mStream, $this->_mGroup, $start, $end, $count);
        }

        return $this->_mRedis->xPending($this->_mStream, $this->_mGroup, $start, $end, $count, $consumer);
    }

    /*
     * 取[$start, $end]范围内的数据, 注意是闭区间
     *
     * $count:条数，null时表示取全部
     * */
    protected function getRange($start = '-', $end = '+', $count = null)
    {
        if (is_null($count)) {
            return $this->_mRedis->xRange($this->_mStream, $start, $end);
        } else {
            return $this->_mRedis->xRange($this->_mStream, $start, $end, $count);
        }
    }

    /*
     * 将pending队列中超时的任务转移给当前对象配置的 consumer。
     *
     * 恢复不再执行“先删除再重新入队”，而是使用 XAUTOCLAIM 原子转移已有的
     * Stream 条目和 PEL 归属。任务 ID 与正文保持不变；进程在认领后、确认前
     * 退出时，任务仍可从目标 consumer 的历史 PEL 中读取。
     *
     * 调用方应使用同一 consumer 且起点为 '0' 的历史读取方式（例如
     * getTask(null, 10, '0')）读取恢复任务，成功处理后调用 delTask()。
     * getTask() 默认的 '>' 不会重新投递历史任务。
     *
     * $idleTime: 超时时间, 毫秒
     * $perPage:每次取的任务数
     *
     * return: 成功恢复（认领且正文存在）的数量
     * */
    public function pendingRestore($idleTime = 5000, $perPage = 20)
    {
        $result = $this->pendingClaim($idleTime, $this->_mConsumer, $perPage);
        return $result === false ? 0 : $result;
    }

    /*
     * 返回最近一次 pendingClaim()/pendingRestore() 扫描中，
     * 因 Stream 正文缺失而清理的无效 PEL 条目数。
     * */
    public function getLastClaimMissingBodyCount()
    {
        return $this->_mLastClaimMissingBodyCount;
    }

    /*
     * 扫描整个消费组的 PEL，把 idle 超过阈值的任务认领给 $newConsumer。
     *
     * 使用 Redis 6.2+ 的 XAUTOCLAIM，从 0-0 开始按服务端返回的游标推进，
     * 直到游标回到 0-0。即使某页没有成功认领也会继续扫描，不依据“本页成功
     * 数量小于页大小”提前结束，因此不会饿死后页的超时任务。
     *
     * 认领范围是整个消费组，而不是当前 consumer。单消费组模型下同一组内的
     * 多个 consumer 都可能成为来源或目标。
     *
     * $idleTime: 超时时间, 毫秒；0 表示可立即重新认领
     * $newConsumer: 之后处理pending任务的消费者名称
     * $perPage: 每次取pending任务的条数
     *
     * return: 满足条件且成功claim的条数；缺少newConsumer时返回false
     * */
    public function pendingClaim($idleTime = 5000, $newConsumer = null, $perPage = 20)
    {
        if (!$newConsumer) {
            return false;
        }

        if (is_string($idleTime) && ctype_digit($idleTime)) {
            $idleTime = (int) $idleTime;
        }
        if (!is_int($idleTime) || $idleTime < 0) {
            throw new \InvalidArgumentException('idleTime must be a non-negative integer');
        }

        if (is_string($perPage) && ctype_digit($perPage)) {
            $perPage = (int) $perPage;
        }
        if (!is_int($perPage) || $perPage <= 0) {
            throw new \InvalidArgumentException('perPage must be a positive integer');
        }

        $this->assertSingleGroup('claim pending tasks');

        $cursor          = '0-0';
        $claimed         = array();
        $missing         = array();
        $this->_mLastClaimMissingBodyCount = 0;

        do {
            list($next, $pageClaimed, $pageMissing) = $this->xAutoClaim(
                (string) $newConsumer,
                $idleTime,
                $cursor,
                $perPage
            );

            foreach ($pageClaimed as $id) {
                $claimed[$id] = true;
            }
            foreach ($pageMissing as $id) {
                $missing[$id] = true;
            }

            if ($next === $cursor && $next !== '0-0') {
                throw new \RuntimeException('XAUTOCLAIM did not advance its cursor');
            }
            $cursor = $next;
        } while ($cursor !== '0-0');

        if (count($missing) > 0) {
            $this->_mLastClaimMissingBodyCount = count($missing);
            foreach (array_chunk(array_keys($missing), $perPage) as $chunk) {
                // 清理正文已缺失的无效 PEL 条目，不计入成功数量
                if ($this->_mRedis->xAck($this->_mStream, $this->_mGroup, $chunk) === false) {
                    error_log(sprintf(
                        'stream-queue: failed to clean %d invalid PEL entries on stream "%s"',
                        count($chunk),
                        $this->_mStream
                    ));
                }
            }
            error_log(sprintf(
                'stream-queue: cleaned %d invalid PEL entr%s without stream body on stream "%s"',
                count($missing),
                count($missing) === 1 ? 'y' : 'ies',
                $this->_mStream
            ));
        }

        return count($claimed);
    }

    /*
     * 执行一次 XAUTOCLAIM 并统一解析 Redis 6.2 与 Redis 7 的响应结构。
     *
     * 返回 array(下一游标, 正文存在的认领ID数组, 正文缺失/已删除的ID数组)。
     * XAUTOCLAIM 在 Redis 6.2 才提供；旧版本明确抛错，不静默降级。
     * */
    protected function xAutoClaim($consumer, $idleTime, $start, $count)
    {
        try {
            $this->_mRedis->clearLastError();
            $reply = $this->_mRedis->eval(self::LUA_AUTO_CLAIM, array(
                $this->_mStream,
                $this->_mGroup,
                $consumer,
                (string) $idleTime,
                $start,
                (string) $count
            ), 1);
        } catch (\RedisException $e) {
            if (stripos($e->getMessage(), 'unknown command') !== false) {
                throw new \RuntimeException(
                    'XAUTOCLAIM requires Redis 6.2 or newer; please upgrade the Redis server',
                    0,
                    $e
                );
            }
            $this->throwGroupOrCapacityException($e);
        }

        if ($reply === false) {
            $error = $this->_mRedis->getLastError() ?: 'XAUTOCLAIM failed';
            if (stripos($error, 'unknown command') !== false) {
                throw new \RuntimeException(
                    'XAUTOCLAIM requires Redis 6.2 or newer; please upgrade the Redis server'
                );
            }
            $this->throwGroupOrCapacityException(new \RedisException($error));
        }

        if (!is_array($reply) || count($reply) < 2) {
            throw new \RuntimeException('unexpected XAUTOCLAIM result');
        }

        $next    = (string) $reply[0];
        $claimed = array();
        $missing = array();

        // 正文存在时形如 [id, [field, value, ...]]
        // Redis 6.2 对正文已删除的条目可能返回 [id, nil] 或类似结构
        foreach ((array) $reply[1] as $entry) {
            if (!is_array($entry) || !isset($entry[0])) {
                continue;
            }

            $id     = (string) $entry[0];
            $fields = $entry[1] ?? null;

            if (is_array($fields) && count($fields) > 0) {
                $claimed[] = $id;
            } else {
                $missing[] = $id;
            }
        }

        // Redis 7 会把 Stream 中已不存在的 PEL 条目放在第三个元素
        if (isset($reply[2]) && is_array($reply[2])) {
            foreach ($reply[2] as $id) {
                $missing[] = (string) $id;
            }
        }

        return array(
            $next,
            array_values(array_unique($claimed)),
            array_values(array_unique($missing)),
        );
    }
}

/* demo
$config = [
        'server' => '10.160.75.237:6379:auth',
        'stream' => 'balltube',
        'consumer' => 'normalprocessor'//可以不设置
    ];

//创建队列
$q = new RedisQueue($config);

//添加任务
$task = ['task'=>1];
$q->addTask($task);

//获取
$timeout = 1000;
$task = $q->getTask($timeout);

//确认并删除
$taskid = key($task);
$q->delTask($taskid);

//处理pending
$q->pendingRestore();
$q->pendingClaim();
 */
