<?php

use PHPUnit\Framework\TestCase;
use StreamQueue\RedisQueue;

final class RedisQueueTest extends TestCase
{
    /** @var Redis */
    private $redis;
    private $stream;
    private $group = 'test-group';
    private $tempPaths = array();

    protected function setUp(): void
    {
        $this->redis = new Redis();
        $this->redis->connect((string) STREAM_QUEUE_TEST_HOST, (int) STREAM_QUEUE_TEST_PORT, 1);
        $this->stream = 'stream-queue-test:' . bin2hex(random_bytes(8));
    }

    protected function tearDown(): void
    {
        if ($this->redis instanceof Redis) {
            $this->redis->del($this->stream);
            $this->redis->close();
        }

        foreach ($this->tempPaths as $path) {
            if (is_file($path)) {
                @unlink($path);
            }
        }
    }

    public function testBasicEnqueueReadAndAcknowledge()
    {
        $queue = $this->makeQueue();

        $id = $queue->addTask(array('payload' => 'hello'));
        $this->assertIsString($id);

        $tasks = $queue->getTask(null, 1);
        $this->assertArrayHasKey($id, $tasks);
        $this->assertSame('hello', $tasks[$id]['payload']);
        $this->assertSame(1, $this->pendingCount());

        $this->assertSame(array(1, 1), $queue->delTask(array($id)));
        $this->assertSame(0, $this->pendingCount());
        $this->assertSame(0, (int) $this->redis->xLen($this->stream));
    }

    public function testUnlimitedMaxLengthAndCustomId()
    {
        $queue = $this->makeQueue('c1', array('maxLength' => 0));

        for ($i = 1; $i <= 25; $i++) {
            $this->assertIsString($queue->addTask(array('n' => (string) $i)));
        }

        // 自定义 ID 必须大于 Stream 当前最大 ID
        $id = $queue->addTask(array('payload' => 'custom'), '9999999999999-0');
        $this->assertSame('9999999999999-0', $id);
        $this->assertSame(26, (int) $this->redis->xLen($this->stream));

        // 无效 ID 属于 Redis 错误，不能被伪装成成功
        $this->assertThrows(\RedisException::class, function () use ($queue) {
            $queue->addTask(array('payload' => 'older'), '5000-0');
        });
        $this->assertSame(26, (int) $this->redis->xLen($this->stream));
    }

    public function testInvalidMaxLengthIsRejected()
    {
        foreach (array(-1, 'abc', 1.5) as $value) {
            $this->assertThrows(\InvalidArgumentException::class, function () use ($value) {
                new RedisQueue(array(
                    'stream'    => $this->stream,
                    'group'     => $this->group,
                    'consumer'  => 'c1',
                    'maxLength' => $value,
                ), $this->redis);
            });
        }
    }

    public function testCapacityFullRejectsWithoutTrimmingPendingTasks()
    {
        $queue = $this->makeQueue('c1', array('maxLength' => 2));

        $first  = $queue->addTask(array('n' => '1'));
        $second = $queue->addTask(array('n' => '2'));
        $tasks  = $queue->getTask(null, 1);
        $pendingId = key($tasks);

        $exception = $this->assertThrows(\OverflowException::class, function () use ($queue) {
            $queue->addTask(array('n' => '3'));
        });
        $this->assertStringContainsString('full', $exception->getMessage());

        $this->assertSame(2, (int) $this->redis->xLen($this->stream));
        $this->assertCount(2, $this->redis->xRange($this->stream, '-', '+'));
        $this->assertSame(1, $this->pendingCount());
        $this->assertArrayHasKey($pendingId, $this->redis->xRange($this->stream, '-', '+'));

        $this->assertSame(array(1, 1), $queue->delTask(array($pendingId)));
        $this->assertSame(1, (int) $this->redis->xLen($this->stream));

        $third = $queue->addTask(array('n' => '3'));
        $this->assertIsString($third);
        $this->assertSame(2, (int) $this->redis->xLen($this->stream));
        $remaining = array_keys($this->redis->xRange($this->stream, '-', '+'));
        $this->assertNotContains($pendingId, $remaining);
        $this->assertContains($second, $remaining);
        $this->assertContains($third, $remaining);
    }

    public function testDelTaskOnlyDeletesSuccessfullyAcknowledgedIds()
    {
        $queue = $this->makeQueue();

        $first  = $queue->addTask(array('n' => '1'));
        $second = $queue->addTask(array('n' => '2'));

        $tasks = $queue->getTask(null, 1);
        $this->assertArrayHasKey($first, $tasks);

        // 未投递的 second 不在 PEL 中，不能因 delTask 被物理删除
        $this->assertSame(array(0, 0), $queue->delTask(array($second)));
        $this->assertSame(2, (int) $this->redis->xLen($this->stream));
        $this->assertArrayHasKey($second, $this->redis->xRange($this->stream, '-', '+'));

        $newTasks = $queue->getTask(null, 10);
        $this->assertArrayHasKey($second, $newTasks);

        $this->assertSame(array(2, 2), $queue->delTask(array($first, $second)));
        $this->assertSame(0, (int) $this->redis->xLen($this->stream));
        $this->assertSame(0, $this->pendingCount());
    }

    public function testPendingClaimHandlesSinglePendingTask()
    {
        $queue = $this->makeQueue('c1');
        $id    = $queue->addTask(array('payload' => 'single'));

        $queue->getTask(null, 1);
        $this->setPendingIdle('c1', array($id), 60000);

        $this->assertSame(1, $queue->pendingClaim(1000, 'c2'));
        $this->assertSame(0, $this->pendingFor('c1'));
        $this->assertSame(1, $this->pendingFor('c2'));
        $this->assertSame(array($id), array_keys($this->redis->xRange($this->stream, '-', '+')));

        $tasks = $this->makeQueue('c2')->getTask(null, 10, '0');
        $this->assertArrayHasKey($id, $tasks);
        $this->assertSame('single', $tasks[$id]['payload']);
    }

    public function testPendingClaimOnEmptyPelReturnsZero()
    {
        $queue = $this->makeQueue();

        $this->assertSame(0, $queue->pendingClaim(0, 'c2'));
        $this->assertSame(0, $queue->pendingRestore(0, 10));
        $this->assertSame(0, $this->pendingCount());
    }

    public function testPendingClaimPaginatesAcrossMultipleBatches()
    {
        $queue = $this->makeQueue('c1');
        $ids   = array();

        for ($i = 1; $i <= 7; $i++) {
            $ids[] = $queue->addTask(array('n' => (string) $i));
        }

        $this->assertCount(7, $queue->getTask(null, 20));
        $this->setPendingIdle('c1', $ids, 60000);

        $this->assertSame(7, $queue->pendingClaim(1000, 'c2', 3));
        $this->assertSame(0, $this->pendingFor('c1'));
        $this->assertSame(7, $this->pendingFor('c2'));
        $this->assertSame(7, $this->pendingCount());
        $this->assertSame(7, (int) $this->redis->xLen($this->stream));
    }

    public function testPendingClaimScansWholeGroupAcrossConsumers()
    {
        $firstQueue  = $this->makeQueue('c1');
        $secondQueue = $this->makeQueue('c2');

        $firstId  = $firstQueue->addTask(array('n' => '1'));
        $secondId = $secondQueue->addTask(array('n' => '2'));

        $this->assertArrayHasKey($firstId, $firstQueue->getTask(null, 1));
        $this->assertArrayHasKey($secondId, $secondQueue->getTask(null, 1));

        $this->setPendingIdle('c1', array($firstId), 60000);
        $this->setPendingIdle('c2', array($secondId), 60000);

        $this->assertSame(2, $firstQueue->pendingClaim(1000, 'c3'));
        $this->assertSame(0, $this->pendingFor('c1'));
        $this->assertSame(0, $this->pendingFor('c2'));
        $this->assertSame(2, $this->pendingFor('c3'));
    }

    public function testPendingClaimContinuesWhenFirstBatchHasNoExpiredTasks()
    {
        $queue = $this->makeQueue('c1');
        $ids   = array();

        for ($i = 1; $i <= 27; $i++) {
            $ids[] = $queue->addTask(array('n' => (string) $i));
        }

        $this->assertCount(27, $queue->getTask(null, 50));

        $recent = array_slice($ids, 0, 25);
        $expired = array_slice($ids, 25);
        $this->setPendingIdle('c1', $recent, 0);
        $this->setPendingIdle('c1', $expired, 60000);

        // perPage=2 时前几批只包含未超时任务；仍必须继续扫描到后页
        $this->assertSame(2, $queue->pendingClaim(5000, 'c2', 2));
        $this->assertSame(25, $this->pendingFor('c1'));
        $this->assertSame(2, $this->pendingFor('c2'));
    }

    public function testPendingClaimWithZeroIdleClaimsImmediately()
    {
        $queue = $this->makeQueue('c1');
        $id    = $queue->addTask(array('n' => '1'));

        $queue->getTask(null, 1);
        $this->assertSame(1, $queue->pendingClaim(0, 'c2'));
        $this->assertSame(1, $this->pendingFor('c2'));
        $this->assertSame(array($id), array_keys($this->redis->xRange($this->stream, '-', '+')));
    }

    public function testPendingClaimValidatesArguments()
    {
        $queue = $this->makeQueue();

        $this->assertFalse($queue->pendingClaim(1000, null));

        $this->assertThrows(\InvalidArgumentException::class, function () use ($queue) {
            $queue->pendingClaim(-1, 'c2');
        });
        $this->assertThrows(\InvalidArgumentException::class, function () use ($queue) {
            $queue->pendingClaim('abc', 'c2');
        });
        $this->assertThrows(\InvalidArgumentException::class, function () use ($queue) {
            $queue->pendingClaim(1000, 'c2', 0);
        });
        $this->assertThrows(\InvalidArgumentException::class, function () use ($queue) {
            $queue->pendingClaim(1000, 'c2', 'x');
        });
    }

    public function testMissingBodyIsNotCountedAndPelIsCleaned()
    {
        $queue = $this->makeQueue('c1');
        $keep  = $queue->addTask(array('payload' => 'keep'));
        $gone  = $queue->addTask(array('payload' => 'gone'));

        $this->assertCount(2, $queue->getTask(null, 10));
        $this->setPendingIdle('c1', array($keep, $gone), 60000);
        $this->assertSame(1, (int) $this->redis->xDel($this->stream, array($gone)));

        $this->assertSame(1, $queue->pendingClaim(1000, 'c2'));
        $this->assertSame(1, $queue->getLastClaimMissingBodyCount());
        $this->assertSame(1, $this->pendingCount());
        $this->assertSame(1, $this->pendingFor('c2'));

        $tasks = $this->makeQueue('c2')->getTask(null, 10, '0');
        $this->assertArrayHasKey($keep, $tasks);
        $this->assertArrayNotHasKey($gone, $tasks);
        $this->assertSame('keep', $tasks[$keep]['payload']);
    }

    public function testPendingRestoreKeepsIdsBodiesAndReadsHistory()
    {
        $worker = $this->makeQueue('worker');
        $ids    = array();

        for ($i = 1; $i <= 3; $i++) {
            $ids[] = $worker->addTask(array('payload' => 'task-' . $i));
        }

        $this->assertCount(3, $worker->getTask(null, 10));
        $this->setPendingIdle('worker', $ids, 60000);

        $before = array_keys($this->redis->xRange($this->stream, '-', '+'));
        $this->assertSame(3, $worker->pendingRestore(1000, 2));

        $this->assertSame($before, array_keys($this->redis->xRange($this->stream, '-', '+')));
        $this->assertSame(3, (int) $this->redis->xLen($this->stream));
        $this->assertSame(3, $this->pendingFor('worker'));

        $restored = $worker->getTask(null, 10, '0');
        $this->assertCount(3, $restored);
        foreach ($ids as $index => $id) {
            $this->assertArrayHasKey($id, $restored);
            $this->assertSame('task-' . ($index + 1), $restored[$id]['payload']);
        }

        // 恢复任务不会通过默认的 '>' 再次投递
        $newTasks = $worker->getTask();
        $this->assertTrue($newTasks === false || $newTasks === array());
    }

    public function testPendingRestoreHandlesMissingBodyGracefully()
    {
        $queue = $this->makeQueue('c1');
        $keep  = $queue->addTask(array('payload' => 'keep'));
        $gone  = $queue->addTask(array('payload' => 'gone'));

        $queue->getTask(null, 10);
        $this->setPendingIdle('c1', array($keep, $gone), 60000);
        $this->redis->xDel($this->stream, array($gone));

        $restore = $this->makeQueue('restorer');
        $this->assertSame(1, $restore->pendingRestore(1000, 10));
        $this->assertSame(1, $restore->getLastClaimMissingBodyCount());
        $this->assertSame(1, $this->pendingCount());
        $this->assertSame(1, $this->pendingFor('restorer'));

        $tasks = $restore->getTask(null, 10, '0');
        $this->assertArrayHasKey($keep, $tasks);
        $this->assertArrayNotHasKey($gone, $tasks);
    }

    public function testPendingRestoreCrashAfterClaimDoesNotLoseTasks()
    {
        $queue = $this->makeQueue('c1');
        $ids   = array();

        for ($i = 1; $i <= 2; $i++) {
            $ids[] = $queue->addTask(array('payload' => 'task-' . $i));
        }

        $queue->getTask(null, 10);
        $this->setPendingIdle('c1', $ids, 60000);
        $before = array_keys($this->redis->xRange($this->stream, '-', '+'));

        $process = $this->spawnFixture('restore_and_exit.php', array(
            'SQ_TEST_CONSUMER'  => 'crashed-restorer',
            'SQ_TEST_IDLE'      => '1000',
            'SQ_TEST_PERPAGE'   => '10',
            'SQ_TEST_EXIT_CODE' => '7',
        ));
        $result = $this->waitProcess($process[0], $process[1]);

        $this->assertSame(7, $result['exitcode'], $result['stderr']);
        $this->assertSame(2, $this->jsonFromOutput($result['stdout'])['count']);

        // 认领后、确认前“崩溃”：原 ID、正文和 PEL 都还在
        $this->assertSame($before, array_keys($this->redis->xRange($this->stream, '-', '+')));
        $this->assertSame(2, (int) $this->redis->xLen($this->stream));
        $this->assertSame(2, $this->pendingFor('crashed-restorer'));
        $this->assertSame(0, $this->pendingFor('c1'));

        $crashQueue = $this->makeQueue('crashed-restorer');
        $tasks      = $crashQueue->getTask(null, 10, '0');
        $this->assertCount(2, $tasks);
        $this->assertSame(array(2, 2), $crashQueue->delTask(array_keys($tasks)));
        $this->assertSame(0, (int) $this->redis->xLen($this->stream));
        $this->assertSame(0, $this->pendingCount());
    }

    public function testPendingRestoreCrashDuringScanKeepsClaimedAndUnclaimedTasks()
    {
        $queue = $this->makeQueue('c1');
        $ids   = array();

        for ($i = 1; $i <= 2; $i++) {
            $ids[] = $queue->addTask(array('payload' => 'task-' . $i));
        }

        $queue->getTask(null, 10);
        $this->setPendingIdle('c1', $ids, 60000);
        $before = array_keys($this->redis->xRange($this->stream, '-', '+'));

        $process = $this->spawnFixture('restore_crash_midscan.php', array(
            'SQ_TEST_CONSUMER' => 'crashed-midscan',
            'SQ_TEST_IDLE'     => '1000',
            'SQ_TEST_PERPAGE'  => '1',
        ));
        $result = $this->waitProcess($process[0], $process[1]);

        $this->assertSame(9, $result['exitcode'], $result['stderr']);

        // 第一次认领后立即终止：已认领任务留在新 consumer 的 PEL，
        // 未扫描任务留在原 consumer 的 PEL，两者正文和 ID 都不变。
        $this->assertSame($before, array_keys($this->redis->xRange($this->stream, '-', '+')));
        $this->assertSame(2, (int) $this->redis->xLen($this->stream));
        $this->assertSame(2, $this->pendingCount());
        $this->assertSame(1, $this->pendingFor('crashed-midscan'));
        $this->assertSame(1, $this->pendingFor('c1'));

        $crashed = $this->makeQueue('crashed-midscan')->getTask(null, 10, '0');
        $this->assertCount(1, $crashed);

        $original  = $this->makeQueue('c1');
        $remaining = $original->getTask(null, 10, '0');
        $this->assertCount(1, $remaining);

        $this->assertSame(array(2, 2), $original->delTask(array_merge(array_keys($crashed), array_keys($remaining))));
        $this->assertSame(0, (int) $this->redis->xLen($this->stream));
        $this->assertSame(0, $this->pendingCount());
    }

    public function testConcurrentRestoreKeepsSingleCopyAndId()
    {
        $queue = $this->makeQueue('c1');
        $id    = $queue->addTask(array('payload' => 'only-one'));

        $queue->getTask(null, 1);
        $this->setPendingIdle('c1', array($id), 60000);

        $startFile = $this->newTempPath();
        $env = array('SQ_TEST_START_FILE' => $startFile, 'SQ_TEST_IDLE' => '1000', 'SQ_TEST_PERPAGE' => '10');

        $first  = $this->spawnFixture('restore_and_exit.php', $env + array('SQ_TEST_CONSUMER' => 'r1'));
        $second = $this->spawnFixture('restore_and_exit.php', $env + array('SQ_TEST_CONSUMER' => 'r2'));

        file_put_contents($startFile, 'go');

        $firstResult  = $this->waitProcess($first[0], $first[1]);
        $secondResult = $this->waitProcess($second[0], $second[1]);

        $this->assertSame(0, $firstResult['exitcode'], $firstResult['stderr']);
        $this->assertSame(0, $secondResult['exitcode'], $secondResult['stderr']);

        $count = $this->jsonFromOutput($firstResult['stdout'])['count']
               + $this->jsonFromOutput($secondResult['stdout'])['count'];
        $this->assertSame(1, $count);

        $this->assertSame(1, (int) $this->redis->xLen($this->stream));
        $this->assertSame(array($id), array_keys($this->redis->xRange($this->stream, '-', '+')));
        $this->assertSame(1, $this->pendingCount());
        $this->assertSame(1, $this->pendingFor('r1') + $this->pendingFor('r2'));
    }

    public function testConcurrentAddDoesNotExceedCapacity()
    {
        // 先由父进程创建 Stream 和消费组，再让子进程争抢唯一名额
        $this->makeQueue('c1', array('maxLength' => 1));

        $startFile = $this->newTempPath();
        $workers   = array();
        for ($i = 1; $i <= 4; $i++) {
            $workers[] = $this->spawnFixture('add_task.php', array(
                'SQ_TEST_MAXLENGTH'  => '1',
                'SQ_TEST_WORKER'     => 'worker-' . $i,
                'SQ_TEST_START_FILE' => $startFile,
            ));
        }

        file_put_contents($startFile, 'go');

        $success  = 0;
        $overflow = 0;
        foreach ($workers as $worker) {
            $result = $this->waitProcess($worker[0], $worker[1]);
            if ($result['exitcode'] === 0) {
                $success++;
            } elseif ($result['exitcode'] === 2) {
                $overflow++;
            } else {
                $this->fail('unexpected fixture exit code: ' . $result['exitcode'] . "\n" . $result['stderr']);
            }
        }

        $this->assertSame(1, $success);
        $this->assertSame(3, $overflow);
        $this->assertSame(1, (int) $this->redis->xLen($this->stream));
    }

    public function testSecondConsumerGroupCausesRegularOperationsToFail()
    {
        $queue = $this->makeQueue('c1');
        $id    = $queue->addTask(array('payload' => 'protected'));

        $queue->getTask(null, 1);
        $this->assertSame(1, $this->pendingCount());

        $this->redis->rawCommand('XGROUP', 'CREATE', $this->stream, 'second-group', '0', 'MKSTREAM');

        $this->assertThrows(\RuntimeException::class, function () use ($queue) {
            $queue->addTask(array('payload' => 'rejected'));
        });
        $this->assertThrows(\RuntimeException::class, function () use ($queue) {
            $queue->getTask(null, 1);
        });
        $this->assertThrows(\RuntimeException::class, function () use ($queue) {
            $queue->pendingClaim(0, 'c2');
        });
        $this->assertThrows(\RuntimeException::class, function () use ($queue) {
            $queue->pendingRestore(0, 10);
        });
        $this->assertThrows(\RuntimeException::class, function () use ($queue, $id) {
            $queue->delTask(array($id));
        });

        // 拒绝操作时正文和 PEL 不得被破坏
        $this->assertSame(1, (int) $this->redis->xLen($this->stream));
        $this->assertSame(1, $this->pendingCount());
        $this->assertArrayHasKey($id, $this->redis->xRange($this->stream, '-', '+'));
    }

    public function testSecondGroupCreatedDuringScanStopsFurtherClaims()
    {
        $queue = new class(array(
            'stream' => $this->stream,
            'group' => $this->group,
            'consumer' => 'c1',
        ), $this->redis) extends RedisQueue {
            private $pages = 0;

            protected function xAutoClaim($consumer, $idleTime, $start, $count)
            {
                if (++$this->pages === 2) {
                    // 精确模拟首批完成后、下一批命令前出现第二组。
                    $this->_mRedis->xGroup('CREATE', $this->_mStream, 'second-group', '0');
                }
                return parent::xAutoClaim($consumer, $idleTime, $start, $count);
            }
        };
        $ids = array();
        for ($i = 0; $i < 3; $i++) {
            $ids[] = $queue->addTask(array('n' => $i));
        }
        $queue->getTask(null, 3);

        $this->assertThrows(\RuntimeException::class, function () use ($queue) {
            $queue->pendingClaim(0, 'c2', 1);
        });
        $this->assertSame(1, $this->pendingFor('c2'));
        $this->assertSame(2, $this->pendingFor('c1'));
        $this->assertSame($ids, array_keys($this->redis->xRange($this->stream, '-', '+')));
        $this->assertSame(3, $this->pendingCount());
    }

    public function testConstructorRejectsDifferentOrAdditionalGroup()
    {
        $this->makeQueue('c1');

        // 同名组可复用
        $reused = $this->makeQueue('c2');
        $this->assertInstanceOf(RedisQueue::class, $reused);

        $this->assertThrows(\RuntimeException::class, function () {
            new RedisQueue(array(
                'stream'   => $this->stream,
                'group'    => 'another-group',
                'consumer' => 'c1',
            ), $this->redis);
        });

        $this->redis->rawCommand('XGROUP', 'CREATE', $this->stream, 'second-group', '0', 'MKSTREAM');

        $this->assertThrows(\RuntimeException::class, function () {
            new RedisQueue(array(
                'stream'   => $this->stream,
                'group'    => $this->group,
                'consumer' => 'c1',
            ), $this->redis);
        });
    }

    public function testSameGroupWithDifferentConsumersWorks()
    {
        $first  = $this->makeQueue('c1');
        $second = $this->makeQueue('c2');

        $firstId  = $first->addTask(array('n' => '1'));
        $secondId = $second->addTask(array('n' => '2'));

        $this->assertArrayHasKey($firstId, $first->getTask(null, 1));
        $this->assertArrayHasKey($secondId, $second->getTask(null, 1));

        $this->assertSame(array(1, 1), $first->delTask(array($firstId)));
        $this->assertSame(array(1, 1), $second->delTask(array($secondId)));
        $this->assertSame(0, (int) $this->redis->xLen($this->stream));
    }

    public function testDestoryStreamRemovesWholeStreamAndGroupState()
    {
        $queue = $this->makeQueue('c1');
        $queue->addTask(array('n' => '1'));
        $queue->addTask(array('n' => '2'));
        $queue->getTask(null, 1);

        $this->assertSame(1, (int) $this->redis->exists($this->stream));

        $queue->destoryStream();

        $this->assertSame(0, (int) $this->redis->exists($this->stream));

        $groups = $this->redis->xInfo('GROUPS', $this->stream);
        $this->assertTrue($groups === false || $groups === array());

        $entries = $this->redis->xRange($this->stream, '-', '+');
        $this->assertTrue($entries === false || $entries === array());
    }

    private function makeQueue($consumer = 'c1', array $extra = array())
    {
        $config = array_merge(array(
            'stream'   => $this->stream,
            'group'    => $this->group,
            'consumer' => $consumer,
        ), $extra);

        return new RedisQueue($config, $this->redis);
    }

    private function setPendingIdle($consumer, array $ids, $idleMs)
    {
        if (count($ids) === 0) {
            return;
        }

        $args = array('XCLAIM', $this->stream, $this->group, $consumer, '0');
        foreach ($ids as $id) {
            $args[] = $id;
        }
        $args[] = 'IDLE';
        $args[] = (string) $idleMs;
        $args[] = 'JUSTID';

        $this->redis->rawCommand(...$args);
    }

    private function pendingCount()
    {
        $info = $this->redis->xPending($this->stream, $this->group);
        return is_array($info) ? (int) $info[0] : 0;
    }

    private function pendingFor($consumer)
    {
        $result = $this->redis->xPending($this->stream, $this->group, '-', '+', 1000, $consumer);
        return is_array($result) ? count($result) : 0;
    }

    private function assertThrows($class, callable $callback)
    {
        try {
            $callback();
        } catch (Throwable $e) {
            $this->assertInstanceOf($class, $e, get_class($e) . ': ' . $e->getMessage());
            return $e;
        }

        $this->fail('expected exception ' . $class . ' was not thrown');
    }

    private function newTempPath()
    {
        $path = sys_get_temp_dir() . '/stream-queue-barrier-' . bin2hex(random_bytes(8));
        $this->tempPaths[] = $path;
        return $path;
    }

    private function spawnFixture($file, array $env)
    {
        $script = __DIR__ . '/fixtures/' . $file;
        $environment = array_merge(array(
            'PATH'            => (string) getenv('PATH'),
            'SQ_TEST_HOST'    => (string) STREAM_QUEUE_TEST_HOST,
            'SQ_TEST_PORT'    => (string) STREAM_QUEUE_TEST_PORT,
            'SQ_TEST_STREAM'  => $this->stream,
            'SQ_TEST_GROUP'   => $this->group,
        ), $env);

        $descriptors = array(
            0 => array('pipe', 'r'),
            1 => array('pipe', 'w'),
            2 => array('pipe', 'w'),
        );

        $process = proc_open(array(PHP_BINARY, $script), $descriptors, $pipes, null, $environment);
        if (!is_resource($process)) {
            $this->fail('cannot start fixture process: ' . $script);
        }

        fclose($pipes[0]);
        return array($process, $pipes);
    }

    private function waitProcess($process, array $pipes, $timeout = 10.0)
    {
        $stdout   = '';
        $stderr   = '';
        $exitCode = -1;
        $deadline = microtime(true) + $timeout;

        stream_set_blocking($pipes[1], false);
        stream_set_blocking($pipes[2], false);

        while (true) {
            $stdout .= (string) stream_get_contents($pipes[1]);
            $stderr .= (string) stream_get_contents($pipes[2]);

            $status = proc_get_status($process);
            if (!$status['running']) {
                $exitCode = $status['exitcode'];
                break;
            }

            if (microtime(true) > $deadline) {
                proc_terminate($process, 9);
                break;
            }

            usleep(10000);
        }

        $stdout .= (string) stream_get_contents($pipes[1]);
        $stderr .= (string) stream_get_contents($pipes[2]);

        fclose($pipes[1]);
        fclose($pipes[2]);
        proc_close($process);

        return array('exitcode' => $exitCode, 'stdout' => $stdout, 'stderr' => $stderr);
    }

    private function jsonFromOutput($output)
    {
        $lines = array_values(array_filter(array_map('trim', explode("\n", $output)), 'strlen'));
        $this->assertNotEmpty($lines, 'fixture produced no output: ' . $output);

        $data = json_decode(end($lines), true);
        $this->assertIsArray($data, 'fixture output is not JSON: ' . $output);

        return $data;
    }
}
