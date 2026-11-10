<?php

require __DIR__ . '/queue_bootstrap.php';

/*
 * 模拟恢复进程在第一次 XAUTOCLAIM 成功后被强制终止：
 * 此时尚未确认，也没有任何回滚或清理逻辑可执行。
 */
class CrashMidScanQueue extends StreamQueue\RedisQueue
{
    protected function xAutoClaim($consumer, $idleTime, $start, $count)
    {
        $result = parent::xAutoClaim($consumer, $idleTime, $start, $count);
        exit(9);
    }
}

try {
    $redis = new Redis();
    $redis->connect(
        sq_test_env('SQ_TEST_HOST', '127.0.0.1'),
        (int) sq_test_env('SQ_TEST_PORT', '6379'),
        1
    );

    $config = array(
        'stream'   => sq_test_env('SQ_TEST_STREAM'),
        'group'    => sq_test_env('SQ_TEST_GROUP', 'g'),
        'consumer' => sq_test_env('SQ_TEST_CONSUMER', 'crashed-midscan'),
    );

    $queue = new CrashMidScanQueue($config, $redis);
    $queue->pendingRestore(
        (int) sq_test_env('SQ_TEST_IDLE', '5000'),
        (int) sq_test_env('SQ_TEST_PERPAGE', '1')
    );

    exit(0);
} catch (Throwable $e) {
    fwrite(STDERR, get_class($e) . ': ' . $e->getMessage() . "\n");
    exit(1);
}
