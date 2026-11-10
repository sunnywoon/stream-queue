<?php

require dirname(__DIR__, 2) . '/vendor/autoload.php';

function sq_test_env($name, $default = null)
{
    $value = getenv($name);
    if ($value === false || $value === '') {
        return $default;
    }

    return $value;
}

function sq_test_queue()
{
    $redis = new Redis();
    $redis->connect(
        sq_test_env('SQ_TEST_HOST', '127.0.0.1'),
        (int) sq_test_env('SQ_TEST_PORT', '6379'),
        1
    );

    $config = array(
        'stream'   => sq_test_env('SQ_TEST_STREAM'),
        'group'    => sq_test_env('SQ_TEST_GROUP', 'g'),
        'consumer' => sq_test_env('SQ_TEST_CONSUMER', 'fixture'),
    );

    $maxLength = sq_test_env('SQ_TEST_MAXLENGTH');
    if ($maxLength !== null) {
        $config['maxLength'] = (int) $maxLength;
    }

    return new StreamQueue\RedisQueue($config, $redis);
}

function sq_test_wait_barrier()
{
    $startFile = sq_test_env('SQ_TEST_START_FILE');
    if ($startFile === null) {
        return;
    }

    $deadline = microtime(true) + 5.0;
    while (!is_file($startFile)) {
        if (microtime(true) > $deadline) {
            fwrite(STDERR, "barrier timeout\n");
            exit(3);
        }
        usleep(5000);
    }
}

function sq_test_json($data)
{
    echo json_encode($data), "\n";
}
