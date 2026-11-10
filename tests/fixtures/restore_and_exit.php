<?php

require __DIR__ . '/queue_bootstrap.php';

try {
    $queue = sq_test_queue();
    sq_test_wait_barrier();

    $count = $queue->pendingRestore(
        (int) sq_test_env('SQ_TEST_IDLE', '5000'),
        (int) sq_test_env('SQ_TEST_PERPAGE', '20')
    );

    sq_test_json(array('count' => $count));
    exit((int) sq_test_env('SQ_TEST_EXIT_CODE', '0'));
} catch (Throwable $e) {
    fwrite(STDERR, get_class($e) . ': ' . $e->getMessage() . "\n");
    exit(1);
}
