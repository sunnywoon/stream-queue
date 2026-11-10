<?php

require __DIR__ . '/queue_bootstrap.php';

try {
    $queue = sq_test_queue();
    sq_test_wait_barrier();

    $id = $queue->addTask(array('worker' => (string) sq_test_env('SQ_TEST_WORKER', 'unknown')));
    sq_test_json(array('id' => $id));
    exit(0);
} catch (OverflowException $e) {
    sq_test_json(array('overflow' => true, 'message' => $e->getMessage()));
    exit(2);
} catch (Throwable $e) {
    fwrite(STDERR, get_class($e) . ': ' . $e->getMessage() . "\n");
    exit(1);
}
