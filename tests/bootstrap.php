<?php

require dirname(__DIR__) . '/vendor/autoload.php';
require __DIR__ . '/RedisTestServer.php';

RedisTestServer::start();

define('STREAM_QUEUE_TEST_HOST', RedisTestServer::host());
define('STREAM_QUEUE_TEST_PORT', RedisTestServer::port());
