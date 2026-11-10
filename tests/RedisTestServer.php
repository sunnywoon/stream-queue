<?php

/*
 * 测试用 Redis 生命周期管理。
 *
 * 默认在 127.0.0.1 的空闲端口启动一个隔离的 redis-server，
 * 使用临时目录、关闭持久化，并在测试进程退出时关闭。
 * 如果设置了 REDIS_HOST / REDIS_PORT，则复用外部实例，
 * 不启动也不关闭外部服务，只使用调用方指定的端口。
 */
final class RedisTestServer
{
    private static $process = null;
    private static $pipes = array();
    private static $directory = null;
    private static $host = '127.0.0.1';
    private static $port = 0;
    private static $managed = false;

    public static function start()
    {
        $envHost = getenv('REDIS_HOST');
        $envPort = getenv('REDIS_PORT');

        if ($envHost !== false && $envHost !== '' && $envPort !== false && $envPort !== '') {
            if (!self::ping($envHost, (int) $envPort)) {
                throw new RuntimeException(sprintf(
                    'cannot connect to external Redis at %s:%d (REDIS_HOST/REDIS_PORT)',
                    $envHost,
                    (int) $envPort
                ));
            }

            self::$host    = $envHost;
            self::$port    = (int) $envPort;
            self::$managed = false;
            return;
        }

        $binary    = getenv('REDIS_SERVER_BIN') ?: 'redis-server';
        $lastError = 'unknown error';

        for ($attempt = 0; $attempt < 5; $attempt++) {
            $port      = self::findFreePort();
            $directory = sys_get_temp_dir() . '/stream-queue-test-' . bin2hex(random_bytes(6));

            if (!mkdir($directory, 0700, true) && !is_dir($directory)) {
                throw new RuntimeException('cannot create temporary Redis directory: ' . $directory);
            }

            $logFile    = $directory . '/redis.log';
            $command    = array(
                $binary,
                '--bind', '127.0.0.1',
                '--port', (string) $port,
                '--save', '',
                '--appendonly', 'no',
                '--daemonize', 'no',
                '--protected-mode', 'no',
                '--dir', $directory,
                '--logfile', $logFile,
            );
            $descriptors = array(
                0 => array('pipe', 'r'),
                1 => array('file', $directory . '/stdout.log', 'a'),
                2 => array('file', $directory . '/stderr.log', 'a'),
            );

            $process = @proc_open($command, $descriptors, $pipes);

            if (!is_resource($process)) {
                $lastError = sprintf('proc_open failed for "%s"', $binary);
                self::removeDirectory($directory);
                continue;
            }

            fclose($pipes[0]);

            if (self::waitUntilReady('127.0.0.1', $port, $process, 5.0)) {
                self::$process   = $process;
                self::$pipes     = $pipes;
                self::$directory = $directory;
                self::$host      = '127.0.0.1';
                self::$port      = $port;
                self::$managed   = true;

                register_shutdown_function(array('RedisTestServer', 'stop'));
                return;
            }

            $lastError = self::readLog($logFile);
            self::terminate($process);
            self::removeDirectory($directory);
        }

        throw new RuntimeException('unable to start redis-server: ' . $lastError);
    }

    public static function stop()
    {
        if (!self::$managed || !is_resource(self::$process)) {
            return;
        }

        self::terminate(self::$process);
        self::$process = null;

        foreach (self::$pipes as $pipe) {
            if (is_resource($pipe)) {
                fclose($pipe);
            }
        }
        self::$pipes = array();

        if (self::$directory !== null) {
            self::removeDirectory(self::$directory);
            self::$directory = null;
        }
    }

    public static function host()
    {
        return self::$host;
    }

    public static function port()
    {
        return self::$port;
    }

    public static function managed()
    {
        return self::$managed;
    }

    private static function findFreePort()
    {
        $socket = @stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);

        if (!is_resource($socket)) {
            throw new RuntimeException('cannot allocate a free local port: ' . $errstr);
        }

        $name = stream_socket_get_name($socket, false);
        fclose($socket);

        $position = strrpos($name, ':');
        if ($position === false) {
            throw new RuntimeException('cannot determine allocated local port from: ' . $name);
        }

        return (int) substr($name, $position + 1);
    }

    private static function waitUntilReady($host, $port, $process, $timeout)
    {
        $deadline = microtime(true) + $timeout;

        do {
            $status = proc_get_status($process);
            if (!$status['running']) {
                return false;
            }

            if (self::ping($host, $port)) {
                return true;
            }

            usleep(50000);
        } while (microtime(true) < $deadline);

        return false;
    }

    private static function ping($host, $port)
    {
        try {
            $redis = new Redis();
            if (!@$redis->connect($host, (int) $port, 0.2)) {
                return false;
            }

            $reply = $redis->ping();
            $redis->close();

            return $reply === true || $reply === '+PONG' || $reply === 'PONG';
        } catch (Throwable $e) {
            return false;
        }
    }

    private static function terminate($process)
    {
        $status = proc_get_status($process);
        if ($status['running']) {
            proc_terminate($process);
        }

        $deadline = microtime(true) + 3.0;
        do {
            $status = proc_get_status($process);
            if (!$status['running']) {
                break;
            }
            usleep(20000);
        } while (microtime(true) < $deadline);

        if ($status['running']) {
            proc_terminate($process, 9);
        }

        @proc_close($process);
    }

    private static function readLog($file)
    {
        if (!is_file($file)) {
            return 'redis-server exited before becoming ready';
        }

        $contents = (string) file_get_contents($file);
        return $contents === '' ? 'redis-server exited before becoming ready' : trim($contents);
    }

    private static function removeDirectory($directory)
    {
        if (!is_dir($directory)) {
            return;
        }

        foreach (glob($directory . '/*') ?: array() as $file) {
            if (is_dir($file)) {
                self::removeDirectory($file);
            } else {
                @unlink($file);
            }
        }

        @rmdir($directory);
    }
}
