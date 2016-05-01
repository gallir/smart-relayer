<?PHP

$redis = new Redis();
$redis->connect('127.0.0.1', 6389);
//$redis->connect('192.168.0.149', 6379);

$value = randString(40000);

$start = microtime(True);
for ($i = 0; $i<1000; $i++) {
    $key = randString(32);

    $response = $redis->ping();
    if ($response != "+PONG" && $response != "+OK") {
        print("Error in PING $response\n");
        exit(1);
    }

    $response = $redis->set($key, $value, 600);
    if ($response === False) {
        printf("Error in SET %s %s\n", $response, $key);
        exit(1);
    }

    $redis->hSet("ROW", $key, $value);
    $redis->hDel("ROW", $key);

    $redis->del($key, $value);
}

$elapsed = microtime(True)-$start;
printf("Elapsed %.2f\n", $elapsed);


function randString($maxLen) {
    $characters = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
    $randomString = '';
    $len = strlen($characters);
    for ($i = 0; $i < $maxLen; $i++) {
        $randomString .= $characters[rand(0, $len - 1)];
    }
    return $randomString;
}
