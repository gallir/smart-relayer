<?PHP

$redis = new Redis();
$redis->connect('127.0.0.1', 6389);

$real_redis = new Redis();
$real_redis->connect('127.0.0.1', 6379);
//$cli = phpiredis_connect("localhost", 6389);
//$cli = phpiredis_connect("192.168.111.2", 6379);

$value = randString(10);

$start = microtime(True);
for ($i = 0; $i<1000; $i++) {
    $key = randString(32);

    $response = $redis->ping();
    if ($response != "+PONG" && $response != "+OK") {
        print("Error in PING $response\n");
        exit(1);
    }

/*
    $response = $redis->select(rand(1,15));
    if ($response === False && $response != "+OK") {
        print("Error in SELECT $response\n");
        exit(1);
    }
*/

    $response = $redis->select(0);
    $response = $redis->set($key, $value, 600);
    if ($response === False) {
        printf("Error in SET %s %s\n", $response, $key);
        exit(1);
    }


    //$response = $redis->del($key); // Return false (:1)

    $response = $redis->select(2);
    if ($response === False && $response != "+OK") {
        print("Error in SELECT $response\n");
        exit(1);
    }

    $response = $redis->hSet("ROW", $key, $value); // Return false (:1)
    //usleep(100);
    //$response = $real_redis->hGet("ROW", $key);
    //var_dump($response);

    $response = $redis->hDel("ROW", $key); // Return false (:1)


    $response = $redis->select(0);
    $response = $redis->get($key);
    if ($response != $value) {
        print("Error in get $key\n");
        var_dump($response);

    }


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
