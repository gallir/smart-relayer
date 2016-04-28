<?PHP

$cli = phpiredis_connect("localhost", 6389);
//$cli = phpiredis_connect("192.168.111.2", 6379);

$value = randString(400000);

$start = microtime(True);
for ($i = 0; $i<1; $i++) {
    $key = randString(32);
    $response = phpiredis_command_bs($cli, array("PING"));
    if ($response != "PONG" && $response != "OK") {
        print("Error in PING $response\n");
        exit(1);
    }
    $response = phpiredis_command_bs($cli, array("SET", $key, $value, "PX", "2000"));
    if ($response != "OK") {
        printf("Error in SET %s\n", $response);
        exit(1);
    }
    $response = phpiredis_command_bs($cli, array("HSET", "ROW", $key, $value));
    if ($response != "OK" && ! (is_integer($response) && $response >= 0) ) {
        printf("Error in HSET %s\n", $response);
        exit(1);
    }
    $response = phpiredis_command_bs($cli, array("HDEL", "ROW", $key));
    if ($response != "OK" && ! (is_integer($response) && $response >= 0) ) {
        printf("Error in HDEL %s\n", $response);
        exit(1);
    }
}
$elapsed = microtime(True)-$start;
printf("Elapsed %.2f\n", $elapsed);
// phpiredis_command_bs($cli, array("HSET", "KKK", $key, $value));
// $command = "HSET {ROW} {$key} ";
// $command .= '"' . base64_encode(serialize($value)) . '"';
// echo phpiredis_command($cli, $command);
// echo "\n";

function randString($maxLen) {
    $characters = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
    $randomString = '';
    $len = strlen($characters);
    for ($i = 0; $i < $maxLen; $i++) {
        $randomString .= $characters[rand(0, $len - 1)];
    }
    return $randomString;
}
