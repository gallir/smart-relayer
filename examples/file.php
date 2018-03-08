<?PHP

if (!isset($argv[1]) || !isset($argv[2]) || !is_numeric($argv[1]) ) {
    printf("Usage: %s [records] [type]\n",basename(__FILE__));
    print("- records: total messages will be send to file\n");
    print("- type:\n");
    print("\traw: json message\n");
    print("\tmulti: send multiple redis commands (MULTI/EXEC) in one transaction (message)\n");
    print("\tset: simple redis SET command\n");
    die();
}

$limit = intval($argv[1]);
$method = $argv[2];

$start = microtime(True);
for ($c = 1; $c<=$limit; $c++) {

    $cli = phpiredis_connect('/tmp/file.sock');

    $response = phpiredis_command_bs($cli, array("PING"));
    if ($response != "PONG" && $response != "OK") {
        print("Error in PING $response\n");
    }

    switch ($method) {
        default:
            // SET
            $t = time();
            $value = json_encode(array("a"=>"text to check", "rand"=>randString(rand(100*1024,200*1024))));
            $key = $t."-".sha1($value);
            $response = phpiredis_command_bs($cli, array("SET", "testing", $key, $t, $value));
            printf("R: %s\n", $response);
            if (substr($response, 0, 3) == "ERR") {
                printf("Error in SET %s\n", $response);
                exit(1);
            }
            usleep(500);
            break;
    }

}
$elapsed = microtime(True)-$start;
printf("Elapsed %.2f\n", $elapsed);


function randString($maxLen, $characters = ' 0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ') {
    $randomString = '';
    $len = strlen($characters);
    for ($i = 0; $i < $maxLen; $i++) {
        $randomString .= $characters[rand(0, $len - 1)];
    }
    return $randomString;
}
