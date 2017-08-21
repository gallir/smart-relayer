<?PHP

if (!isset($argv[1]) || !isset($argv[2]) || !is_numeric($argv[1]) ) {
    printf("Usage: %s [records] [type]\n",basename(__FILE__));
    print("- records: total messages will be send to kinesis firehose\n");
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

    $cli = phpiredis_connect('/tmp/firehose.sock');

    $response = phpiredis_command_bs($cli, array("PING"));
    if ($response != "PONG" && $response != "OK") {
        print("Error in PING $response\n");
    }

    switch ($method) {
        case "raw":

            $data = array();
            for ($sadd = 0; $sadd < 5; $sadd++) { 
                $data["list"] = randString(10);
            }
            
            for ($sadd = 0; $sadd < 5; $sadd++) { 
                $data["dict"][ randString(5) ] = randString(10);
            }

            $json = json_encode(array('_ts'=>microtime(true), 'data'=>$data));
            $response = phpiredis_command_bs($cli, array("RAWSET", $json));
            if ($response != "OK" && ! (is_integer($response) && $response >= 0) ) {
                printf("Error in HMSET %s\n", $response);
                exit(1);
            }

            break;

        case "multi":

            $response = phpiredis_command_bs($cli, array("MULTI"));
            if ($response != "OK" && ! (is_integer($response) && $response >= 0) ) {
                printf("Error in MULTI %s\n", $response);
                exit(1);
            }

            // SET
            $key = randString(5);
            $value = randString(10);
            $response = phpiredis_command_bs($cli, array("SET", $key, $value));
            if ($response != "OK") {
                printf("Error in SET %s\n", $response);
                exit(1);
            }

            // SADD
            $key = "list".randString(5);
            for($i=0; $i<5; $i++) {
                $response = phpiredis_command_bs($cli, array("SADD", $key, randString(10)));
                if ($response != "OK" && ! (is_integer($response) && $response >= 0) ) {
                    printf("Error in SADD %s\n", $response);
                    exit(1);
                }
            }

            // HMSET
            $key = "dict".randString(5);
            for($i=0; $i<5; $i++) {
                $_command = array("HMSET", $key, randString(5), randString(10));
                $response = phpiredis_command_bs($cli,  $_command);
                if ($response != "OK" && ! (is_integer($response) && $response >= 0) ) {
                    printf("Error in HMSET %s\n", $response);
                    exit(1);
                }
            }

            $response = phpiredis_command_bs($cli, array("EXEC"));
            if ($response != "OK" && ! (is_integer($response) && $response >= 0) ) {
                printf("Error in EXEC %s\n", $response);
                exit(1);
            }

            break;

        default:
            // SET
            $key = randString(5);
            $value = randString(10);
            $response = phpiredis_command_bs($cli, array("SET", $key, $value));
            if ($response != "OK") {
                printf("Error in SET %s\n", $response);
                exit(1);
            }
            break;
    }

}
$elapsed = microtime(True)-$start;
printf("Elapsed %.2f\n", $elapsed);


function randString($maxLen, $characters = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ') {
    $randomString = '';
    $len = strlen($characters);
    for ($i = 0; $i < $maxLen; $i++) {
        $randomString .= $characters[rand(0, $len - 1)];
    }
    return $randomString;
}
