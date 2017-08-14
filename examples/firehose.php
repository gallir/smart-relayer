<?PHP



for ($sadd = 0; $sadd <= (int) 15; $sadd++) { 
    $array1[] = randString(20, "1233");
}

for ($sadd = 0; $sadd <= (int) 15; $sadd++) { 
    $hash1[ randString(3,"0ABC") ] = randString(20, '9876 .€$#`'."\n");
}


$limit = intval($argv[1]);
if ($limit < 1) {
    $limit = 1;
}

$case = 1;
if (isset($argv[2]) && $argv[2] == "json") {
    $case = 2;
}

$start = microtime(True);
for ($i = 1; $i<=$limit; $i++) {
    $cli = phpiredis_connect('/tmp/redis.sock');

    $response = phpiredis_command_bs($cli, array("PING"));
    if ($response != "PONG" && $response != "OK") {
        print("Error in PING $response\n");
        exit(1);
    }

    // // SET
    // $key = randString(5,"kkkkkkkkkkkkkkkk");
    // $value = randString(10);
    // $response = phpiredis_command_bs($cli, array("SET", $key, $value));
    // if ($response != "OK") {
    //     printf("Error in SET %s\n", $response);
    //     exit(1);
    // }



    if ($case == 1) {
        /**
        * Transaction - START
        */
        $response = phpiredis_command_bs($cli, array("MULTI"));
        if ($response != "OK" && ! (is_integer($response) && $response >= 0) ) {
            printf("Error in HMSET %s\n", $response);
            exit(1);
        }
        //---------------------------


        // SADD
        $key = "array1";
        foreach($array1 as $value) {
            $response = phpiredis_command_bs($cli, array("SADD", $key, $value));
            if ($response != "OK" && ! (is_integer($response) && $response >= 0) ) {
                printf("Error in SADD %s\n", $response);
                exit(1);
            }
        }


        /**
        * HMSET
        */
        $key = "hash1";
        foreach ($hash1 as $k => $v) {
            $_command = array("HMSET", $key, $k, $v);
            $response = phpiredis_command_bs($cli,  $_command);
        }
        if ($response != "OK" && ! (is_integer($response) && $response >= 0) ) {
            printf("Error in HMSET %s\n", $response);
            exit(1);
        }


        //---------------------------
        $response = phpiredis_command_bs($cli, array("EXEC"));
        if ($response != "OK" && ! (is_integer($response) && $response >= 0) ) {
            printf("Error in HMSET %s\n", $response);
            exit(1);
        }

    } else {

        /**
        * RAW
        */
        $data = array(
            "array1" => $array1,
            "hash1" => $hash1,
        );

        $json = json_encode(array('_ts'=>microtime(true), 'data'=>$data), JSON_UNESCAPED_UNICODE | JSON_HEX_QUOT | JSON_HEX_APOS | JSON_HEX_TAG | JSON_UNESCAPED_UNICODE);
        var_dump($json);

        $response = phpiredis_command_bs($cli, array("RAWSET", $json));
        if ($response != "OK" && ! (is_integer($response) && $response >= 0) ) {
            printf("Error in HMSET %s\n", $response);
            exit(1);
        }
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
