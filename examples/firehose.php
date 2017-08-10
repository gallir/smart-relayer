<?PHP


$start = microtime(True);
for ($i = 0; $i<10; $i++) {
    $cli = phpiredis_connect('/tmp/redis.sock');
    //$cli = phpiredis_connect('mamut.apsl.net', 6379);

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


    // /**
    //  * RAW
    //  */
    // $response = phpiredis_command_bs($cli, array("RAWSET", json_encode(array("test"=>"raw json"))));
    // if ($response != "OK" && ! (is_integer($response) && $response >= 0) ) {
    //     printf("Error in HMSET %s\n", $response);
    //     exit(1);
    // }


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
    $key = randString(5);
    for ($sadd = 0; $sadd <= (int) rand(2,100); $sadd++) { 
        $value = randString(10, "111");
        $response = phpiredis_command_bs($cli, array("SADD", $key, $value));
        if ($response != "OK" && ! (is_integer($response) && $response >= 0) ) {
            printf("Error in SADD %s\n", $response);
            exit(1);
        }
    }


    /**
     * HMSET
     */
    for ($sadd = 0; $sadd <= (int) rand(2,100); $sadd++) { 
        $keys[ randString(3,"0ABC") ] = randString((int) rand(7,10), '1234567890 .$€#@'."\n");
    }

    $_command = array("HMSET", "HMSETKEY");
    $_keys = array_map(
        function ($v, $k) { global $_command;  $_command[] = $k; $_command[] = $v; return $_command; },
        $keys,
        array_keys($keys)
    );

    $response = phpiredis_command_bs($cli,  $_command);
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
    /**
     * Transaction - END
     */


    //sleep(1);
    //usleep(rand(0,1));

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
