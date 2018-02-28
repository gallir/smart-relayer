<?php

$dir = "/tmp/test";

$cli = phpiredis_connect('/tmp/file.sock');
$limit = 50;

$response = phpiredis_command_bs($cli, array("PING"));
if ($response != "PONG" && $response != "OK") {
    print("Error in PING $response\n");
}


// (
//     [0] => /tmp/test/testing/2018/01/03/18/11/ff08c0b2acccebd25801857eba1572a9a653abc4.log
//     [1] => testing
//     [2] => 2018
//     [3] => 01
//     [4] => 03
//     [5] => 18
//     [6] => 11
//     [7] => ff08c0b2acccebd25801857eba1572a9a653abc4
// )

foreach(glob($dir."/*/*/*/*/*/*/*.log*") as $file) {
    if (preg_match("/.*\/(\w+)\/(\d+)\/(\d+)\/(\d+)\/(\d+)\/(\d+)\/([^\.]+)\.log(\.gz)*$/", $file, $r)) {

        $original = file_get_contents($file);
        if (preg_match("/\.gz$/", $file)) {
            $original = gzdecode($original);
        }
        $ol = mb_strlen($original);
        $t = mktime($r[5], $r[6], 0, $r[3], $r[4], $r[2]);
        $project = $r[1];
        $key = $r[7];

        

        $cmd = array("GET", $project, $key, $t);
        //printf("C: %s\n", join(" ", $cmd));
        $startTime = microtime(True);
        $response = phpiredis_command_bs($cli, $cmd);
        $elapsed = microtime(true)-$startTime;
        $rl = mb_strlen($response);

        if ($response == $original) {
            continue;
        }

        printf("GET %s %s %d", $project, $key, $t);
        printf(" -- [%d] elapsed %0.3f OK\n", $rl, $elapsed);

        print("\n");
        printf("\tO [%d]: %s...%s\n", $ol, mb_substr($original, 0, $limit), mb_substr($original, $ol-$limit, $ol));
        printf("\tR [%d]: %s...%s\n", $rl, mb_substr($response, 0, $limit), mb_substr($response, $rl-$limit, $rl));
        
    }
}