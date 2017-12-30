<?php

$dir = "/tmp/test";

$cli = phpiredis_connect('/tmp/file.sock');
$limit = 50;

$response = phpiredis_command_bs($cli, array("PING"));
if ($response != "PONG" && $response != "OK") {
    print("Error in PING $response\n");
}

foreach(glob($dir."/*/*/*/*/*/*/*.log") as $file) {
    if (preg_match("/.*\/([^\.\-]+)-([^\.\-]+)\.log$/", $file, $r)) {

        $original = file_get_contents($file);
        $ol = mb_strlen($original);
        printf("STGET %s %s", $r[1], $r[2]);
        

        $startTime = microtime(True);
        $response = phpiredis_command_bs($cli, array("STGET", $r[1], $r[2]));
        $elapsed = microtime(true)-$startTime;
        $rl = mb_strlen($response);

        if ($response == $original) {
            printf(" -- [%d] elapsed %0.3f OK\n", $rl, $elapsed);
            continue;
        }

        print("\n");
        printf("\tO [%d]: %s...%s\n", $ol, mb_substr($original, 0, $limit), mb_substr($original, $ol-$limit, $ol));
        printf("\tR [%d]: %s...%s\n", $rl, mb_substr($response, 0, $limit), mb_substr($response, $rl-$limit, $rl));
        
    }
}