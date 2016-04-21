<?PHP

$cli = phpiredis_connect("localhost", 6389);

var_dump($cli);

$key = "KEY__1";
$value = base64_encode(serialize(array(1, 2, 4, 5, 6, 7, 2, 23 ,22)));

phpiredis_command_bs($cli, array("SET", $key, $value, "PX", "2000"));
// phpiredis_command_bs($cli, array("HSET", "KKK", $key, $value));
$command = "HSET {ROW} {$key} ";
$command .= '"' . base64_encode(serialize($value)) . '"';
// phpiredis_command($cli, $command);
