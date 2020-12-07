<?php

if(!isset($_GET["execute"])){
	exit(json_encode(array("status"=>0)));
}

$params = explode("/",$_GET["execute"]);

$table = $params[0];
if(isset($params[1]))$offset = $params[1];
if(isset($params[2]))$limit = $params[2];

//check $table format
if(!preg_match('/^[a-zA-Z]+[a-zA-Z0-9_]+$/', $table)){
	exit(json_encode(array("status"=>0)));
}

$conn = pg_connect("host=localhost dbname=dbbappenas user=postgres password=1234567890");

$ret = pg_prepare($conn, "", "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '$table' ; ");
$ret = pg_execute($conn, "", array());

$col = array();
while($row = pg_fetch_assoc($ret)){
	$col[] = $row["column_name"];
}

$ret = pg_prepare($conn, "", "SELECT * FROM $table OFFSET $1 LIMIT $2");
$ret = pg_execute($conn, "", array($offset, $limit));

$data = array($col);
while($row = pg_fetch_row($ret)){
	$data[] = $row;
}

exit(json_encode($data));
