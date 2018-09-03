<?php
require_once 'config.php';

function get_time_by_depcom($start, $stop, $pdo) { //je passe en paramètre de ma fonction l'id de l'article souhaité et l'objet PDO pour exécuter la requête
    $sql = "SELECT \"TPS\" FROM matrice_depcom WHERE \"DEPCOM_START\" = '".$start."' AND \"DEPCOM_STOP\" = '".$stop."' order by \"TPS\" limit 1"; //je réalise ma requête avec l'ID passée en paramètres
    //$sql = "SELECT * from matrice_depcom limit 1";
    $exe = $pdo->query($sql); //j'exécute ma requête
    while($result = $exe->fetch(PDO::FETCH_OBJ)) {
        //$Detail_article = array("Titre" => $result->Titre, "Date" => $result->Date, "Article" => $result->Article);//je mets le résultat de ma requête dans une variable
        $Detail_article = array("TPS" => $result->TPS, "START" => $start, "STOP" => $stop);
    }
    return $Detail_article; //je retourne l'article en question
}

function get_time_by_longlat($start_long, $start_lat, $stop_long, $stop_lat){
    $url = "http://51.254.121.49:5000table/v1/driving/".$start_long.",".$start_lat.";".$stop_long.",".$stop_lat;
    $response = file_get_contents("http://51.254.121.49:5000/table/v1/driving/".$start_long.",".$start_lat.";".$stop_long.",".$stop_lat);
    //$response = file_get_contents("gougle.fr");
    $data = json_decode($response);
    $duration = $data->{'durations'}[1][0];
    return $duration;
    //return $data;
}

$possible_url = array("get_list_articles", "get_time_depcom", "get_time_longlat"); //je définis les URLs valables
$value = "Une erreur est survenue"; //je mets le message d'erreur par défaut dans une variable
if (isset($_GET["action"]) && in_array($_GET["action"], $possible_url)) { //si l'URL est OK
switch ($_GET["action"]) {
case "get_list_articles": $value = get_list_articles($pdo); break; //Je récupère la liste des articles
case "get_time_depcom": if (isset($_GET["start"]) && isset($_GET["stop"])) $value = get_time_by_depcom($_GET["start"], $_GET["stop"], $pdo); //si l'ID est spécifié alors je renvoie l'article en question
case "get_time_longlat": if (isset($_GET["start_long"]) && $_GET["start_lat"] && $_GET["stop_long"] && $_GET["stop_lat"]) $value = get_time_by_longlat($_GET["start_long"], $_GET["start_lat"],$_GET["stop_long"], $_GET["stop_lat"]); //si l'ID est spécifié alors je renvoie l'article en question

else $value = "Argument manquant"; break; } //si l'ID n'est pas valable je change mon message d'erreur
}
exit(json_encode($value)); //je retourne ma réponse en JSON

?>