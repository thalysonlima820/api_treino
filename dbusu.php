<?php
if (PHP_SAPI != 'cli') {
    exit('rodar via cli');
}

require __DIR__ . '/vendor/autoload.php';



// Instantiate the app
$settings = require __DIR__ . '/src/settings.php';
$app = new \Slim\App($settings);


$dependencies = require __DIR__ . '/src/dependencies.php';

$db = $container->get('db');

$schema = $db->schema();
$tabela = 'usuarios';

$schema->dropIfExists( $tabela );

// Cria a tabela produtos
$schema->create($tabela, function($table){
	
	$table->increments('id');
	$table->string('nome', 100);
	$table->string('email');
	$table->string('senha');
	$table->string('token');
    $table->timestamps();

});


