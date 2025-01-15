<?php

use Slim\Http\Request;
use Slim\Http\Response;

$app->options('/{routes:.+}', function ($request, $response, $args) {
    return $response;
});

// Routes
require __DIR__ . '/routes/autenticacao.php';
require __DIR__ . '/routes/precificacao.php';
require __DIR__ . '/routes/etiqueta.php';
require __DIR__ . '/routes/metas.php';
require __DIR__ . '/routes/usuarios.php';
require __DIR__ . '/routes/financeiro.php';
require __DIR__ . '/routes/estoque.php';
require __DIR__ . '/routes/dre.php';
require __DIR__ . '/routes/Limite.php';
require __DIR__ . '/routes/Transferencia.php';
require __DIR__ . '/routes/Log.php';
require __DIR__ . '/routes/Prazo.php';
require __DIR__ . '/routes/Notificacao.php';
require __DIR__ . '/routes/Agenda.php';
require __DIR__ . '/routes/Concorrente.php';
require __DIR__ . '/routes/EntradaSemVenda.php';




// Catch-all route to serve a 404 Not Found page if none of the routes match
// NOTE: make sure this route is defined last
$app->map(['GET', 'POST', 'PUT', 'DELETE', 'PATCH'], '/{routes:.+}', function($req, $res) {
    $handler = $this->notFoundHandler; // handle using the default Slim page not found handler
    return $handler($req, $res);
});