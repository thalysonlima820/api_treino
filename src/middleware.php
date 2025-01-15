<?php


use Tuupola\Middleware\JwtAuthentication;

// Middleware JWT
$app->add(new JwtAuthentication([
    "header" => "BIASIAMD", // O nome do header personalizado onde o token será enviado
    "regexp" => "/(.*)/", // Regex para capturar o token
    "path" => ["/api"], // Caminhos protegidos
    "ignore" => ["/api/token"], // Caminhos não protegidos
    "secret" => $container->get('settings')['secretKey'], // Chave secreta para assinar/verificar o token
    "algorithm" => ["HS256"], // Algoritmo usado para assinar/verificar o token
    "secure" => false, // Permite uso sobre HTTP (não seguro). Lembre-se de alterar para "true" em produção
    "attribute" => "jwt", // Onde armazenar os dados decodificados do token
    "error" => function ($response, $arguments) {
        $data = array(
            "status" => "erro",
            "mensagem" => $arguments["message"]
        );
        return $response->withJson($data, 401);
    }
]));

// Middleware para CORS
$app->add(function ($req, $res, $next) {
    $response = $next($req, $res);

    return $response
        ->withHeader('Access-Control-Allow-Origin', '*')
        ->withHeader('Access-Control-Allow-Headers', 'X-Requested-With, Content-Type, Accept, Origin, Authorization, BIASIAMD')
        ->withHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
});
