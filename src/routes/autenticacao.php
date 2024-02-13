<?php
use App\Models\Produto;
use App\Models\Usuario;
use Slim\Http\Request;
use Slim\Http\Response;

use Firebase\JWT\JWT;

//geraçao de token
$app->post('/api/token', function($request, $response) {

    $dados = $request->getParsedBody();

    $email = $dados['email'] ?? null;
    $senha = $dados['senha'] ?? null;

    $usuario = Usuario::where('email', $email)->first();

    if (!is_null($usuario) && (md5($senha) === $usuario->senha)) {

        // Converter o objeto Usuario para array
        $usuarioArray = $usuario->toArray();

        // gerar token
        $secretKey = $this->get('settings')['secretKey'];
        $chaveAcesso = JWT::encode($usuarioArray, $secretKey, 'HS256');

        return $response->withJson([
            'chave' => $chaveAcesso 
        ]);

    }

    return $response->withJson([
        'status' => 'erro'
    ], 401);

});
