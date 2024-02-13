<?php

use App\Models\Usuario;
use Slim\Http\Request;
use Slim\Http\Response;

// Routes


$app->group('/v1', function(){

    $this->get('/usuarios/lista', function( $request, $response ){

        $usuario = Usuario::get();

        return $response->withJson($usuario);

    });
    $this->get('/usuarios/lista/{id}', function( $request, $response, $args){

        $usuario = Usuario::findOrFail($args['id']);

        return $response->withJson($usuario);
    });
    $this->post('/usuarios/adicionar', function( $request, $response, $args){

        $dados = $request->getParsedBody();

        $senhamd5 = md5($dados['senha']);

        $dados['senha'] = $senhamd5;

        $usuario = Usuario::create($dados);
        return $response->withJson($usuario);

    });

    $this->get('/usuarios/adicionar/{nome}/{email}/{senha}', function( $request, $response, $args){

        $dados = $args;

        $senhaMd5 = md5($dados['senha']);
        
        $usuario = Usuario::create([
            'nome' => $dados['nome'],
            'email' => $dados['email'],
            'senha' => $senhaMd5
        ]);

        return $response->withJson($usuario);
    });

    $this->get('/usuarios/remover/{id}', function( $request, $response, $args){



        $usuario = Usuario::findOrFail($args['id']);
        $usuario->delete();
        return $response->withJson($usuario);
    });
    

    //adicionar token
    $this->put('/usuarios/atualiza/{id}', function( $request, $response, $args){

        $dados = $request->getParsedBody();

        $senhamd5 = md5($dados['senha']);

        $dados['senha'] = $senhamd5;

        $produto = Usuario::findOrFail( $args['id'] );
        $produto->update ($dados);
        return $response->withJson( $produto );

    });
    

});