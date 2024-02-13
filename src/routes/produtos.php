<?php

use App\Models\Produto;
use Slim\Http\Request;
use Slim\Http\Response;

// Routes


$app->group('/api/v1', function () {

    $this->get('/produtos/lista', function ($request, $response) {

        $produtos = Produto::get();
        return $response->withJson($produtos);
    });

    $this->post('/produtos/adiciona', function ($request, $response) {

        $dados = $request->getParsedBody();

        //Validaçoes

        $produto = Produto::create($dados);
        return $response->withjson($produto);
    });

    $this->get('/produtos/lista/{id}', function ($request, $response, $args) {



        $produto = Produto::findOrFail($args['id']);

        $titulo = $produto['titulo'];


        return $response->withJson($produto);
    });

    $this->put('/produtos/atualiza/{id}', function ($request, $response, $args) {

        $dados = $request->getParsedBody();

        $produto = Produto::findOrFail($args['id']);
        $produto->update($dados);
        return $response->withJson($produto);
    });
    $this->get('/produtos/remove/{id}', function ($request, $response, $args) {


        $produto = Produto::findOrFail($args['id']);
        $produto->delete();
        return $response->withJson($produto);
    });






    $this->get('/produtos/adicionaviaurl/{titulo}/{descricao}/{preco}/{fabricante}', function ($request, $response, $args) {

        $dados = $args;

        $produto = Produto::create([
            'titulo' => $dados['titulo'],
            'descricao' => $dados['descricao'],
            'preco' => $dados['preco'],
            'fabricante' => $dados['fabricante']
        ]);

        // var_dump($dados);

        return $response->withJson($produto);
    });

    $this->get('/produtos/atualizar/{id}/{titulo}/{descricao}/{preco}/{fabricante}', function ($request, $response, $args) {

        $dados = $args;
        $id = $dados['id'];

        // Encontrar o produto pelo ID
        $produto = Produto::findOrFail($id);

        // Atualizar os atributos do produto
        $produto->update([
            'titulo' => $dados['titulo'],
            'descricao' => $dados['descricao'],
            'preco' => $dados['preco'],
            'fabricante' => $dados['fabricante']
        ]);

        return $response->withJson($produto);
    });
});


