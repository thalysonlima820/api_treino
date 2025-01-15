<?php

use Slim\Http\Request;
use Slim\Http\Response;

use Symfony\Component\Console\Descriptor\Descriptor;

// Rota para listar os dados de PCFILIAL
$app->group('/v1', function () {

    $this->get('/notificacao/orcamento', function (Request $request, Response $response) {
        // Pega a conexão do Oracle configurada
        $settings = $this->get('settings')['db'];
        $dsn = $settings['dsn'];
        $username = $settings['username'];
        $password = $settings['password'];

        // Conectando ao Oracle
        $conexao = oci_connect($username, $password, $dsn);

        if (!$conexao) {
            $e = oci_error();
            throw new Exception($e['message']);
        }

        // Comando SQL para consultar os registros com STATUS = 'P'
        $consulta = "SELECT C.CONTA, D.VALORNORMAL AS VALOR, B.NOME AS BANCO, D.MOEDA, D.USUARIO FROM SITERECDESP D, PCCONTA C, PCBANCO B
            WHERE D.CONTA = C.CODCONTA
            AND D.BANCO = B.CODBANCO
            AND D.STATUS = 'P'";

        // Preparando e executando o comando de consulta
        $statement = oci_parse($conexao, $consulta);
        $resultado = oci_execute($statement);

        if ($resultado) {
            $dados = [];
            while ($row = oci_fetch_assoc($statement)) {
                $dados[] = $row;
            }

            // Retorna os resultados em formato JSON
            return $response->withJson($dados, 200);
        } else {
            $e = oci_error($statement);
            return $response->withJson(['error' => $e['message']], 500);
        }

        // Liberando os recursos
        oci_free_statement($statement);
        oci_close($conexao);
    });

});
