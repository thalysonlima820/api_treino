<?php

use Slim\Http\Request;
use Slim\Http\Response;

use Symfony\Component\Console\Descriptor\Descriptor;

// Rota para listar os dados de PCFILIAL
$app->group('/api/v1', function () {

    $this->post('/log/preco', function (Request $request, Response $response) {
        // Pega a conexão do Oracle configurada
        $settings = $this->get('settings')['db'];
        $dsn = $settings['dsn'];
        $username = $settings['username'];
        $password = $settings['password'];
    
        // Conectando ao Oracle
        $conexao = oci_connect($username, $password, $dsn);
    
        if (!$conexao) {
            $e = oci_error();
            return $response->withJson(['error' => $e['message']], 500);
        }
    
        // Obtém os dados enviados no corpo da requisição POST
        $params = $request->getParsedBody();
        $CODPROD = $params['CODPROD'] ?? null;
        $CODUSU = $params['CODUSU'] ?? null;
        $VALOR = $params['VALOR'] ?? null;
    
        // Valida se os parâmetros necessários foram enviados
        if (!$CODPROD || !$CODUSU || !$VALOR) {
            oci_close($conexao); // Fecha a conexão em caso de erro
            return $response->withJson(['error' => 'Parâmetros inválidos'], 400);
        }
    
        // Query para inserir os dados no log
        $GRAVARlOG = "INSERT INTO SITELOGUPPRECO 
                      (CODPROD, CODUSU, VALOR, DATA)
                      VALUES 
                      (:CODPROD, :CODUSU, :VALOR, TRUNC(SYSDATE))";
    
        // Preparando a consulta
        $baixa = oci_parse($conexao, $GRAVARlOG);
        oci_bind_by_name($baixa, ":CODPROD", $CODPROD);
        oci_bind_by_name($baixa, ":CODUSU", $CODUSU);
        oci_bind_by_name($baixa, ":VALOR", $VALOR);
    
        // Executando a consulta
        $resultbaixa = oci_execute($baixa, OCI_COMMIT_ON_SUCCESS);
    
        // Verifica se a execução foi bem-sucedida
        if ($resultbaixa) {
            $responseData = ['message' => 'Dados inseridos com sucesso'];
            $status = 200;
        } else {
            $e = oci_error($baixa);
            $responseData = ['error' => $e['message']];
            $status = 500;
        }
    
        // Libera os recursos e fecha a conexão
        oci_free_statement($baixa);
        oci_close($conexao);
    
        return $response->withJson($responseData, $status);
    });

    $this->get('/log/usuario/get', function (Request $request, Response $response) {
        try {
            // Configuração do banco de dados
            $settings = $this->get('settings')['db'];
            $dsn = $settings['dsn'];
            $username = $settings['username'];
            $password = $settings['password'];
    
            // Conectando ao Oracle
            $conexao = oci_connect($username, $password, $dsn);
    
            if (!$conexao) {
                $e = oci_error();
                return $response->withJson(['error' => 'Falha na conexão com o banco de dados.'], 500);
            }
    
            // Comando SQL para consultar os registros
            $consulta = "SELECT CODUSUARIO, NOME, ULTIMO_LOGIN, PERMISSAO, IDTELEGRAM FROM SITEUSUARIO";
    
            // Executando a consulta
            $statement = oci_parse($conexao, $consulta);
            if (!oci_execute($statement)) {
                $e = oci_error($statement);
                return $response->withJson(['error' => $e['message']], 500);
            }
    
            // Manipulando os resultados e forçando a codificação UTF-8
            $dados = [];
            while ($row = oci_fetch_assoc($statement)) {
                $row = array_map(function ($value) {
                    return is_string($value) ? utf8_encode($value) : $value;
                }, $row);
                $dados[] = $row;
            }
    
            // Liberando os recursos
            oci_free_statement($statement);
            oci_close($conexao);
    
            // Retornando os dados em JSON
            return $response->withJson($dados, 200);
        } catch (Exception $e) {
            // Captura de exceções
            return $response->withJson(['error' => $e->getMessage()], 500);
        }
    });
    

});
