<?php

use Slim\Http\Request;
use Slim\Http\Response;

use Symfony\Component\Console\Descriptor\Descriptor;

// Rota para listar os dados de PCFILIAL
$app->group('/api/v1', function () {

    $this->get('/margem/porcentagem/get', function (Request $request, Response $response) {
        $settings = $this->get('settings')['db'];
        $dsn = $settings['dsn'];
        $username = $settings['username'];
        $password = $settings['password'];

        // Conectando ao Oracle
        $conexao = oci_connect($username, $password, $dsn);
        if (!$conexao) {
            $e = oci_error();
            // Log de erro no banco de dados
            $this->logger->error("Erro de conexão ao Oracle: " . $e['message']);
            return $response->withJson(['error' => 'Erro de conexão ao banco de dados'], 500);
        }

        $sql = "SELECT * FROM SITEPRAZOFINANCEIRO WHERE TITLE = 'MARGEM'";

        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            // Log de erro na preparação da consulta SQL
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        // Executa a consulta
        if (!oci_execute($stmt)) {
            $e = oci_error($stmt);
            // Log de erro na execução da consulta SQL
            $this->logger->error("Erro ao executar a consulta SQL: " . $e['message']);
            oci_free_statement($stmt);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro ao executar a consulta SQL'], 500);
        }

        // Coletar os resultados em um array
        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) !== false) {
            $row['DAYS'] = isset($row['DAYS']) ? (float)$row['DAYS'] : null;
            $filiais[] = $row;
        }

        // Fechar a conexão
        oci_free_statement($stmt);
        oci_close($conexao);

        // Convertendo para UTF-8 os resultados
        foreach ($filiais as &$filial) {
            array_walk_recursive($filial, function (&$item) {
                if (!mb_detect_encoding($item, 'utf-8', true)) {
                    $item = utf8_encode($item);
                }
            });
        }

        // Log de sucesso
        $this->logger->info("Consulta executada com sucesso.");

        // Retornar resultados em JSON
        return $response->withJson($filiais);
    });

    $this->get('/prazo/financeiro/get', function (Request $request, Response $response) {
        $settings = $this->get('settings')['db'];
        $dsn = $settings['dsn'];
        $username = $settings['username'];
        $password = $settings['password'];

        // Conectando ao Oracle
        $conexao = oci_connect($username, $password, $dsn);
        if (!$conexao) {
            $e = oci_error();
            // Log de erro no banco de dados
            $this->logger->error("Erro de conexão ao Oracle: " . $e['message']);
            return $response->withJson(['error' => 'Erro de conexão ao banco de dados'], 500);
        }

        $sql = "SELECT * FROM SITEPRAZOFINANCEIRO WHERE TITLE != 'MARGEM'";

        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            // Log de erro na preparação da consulta SQL
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        // Executa a consulta
        if (!oci_execute($stmt)) {
            $e = oci_error($stmt);
            // Log de erro na execução da consulta SQL
            $this->logger->error("Erro ao executar a consulta SQL: " . $e['message']);
            oci_free_statement($stmt);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro ao executar a consulta SQL'], 500);
        }

        // Coletar os resultados em um array
        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) !== false) {
            $row['DAYS'] = isset($row['DAYS']) ? (float)$row['DAYS'] : null;
            $filiais[] = $row;
        }

        // Fechar a conexão
        oci_free_statement($stmt);
        oci_close($conexao);

        // Convertendo para UTF-8 os resultados
        foreach ($filiais as &$filial) {
            array_walk_recursive($filial, function (&$item) {
                if (!mb_detect_encoding($item, 'utf-8', true)) {
                    $item = utf8_encode($item);
                }
            });
        }

        // Log de sucesso
        $this->logger->info("Consulta executada com sucesso.");

        // Retornar resultados em JSON
        return $response->withJson($filiais);
    });

    $this->post('/prazo/financeiro', function (Request $request, Response $response) {
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
        $TITLE = $params['TITLE'] ?? null;
        $DAYS = $params['DAYS'] ?? null;

        // Valida se os parâmetros necessários foram enviados
        if (!$TITLE || !$DAYS ) {
            oci_close($conexao); // Fecha a conexão em caso de erro
            return $response->withJson(['error' => 'Parâmetros inválidos'], 400);
        }

        // Query para inserir os dados no log
        $GRAVARlOG = "UPDATE SITEPRAZOFINANCEIRO SET DAYS = :DAYS WHERE TITLE = :TITLE";

        // Preparando a consulta
        $baixa = oci_parse($conexao, $GRAVARlOG);
        oci_bind_by_name($baixa, ":TITLE", $TITLE);
        oci_bind_by_name($baixa, ":DAYS", $DAYS);

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

    $this->get('/markup/porcentagem/get', function (Request $request, Response $response) {
        $settings = $this->get('settings')['db'];
        $dsn = $settings['dsn'];
        $username = $settings['username'];
        $password = $settings['password'];

        // Conectando ao Oracle
        $conexao = oci_connect($username, $password, $dsn);
        if (!$conexao) {
            $e = oci_error();
            // Log de erro no banco de dados
            $this->logger->error("Erro de conexão ao Oracle: " . $e['message']);
            return $response->withJson(['error' => 'Erro de conexão ao banco de dados'], 500);
        }

        $sql = "SELECT 
                S.CODSEC, 
                MK.MARKUP, 
                MK.CURVA, 
                D.DESCRICAO AS DEPARTAMENTO, 
                S.DESCRICAO AS SECAO
            FROM 
                PCSECAO S
            LEFT JOIN 
                SITEMARKUP MK ON S.CODSEC = MK.CODSEC
            JOIN 
                PCDEPTO D ON D.CODEPTO = S.CODEPTO
            WHERE D.CODEPTO IN (17,18,19,20,21,22,23,24,25,26,27,28,116)
            ORDER BY 
                D.DESCRICAO, MK.CURVA
        ";

        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            // Log de erro na preparação da consulta SQL
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        // Executa a consulta
        if (!oci_execute($stmt)) {
            $e = oci_error($stmt);
            // Log de erro na execução da consulta SQL
            $this->logger->error("Erro ao executar a consulta SQL: " . $e['message']);
            oci_free_statement($stmt);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro ao executar a consulta SQL'], 500);
        }

        // Coletar os resultados em um array
        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) !== false) {
            $row['CODSEC'] = isset($row['CODSEC']) ? (float)$row['CODSEC'] : null;
            $row['MARKUP'] = isset($row['MARKUP']) ? (float)$row['MARKUP'] : null;
            $filiais[] = $row;
        }

        // Fechar a conexão
        oci_free_statement($stmt);
        oci_close($conexao);

        // Convertendo para UTF-8 os resultados
        foreach ($filiais as &$filial) {
            array_walk_recursive($filial, function (&$item) {
                if (!mb_detect_encoding($item, 'utf-8', true)) {
                    $item = utf8_encode($item);
                }
            });
        }

        // Log de sucesso
        $this->logger->info("Consulta executada com sucesso.");

        // Retornar resultados em JSON
        return $response->withJson($filiais);
    });

    $this->post('/markup/porcentagem', function (Request $request, Response $response) {
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
        $MARKUP = $params['MARKUP'] ?? null;
        $CODSEC = $params['CODSEC'] ?? null;

        // Valida se os parâmetros necessários foram enviados
        if (!$MARKUP || !$CODSEC ) {
            oci_close($conexao); // Fecha a conexão em caso de erro
            return $response->withJson(['error' => 'Parâmetros inválidos'], 400);
        }

        // Query para inserir os dados no log
        $GRAVARlOG = "UPDATE SITEPRAZOFINANCEIRO SET MARKUP = :MARKUP WHERE CODSEC = :CODSEC";

        // Preparando a consulta
        $baixa = oci_parse($conexao, $GRAVARlOG);
        oci_bind_by_name($baixa, ":MARKUP", $MARKUP);
        oci_bind_by_name($baixa, ":CODSEC", $CODSEC);

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
});
