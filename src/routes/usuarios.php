<?php

use Slim\Http\Request;
use Slim\Http\Response;

use Symfony\Component\Console\Descriptor\Descriptor;

// Rota para listar os dados de PCFILIAL
$app->group('/v1', function () {


    $this->post('/usuario/insert', function (Request $request, Response $response) {
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
    
        // Obtém os dados enviados no corpo da requisição POST
        $params = $request->getParsedBody();
        $nome = $params['nome'] ?? null;
        $senha = $params['senha'] ?? null;
        $token = $params['token'] ?? null;
    
        // Valida se os parâmetros necessários foram enviados
        if (!$nome || !$senha || !$token) {
            return $response->withJson(['error' => 'Parâmetros inválidos. Nome, senha e token são obrigatórios.'], 400);
        }
    
        // Criptografando a senha com MD5
        $senhaHash = md5($senha);
    
        // Verifica se o usuário já existe na tabela SITEUSUARIO
        $sqlSelect = "SELECT COUNT(*) AS TOTAL FROM SITEUSUARIO WHERE NOME = :nome";
        $stmtSelect = oci_parse($conexao, $sqlSelect);
        oci_bind_by_name($stmtSelect, ":nome", $nome);
        oci_execute($stmtSelect);
        $row = oci_fetch_assoc($stmtSelect);
        $total = $row['TOTAL'];
        oci_free_statement($stmtSelect);
    
        if ($total > 0) {
            // Se o usuário já existir, faz o UPDATE
            $sqlUpdate = "UPDATE SITEUSUARIO SET SENHA = :senha, TOKEN = :token WHERE NOME = :nome";
            $stmtUpdate = oci_parse($conexao, $sqlUpdate);
            oci_bind_by_name($stmtUpdate, ":nome", $nome);
            oci_bind_by_name($stmtUpdate, ":senha", $senhaHash);
            oci_bind_by_name($stmtUpdate, ":token", $token);
    
            if (!oci_execute($stmtUpdate)) {
                $e = oci_error($stmtUpdate);
                oci_free_statement($stmtUpdate);
                oci_close($conexao);
                return $response->withJson(['error' => 'Erro ao atualizar usuário: ' . $e['message']], 500);
            }
    
            oci_free_statement($stmtUpdate);
            $message = 'Usuário atualizado com sucesso.';
        } else {
            // Se o usuário não existir, faz o INSERT
            $sqlInsert = "INSERT INTO SITEUSUARIO (NOME, SENHA, TOKEN) VALUES (:nome, :senha, :token)";
            $stmtInsert = oci_parse($conexao, $sqlInsert);
            oci_bind_by_name($stmtInsert, ":nome", $nome);
            oci_bind_by_name($stmtInsert, ":senha", $senhaHash);
            oci_bind_by_name($stmtInsert, ":token", $token);
    
            if (!oci_execute($stmtInsert)) {
                $e = oci_error($stmtInsert);
                oci_free_statement($stmtInsert);
                oci_close($conexao);
                return $response->withJson(['error' => 'Erro ao inserir usuário: ' . $e['message']], 500);
            }
    
            oci_free_statement($stmtInsert);
            $message = 'Usuário inserido com sucesso.';
        }
    
        // Fechar a conexão
        oci_close($conexao);
    
        return $response->withJson(['message' => $message], 200);
    });
    
    


});
