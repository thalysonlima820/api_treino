<?php

use Slim\Http\Request;
use Slim\Http\Response;

use Symfony\Component\Console\Descriptor\Descriptor;

// Rota para listar os dados de PCFILIAL
$app->group('/v1', function () {

    $this->get('/preco/{codprod}', function (Request $request, Response $response) {
        $codprod = $request->getAttribute('codprod');
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
    
        // Consulta SQL
        $sql = "WITH OFERTAS AS (
                    SELECT 
                        CASE 
                            WHEN 'N' = 'S' THEN PCPRODUT.DESCRICAO 
                            ELSE NVL(PCEMBALAGEM.DESCRICAOECF, PCPRODUT.DESCRICAO) 
                        END AS DESCRICAO,
                        PCPRODUT.CODPROD,
                        PCPRODUT.DESCRICAO AS DESCRICAO_PRODUT,
                        PCEMBALAGEM.CODAUXILIAR,
                        PCOFERTAPROGRAMADAI.VLOFERTA,
                        PCOFERTAPROGRAMADAI.CODOFERTA,
                        PCPRODUT.DIRETORIOFOTOS
                    FROM PCEST
                    INNER JOIN PCPRODUT ON PCEST.CODPROD = PCPRODUT.CODPROD
                    INNER JOIN PCOFERTAPROGRAMADAI ON PCEST.CODFILIAL = PCOFERTAPROGRAMADAI.CODFILIAL
                    INNER JOIN PCEMBALAGEM ON PCOFERTAPROGRAMADAI.CODAUXILIAR = PCEMBALAGEM.CODAUXILIAR
                        AND PCOFERTAPROGRAMADAI.CODFILIAL = PCEMBALAGEM.CODFILIAL
                        AND PCEST.CODPROD = PCEMBALAGEM.CODPROD
                        AND PCEST.CODFILIAL = PCEMBALAGEM.CODFILIAL
                    WHERE PCOFERTAPROGRAMADAI.CODFILIAL = '1'
                ),
                CODOFERTAS AS (
                    SELECT 
                        PCOFERTAPROGRAMADAC.CODOFERTA,
                        PCOFERTAPROGRAMADAC.DTINICIAL,
                        PCOFERTAPROGRAMADAC.DTFINAL
                    FROM PCOFERTAPROGRAMADAC
                    WHERE PCOFERTAPROGRAMADAC.CODFILIAL = '1'
                    AND PCOFERTAPROGRAMADAC.DTCANCEL IS NULL
                    AND TRUNC(SYSDATE) BETWEEN PCOFERTAPROGRAMADAC.DTINICIAL AND PCOFERTAPROGRAMADAC.DTFINAL
                )
                -- Primeira tentativa: verificar ofertas
                SELECT 
                    O.DESCRICAO_PRODUT AS PRODUTO,
                    O.CODPROD AS CODPRODUTO,
                    O.VLOFERTA AS VALOR,
                    O.CODAUXILIAR AS CODBARRA
                    
                FROM OFERTAS O
                INNER JOIN CODOFERTAS C ON O.CODOFERTA = C.CODOFERTA
                WHERE O.CODPROD = :codprod
    
                UNION ALL
    
                -- Segunda tentativa: se a oferta não for encontrada, buscar PTABELA
                SELECT 
                    P.DESCRICAO AS PRODUTO,
                    E.CODPROD AS CODPRODUTO,
                    E.PTABELA AS VALOR,
                    E.CODAUXILIAR AS CODBARRA
                FROM PCEMBALAGEM E, PCPRODUT P
                WHERE E.CODPROD = P.CODPROD
                AND E.CODPROD = :codprod
                AND E.CODFILIAL = 1
                AND NOT EXISTS (
                    SELECT 1
                    FROM OFERTAS O
                    INNER JOIN CODOFERTAS C ON O.CODOFERTA = C.CODOFERTA
                    WHERE O.CODPROD = :codprod
                )";
    
        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            // Log de erro na preparação da consulta SQL
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }
    
        // Vincular parâmetro :codprod
        oci_bind_by_name($stmt, ':codprod', $codprod);
    
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
            $filiais[] = $row;
        }
    
        // Fechar a conexão
        oci_free_statement($stmt);
        oci_close($conexao);
    
        // Verifica se há resultados
        if (empty($filiais)) {
            // Log de retorno vazio com o produto pesquisado
            $this->logger->info("Nenhum resultado encontrado para o produto CODPROD = $codprod.");
            return $response->withJson(['message' => 'Nenhum dado encontrado'], 404);
        }
    
        // Convertendo para UTF-8 os resultados
        foreach ($filiais as &$filial) {
            array_walk_recursive($filial, function (&$item) {
                if (!mb_detect_encoding($item, 'utf-8', true)) {
                    $item = utf8_encode($item);
                }
            });
        }
    
        // Log de sucesso
        $this->logger->info("Consulta executada com sucesso para o produto CODPROD = $codprod.");
    
        // Retornar resultados em JSON
        return $response->withJson($filiais);
    });
    
   
});
