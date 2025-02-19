<?php

use Slim\Http\Request;
use Slim\Http\Response;

$app->group('/api/v1', function () {

    $this->get('/dre/venda/{ano}/{mes}', function (Request $request, Response $response) {
        $settings = $this->get('settings')['db'];
        $dsn = $settings['dsn'];
        $username = $settings['username'];
        $password = $settings['password'];

        // Conectando ao Oracle
        $conexao = oci_connect($username, $password, $dsn);
        if (!$conexao) {
            $e = oci_error();
            $this->logger->error("Erro de conexão ao Oracle: " . $e['message']);
            return $response->withJson(['error' => 'Erro de conexão ao banco de dados'], 500);
        }

        // Obtém os parâmetros de ano e mês
        $ano = $request->getAttribute('ano');
        $mes = strtoupper($request->getAttribute('mes')); // Converte o mês para maiúsculas (ex: "JAN")

        // Consulta SQL com GROUP BY
        $sql = "SELECT
                    TO_CHAR(M.DTMOV, 'MON', 'NLS_DATE_LANGUAGE = AMERICAN') AS MES,
                    M.CODFILIAL,
                    ROUND(SUM(M.QT * M.CUSTOFIN), 2) AS CUSTO,
                    ROUND(SUM(M.QT * M.PUNIT), 2) AS VENDA,
                    COUNT(DISTINCT M.NUMTRANSVENDA) AS NUMVENDAS,
                    ROUND(SUM(M.QT * M.PUNIT) - SUM(M.QT * M.CUSTOFIN), 2) AS LUCRO,
                    CASE 
                        WHEN SUM(M.QT * M.PUNIT) = 0 THEN 0
                        ELSE ROUND((((SUM(M.QT * M.PUNIT) - SUM(M.QT * M.CUSTOFIN)) * 100) / SUM(M.QT * M.PUNIT)), 2) 
                    END AS MARGEM
                FROM
                    PCMOV M,
                    PCDEPTO D,
                    PCPRODUT P
                WHERE
                    M.CODPROD = P.CODPROD
                    AND P.CODEPTO = D.CODEPTO
                    AND EXTRACT(YEAR FROM M.DTMOV) = :ano
                    AND TO_CHAR(M.DTMOV, 'MON', 'NLS_DATE_LANGUAGE = AMERICAN') = :mes
                    AND M.CODOPER IN ('S', 'SB')
                GROUP BY M.CODFILIAL,
                    TO_CHAR(M.DTMOV, 'MON', 'NLS_DATE_LANGUAGE = AMERICAN')
                    ORDER BY  M.CODFILIAL";

        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        // Associar as variáveis de ano e mês aos placeholders SQL
        oci_bind_by_name($stmt, ":ano", $ano);
        oci_bind_by_name($stmt, ":mes", $mes);

        // Executa a consulta
        if (!oci_execute($stmt)) {
            $e = oci_error($stmt);
            $this->logger->error("Erro ao executar a consulta SQL: " . $e['message']);
            oci_free_statement($stmt);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro ao executar a consulta SQL'], 500);
        }

        // Coletar os resultados em um array
        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) !== false) {
            // Convertendo valores para número
            $filiais[] = [
                'MES' => $row['MES'],
                'CODFILIAL' => (int)$row['CODFILIAL'],
                'CUSTO' => (float)$row['CUSTO'],
                'VENDA' => (float)$row['VENDA'],
                'NUMVENDAS' => (int)$row['NUMVENDAS'],
                'LUCRO' => (float)$row['LUCRO'],
                'MARGEM' => (float)$row['MARGEM']
            ];
        }

        // Fechar a conexão
        oci_free_statement($stmt);
        oci_close($conexao);

        // Verifica se há resultados
        if (empty($filiais)) {
            $this->logger->info("Nenhum resultado encontrado.");
            return $response->withJson(['message' => 'Nenhum dado encontrado'], 404);
        }

        // Log de sucesso
        $this->logger->info("Consulta executada com sucesso.");

        // Retornar resultados em JSON
        return $response->withJson($filiais);
    });

    $this->get('/dre/conta/{ano}/{mes}', function (Request $request, Response $response) {
        $settings = $this->get('settings')['db'];
        $dsn = $settings['dsn'];
        $username = $settings['username'];
        $password = $settings['password'];

        // Conectando ao Oracle
        $conexao = oci_connect($username, $password, $dsn);
        if (!$conexao) {
            $e = oci_error();
            $this->logger->error("Erro de conexão ao Oracle: " . $e['message']);
            return $response->withJson(['error' => 'Erro de conexão ao banco de dados'], 500);
        }

        // Obtém os parâmetros de ano e mês
        $ano = $request->getAttribute('ano');
        $mes = strtoupper($request->getAttribute('mes')); // Converte o mês para maiúsculas (ex: "JAN")

        // Consulta SQL com GROUP BY
        $sql = "WITH MESES AS (
                    SELECT '01' AS MES FROM DUAL UNION ALL
                    SELECT '02' AS MES FROM DUAL UNION ALL
                    SELECT '03' AS MES FROM DUAL UNION ALL
                    SELECT '04' AS MES FROM DUAL UNION ALL
                    SELECT '05' AS MES FROM DUAL UNION ALL
                    SELECT '06' AS MES FROM DUAL UNION ALL
                    SELECT '07' AS MES FROM DUAL UNION ALL
                    SELECT '08' AS MES FROM DUAL UNION ALL
                    SELECT '09' AS MES FROM DUAL UNION ALL
                    SELECT '10' AS MES FROM DUAL UNION ALL
                    SELECT '11' AS MES FROM DUAL UNION ALL
                    SELECT '12' AS MES FROM DUAL
                ),
                VLPREVISTO AS (
                    SELECT 
                        TO_CHAR(PCLANC.DTVENC, 'MM') AS MES,  -- Extrai apenas o mês
                        PCCONTA.GRUPOCONTA, 
                        PCGRUPO.GRUPO, 
                        NVL(SUM(PCLANC.VALOR), 0) * (-1) AS VALOR_PREVISTO  -- Adiciona um alias
                    FROM 
                        PCLANC, 
                        PCNFSAID, 
                        PCCONTA, 
                        PCGRUPO
                    WHERE 
                        PCLANC.NUMTRANSVENDA = PCNFSAID.NUMTRANSVENDA(+)
                        AND PCLANC.CODCONTA = PCCONTA.CODCONTA
                        AND PCCONTA.GRUPOCONTA = PCGRUPO.CODGRUPO
                        AND NVL(PCLANC.CODROTINABAIXA, 0) <> 737
                        AND PCLANC.VALOR <> 0
                        AND NVL(PCNFSAID.CONDVENDA, 0) NOT IN (10, 20, 98, 99)
                        AND NVL(PCNFSAID.CODFISCAL, 0) NOT IN (522, 622, 722, 532, 632, 732)
                        AND PCLANC.DTPAGTO IS NULL
                        AND EXTRACT(YEAR FROM PCLANC.DTVENC) = :ano
                        AND TO_CHAR(PCLANC.DTPAGTO, 'MON', 'NLS_DATE_LANGUAGE = AMERICAN') = :mes
                    GROUP BY 
                        TO_CHAR(PCLANC.DTVENC, 'MM'),  
                        PCCONTA.GRUPOCONTA, 
                        PCGRUPO.GRUPO
                ),
                VLREALIZADO AS (
                   SELECT 
					    MES, 
					    SUM(VLREALIZADO) AS VLREALIZADO,
					    GRUPOCONTA,
					    CODCONTA
					FROM (
					    SELECT 
					        NVL(SUM(
					            (DECODE(nvl(PCLANC.VPAGO, 0), 0,
					                    DECODE(PCLANC.DESCONTOFIN, PCLANC.VALOR, PCLANC.VALOR, 
					                        DECODE(PCLANC.VALORDEV, PCLANC.VALOR, PCLANC.VALOR, 0)),
					                    nvl(PCLANC.VPAGO, 0) + NVL(PCLANC.VALORDEV, 0)) * (1))), 0) * (-1) AS VLREALIZADO, 
					        TO_CHAR(PCLANC.DTPAGTO, 'MM') AS MES,
					        PCCONTA.GRUPOCONTA AS GRUPOCONTA,
					        PCLANC.CODCONTA AS CODCONTA
					    FROM 
					        PCLANC
					        JOIN PCCONTA ON PCLANC.CODCONTA = PCCONTA.CODCONTA
					        LEFT JOIN PCNFSAID ON PCLANC.NUMTRANSVENDA = PCNFSAID.NUMTRANSVENDA
					    WHERE 
					        NVL(PCNFSAID.CONDVENDA, 0) NOT IN (10, 20, 98, 99)
					        AND NVL(PCNFSAID.CODFISCAL, 0) NOT IN (522, 622, 722, 532, 632, 732)
					        AND NVL(PCLANC.CODROTINABAIXA, 0) <> 737
					        AND EXTRACT(YEAR FROM PCLANC.DTPAGTO) = :ano
					        AND TO_CHAR(PCLANC.DTPAGTO, 'MON', 'NLS_DATE_LANGUAGE = AMERICAN') =  :mes
					        AND NVL(PCLANC.VPAGO, 0) <> 0
					    GROUP BY 
					        TO_CHAR(PCLANC.DTPAGTO, 'MM'), 
					        PCCONTA.GRUPOCONTA,
					        PCLANC.CODCONTA
					)
					GROUP BY MES, GRUPOCONTA, CODCONTA

                )
               SELECT 
                    M.MES,
                    PORC.PORCENTAGEM,
                    C.CODCONTA, 
                    C.CONTA,
                    G.CODGRUPO AS GRUPOCONTA,
                    G.GRUPO, 
                    NVL(R.VLREALIZADO, 0) AS VLREALIZADO,
                    NVL(P.VALOR_PREVISTO, 0) AS VLPREVISTO,
                    (NVL(R.VLREALIZADO, 0) + NVL(P.VALOR_PREVISTO, 0)) AS REALIZADO_MAIS_PREV
                FROM 
                    PCGRUPO G  
                CROSS JOIN 
                    MESES M  -- Gera todas as combinações de meses
                LEFT JOIN 
                    VLREALIZADO R ON G.CODGRUPO = R.GRUPOCONTA AND R.MES = M.MES
                LEFT JOIN 
                    VLPREVISTO P ON G.CODGRUPO = P.GRUPOCONTA AND P.MES = M.MES
                LEFT JOIN
                	PCCONTA C ON R.CODCONTA = C.CODCONTA   
                LEFT JOIN
                	SITEPORCENTAGEMCONTA PORC ON C.CODCONTA = PORC.CODCONTA
                WHERE 
				    (NVL(R.VLREALIZADO, 0) <> 0 OR NVL(P.VALOR_PREVISTO, 0) <> 0)
			  	AND G.CODGRUPO IN (500,300,302,404,893,311,895,600,900, 406,301)

                ORDER BY 
                    CASE 
                        WHEN G.CODGRUPO = 500 THEN 1
                        WHEN G.CODGRUPO = 300 THEN 2
                        WHEN G.CODGRUPO = 302 THEN 3
                        WHEN G.CODGRUPO = 404 THEN 4
                        WHEN G.CODGRUPO = 893 THEN 5
                        WHEN G.CODGRUPO = 311 THEN 6
                        WHEN G.CODGRUPO = 895 THEN 7
                        WHEN G.CODGRUPO = 600 THEN 8
                        WHEN G.CODGRUPO = 900 THEN 9
                        ELSE 10
                    END,
                    M.MES
                   ";

        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        // Associar as variáveis de ano e mês aos placeholders SQL
        oci_bind_by_name($stmt, ":ano", $ano);
        oci_bind_by_name($stmt, ":mes", $mes);

        // Executa a consulta
        if (!oci_execute($stmt)) {
            $e = oci_error($stmt);
            $this->logger->error("Erro ao executar a consulta SQL: " . $e['message']);
            oci_free_statement($stmt);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro ao executar a consulta SQL'], 500);
        }

        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) !== false) {
            array_walk_recursive($row, function (&$item) {
                if (is_string($item) && !mb_detect_encoding($item, 'UTF-8', true)) {
                    $item = utf8_encode($item);
                }
            });

            // Convertendo os campos desejados para números
            $filiais[] = [
                'MES' => (int)$row['MES'],
                'CODCONTA' => (int)$row['CODCONTA'],
                'GRUPOCONTA' => (int)$row['GRUPOCONTA'],
                'VLREALIZADO' => (float)$row['VLREALIZADO'],
                'PORCENTAGEM' => isset($row['PORCENTAGEM']) ? (float)$row['PORCENTAGEM'] : 0, // Tratamento de null
                'VLPREVISTO' => (float)$row['VLPREVISTO'],
                'REALIZADO_MAIS_PREV' => (float)$row['REALIZADO_MAIS_PREV'],
                'CONTA' => $row['CONTA'], // mantendo como string
                'GRUPO' => $row['GRUPO']   // mantendo como string
            ];
        }

        // Fechar a conexão
        oci_free_statement($stmt);
        oci_close($conexao);

        // Verifica se há resultados
        if (empty($filiais)) {
            $this->logger->info("Nenhum resultado encontrado.");
            return $response->withJson(['message' => 'Nenhum dado encontrado'], 404);
        }

        // Log de sucesso
        $this->logger->info("Consulta executada com sucesso.");

        // Codificar e retornar JSON com substituição de caracteres inválidos
        $jsonData = json_encode($filiais, JSON_INVALID_UTF8_SUBSTITUTE);
        return $response->write($jsonData)->withHeader('Content-Type', 'application/json');
    });

});