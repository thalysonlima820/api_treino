<?php

use Slim\Http\Request;
use Slim\Http\Response;
use Symfony\Component\Console\Descriptor\Descriptor;

$app->group('/api/v1', function () {

    
    $this->get('/estoque/lista/produto/{dataInicio}/{datafim}', function (Request $request, Response $response) {

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

        // Obtém parâmetros da rota
        $dataInicio = $request->getAttribute('dataInicio');
        $datafim = $request->getAttribute('datafim');

        // Função para converter data para formato 'YYYY-MM-DD'
        function formatarData($data)
        {
            try {
                $dataConvertida = DateTime::createFromFormat('d-M-Y', $data) ?: DateTime::createFromFormat('Y-m-d', $data);
                if ($dataConvertida !== false) {
                    return $dataConvertida->format('Y-m-d');
                }
                throw new Exception("Formato de data inválido: $data");
            } catch (Exception $e) {
                // Lança um erro ou registra o problema
                error_log($e->getMessage());
                return null;
            }
        }
        

        // Formata a data para 'YYYY-MM-DD'
        $DATAP_FORMATADA_inicio = formatarData($dataInicio);
        $DATAP_FORMATADA_fim = formatarData($datafim);

        $sql = "WITH VENDIDO AS (
            SELECT P.CODEPTO, P.CODPROD, P.DESCRICAO, P.CODAUXILIAR,  SUM(M.QT) QT_VENDIDO, M.PUNIT, SUM(M.QT * M.PUNIT) VL_TOTAL, M.CODFILIAL
            FROM PCPRODUT P, PCMOV M
            WHERE P.CODPROD = M.CODPROD
            AND M.CODOPER = 'S'
            AND M.DTMOV BETWEEN TO_DATE(:dataInicio, 'YYYY-MM-DD') AND TO_DATE(:datafim, 'YYYY-MM-DD')
            GROUP BY P.CODEPTO, P.CODPROD, P.DESCRICAO, P.CODAUXILIAR,  M.PUNIT, M.CODFILIAL

            ),

            CUSTO AS (

            SELECT
                DTMOV,
                CODPROD,
                PUNIT,
                ROW_NUMBER() OVER (PARTITION BY M.CODPROD ORDER BY M.DTMOV DESC) AS RN
            FROM PCMOV M
                WHERE M.DTMOV > TO_DATE('01-JAN-2024', 'DD-MON-YYYY')
                AND M.CODOPER = 'E'
                AND M.EMBALAGEM = 'UN'
                AND M.CODFILIAL = 1
            )


            SELECT 
                V.CODPROD,
                V.CODAUXILIAR,
                V.DESCRICAO,
                SUM(V.QT_VENDIDO) AS QT_VENDIDO,
                SUM(V.VL_TOTAL) AS VL_TOTAL_SAIDA,
                B.PVENDA,
                C.PUNIT,
                ROUND(
                    CASE 
                        WHEN SUM(B.PVENDA) = 0 THEN 0
                        ELSE ((SUM(B.PVENDA) - SUM(C.PUNIT)) / SUM(B.PVENDA)) * 100
                    END,
                    2
                ) AS MARGEM_PRODUTO,
                D.DESCRICAO AS DEPARTAMENTO
            FROM  
                VENDIDO V
       
                JOIN PCEMBALAGEM B ON V.CODPROD = B.CODPROD AND V.CODFILIAL = B.CODFILIAL
                JOIN CUSTO C ON V.CODPROD = C.CODPROD
                JOIN PCDEPTO D ON V.CODEPTO = D.CODEPTO
                WHERE C.RN = 1
                AND V.CODFILIAL = 1
            GROUP BY 	
                V.CODPROD,
                V.CODAUXILIAR,
                V.DESCRICAO,
                B.PVENDA,
                C.PUNIT,
                D.DESCRICAO
                
                ";

        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        // Associar a data formatada ao placeholder SQL
        oci_bind_by_name($stmt, ":dataInicio", $DATAP_FORMATADA_inicio);
        oci_bind_by_name($stmt, ":datafim", $DATAP_FORMATADA_fim);

        // Executa a consulta
        if (!oci_execute($stmt)) {
            $e = oci_error($stmt);
            $this->logger->error("Erro ao executar a consulta SQL: " . json_encode($e));
            oci_free_statement($stmt);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro ao executar a consulta SQL', 'details' => $e], 500);
        }
        

        // Coletar os resultados
        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) !== false) {
            $row['CODPROD'] = isset($row['CODPROD']) ? (int)$row['CODPROD'] : null;
            $row['CODAUXILIAR'] = isset($row['CODAUXILIAR']) ? (int)$row['CODAUXILIAR'] : null;
            
            $row['QT_VENDIDO'] = isset($row['QT_VENDIDO']) ? (float)$row['QT_VENDIDO'] : null;
            $row['VL_TOTAL_SAIDA'] = isset($row['VL_TOTAL_SAIDA']) ? (float)$row['VL_TOTAL_SAIDA'] : null;
            $row['PVENDA'] = isset($row['PVENDA']) ? (float)$row['PVENDA'] : null;
            $row['PUNIT'] = isset($row['PUNIT']) ? (float)$row['PUNIT'] : null;
            $row['MARGEM_PRODUTO'] = isset($row['MARGEM_PRODUTO']) ? (float)$row['MARGEM_PRODUTO'] : null;
 
             $filiais[] = $row;
            $filiais[] = $row;
        }

        // Fechar a conexão
        oci_free_statement($stmt);
        oci_close($conexao);

        // Verificar se há resultados


        // Convertendo resultados para UTF-8
        foreach ($filiais as &$filial) {
            array_walk_recursive($filial, function (&$item) {
                if (!mb_detect_encoding($item, 'utf-8', true)) {
                    $item = utf8_encode($item);
                }
            });
        }

        $this->logger->info("Consulta executada com sucesso.");

        // Retornar resultados em JSON
        return $response->withJson($filiais);
    });
    $this->get('/estoque/tranferencia/lista/{dataInicio}/{datafim}/{filial}', function (Request $request, Response $response) {

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

        // Obtém parâmetros da rota
        $dataInicio = $request->getAttribute('dataInicio');
        $datafim = $request->getAttribute('datafim');
        $filial = $request->getAttribute('filial');

        // Função para converter data para formato 'YYYY-MM-DD'
        function formatarData($data)
        {
            // Tenta interpretar a data no formato 'DD-MMM-YYYY'
            $dataConvertida = DateTime::createFromFormat('d-M-Y', $data);
            if ($dataConvertida !== false) {
                return $dataConvertida->format('Y-m-d');
            }
            // Tenta interpretar a data no formato 'YYYY-MM-DD'
            $dataConvertida = DateTime::createFromFormat('Y-m-d', $data);
            if ($dataConvertida !== false) {
                return $dataConvertida->format('Y-m-d');
            }
            // Se o formato não for reconhecido, retorna a data original (ou você pode lançar um erro)
            return $data;
        }

        // Formata a data para 'YYYY-MM-DD'
        $DATAP_FORMATADA_inicio = formatarData($dataInicio);
        $DATAP_FORMATADA_fim = formatarData($datafim);

        $sql = "WITH ENTRADA AS (
                    SELECT PC.CODEPTO, SUM(M.QT) AS TOTAL_QT_ENTRADA, SUM(M.QT * M.PUNIT) AS VALOR_TOTAL_ENTRADA 
                    FROM PCMOV M, PCPEDIDO P, PCPRODUT PC
                    WHERE M.CODOPER IN ('E', 'ET' )
                    AND M.CODPROD = PC.CODPROD
                    AND M.NUMPED = P.NUMPED
                    AND M.CODFILIAL = :filial
                    AND P.TIPOBONIFIC = 'N'
                    AND M.DTMOV BETWEEN TO_DATE(:dataInicio, 'YYYY-MM-DD') AND TO_DATE(:datafim, 'YYYY-MM-DD')
                   
                    
                    GROUP BY PC.CODEPTO
                ),
                SAIDA AS (
                    SELECT PC.CODEPTO, SUM(M.QT) AS TOTAL_QT_SAIDA, SUM(M.QT * M.PUNIT) AS VALOR_TOTAL_SAIDA 
                    FROM PCMOV M, PCPRODUT PC
                    WHERE M.CODOPER IN  ('S', 'ST')
                    AND M.CODPROD = PC.CODPROD
                    AND M.CODFILIAL = :filial
                   AND M.DTMOV BETWEEN TO_DATE(:dataInicio, 'YYYY-MM-DD') AND TO_DATE(:datafim, 'YYYY-MM-DD')
                    GROUP BY PC.CODEPTO
                ),
                PRODUTO AS (
                    SELECT DISTINCT P.CODEPTO, D.DESCRICAO AS DEPARTAMENTO
                    FROM PCPRODUT P
                    JOIN PCDEPTO D ON P.CODEPTO = D.CODEPTO
                ),
                SAIDA_DEVOLUCAO AS (
                
                    SELECT PC.CODEPTO, SUM(M.QT) AS TOTAL_QT_DELV, SUM(M.QT * M.PUNIT) AS VALOR_TOTAL_DEVL 
                    FROM PCMOV M, PCPRODUT PC
                    WHERE M.CODOPER = 'SD'
                    AND M.CODPROD = PC.CODPROD
                    AND M.CODFILIAL = :filial
                    AND M.DTMOV BETWEEN TO_DATE(:dataInicio, 'YYYY-MM-DD') AND TO_DATE(:datafim, 'YYYY-MM-DD')
             
                    GROUP BY PC.CODEPTO
                
                )
               SELECT 
               P.CODEPTO,
                    P.DEPARTAMENTO,
                    CASE 
			        WHEN SD.VALOR_TOTAL_DEVL IS NULL THEN E.VALOR_TOTAL_ENTRADA
			        ELSE (E.VALOR_TOTAL_ENTRADA - SD.VALOR_TOTAL_DEVL)
			        END AS VALOR_TOTAL_ENTRADA,
                    SD.VALOR_TOTAL_DEVL as VALOR_DEVLUCAO,

                         CASE 
                        WHEN P.DEPARTAMENTO = 'ACOUGUE' THEN 25
                        WHEN P.DEPARTAMENTO = 'BAZAR' THEN 35
                        WHEN P.DEPARTAMENTO = 'BEBIDAS' THEN 20
                        WHEN P.DEPARTAMENTO = 'HORTIFRUTI' THEN 31
                        WHEN P.DEPARTAMENTO = 'LIMPEZA' THEN 20
                        WHEN P.DEPARTAMENTO = 'MATINAIS' THEN 17
                        WHEN P.DEPARTAMENTO = 'MERCEARIA DE ALTO GIRO' THEN 18
                        WHEN P.DEPARTAMENTO = 'MERCEARIA DOCE' THEN 25
                        WHEN P.DEPARTAMENTO = 'PADARIA' THEN 50
                        WHEN P.DEPARTAMENTO = 'PERECIVEIS CONG/RESF' THEN 23
                        WHEN P.DEPARTAMENTO = 'PERECIVEIS LACTEOS' THEN 25
                        WHEN P.DEPARTAMENTO = 'PERFUMARIA' THEN 25
                        WHEN P.DEPARTAMENTO = 'PET SHOP' THEN 26


                        -- Adicione outros departamentos e margens ideais conforme necessário
                        ELSE 0 -- Valor padrão para departamentos sem especificação
                    END AS MARGEM_IDEAL,
                        
                    S.VALOR_TOTAL_SAIDA,
                    ROUND((((S.VALOR_TOTAL_SAIDA - E.VALOR_TOTAL_ENTRADA) / S.VALOR_TOTAL_SAIDA) * 100), 2) AS MARGEM,
                    CASE 
                        WHEN E.VALOR_TOTAL_ENTRADA > S.VALOR_TOTAL_SAIDA THEN 'PREJUIZO POTENCIAL'
                        ELSE 'LUCRO POTENCIAL'
                    END AS RELATORIO_VALOR
                FROM SAIDA S 
                LEFT JOIN ENTRADA E ON S.CODEPTO = E.CODEPTO 
                 JOIN PRODUTO P ON S.CODEPTO = P.CODEPTO
                LEFT JOIN SAIDA_DEVOLUCAO SD ON SD.CODEPTO = E.CODEPTO
                WHERE P.DEPARTAMENTO != 'USO INTERNO'
                ORDER BY P.DEPARTAMENTO
                
                ";

        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        // Associar a data formatada ao placeholder SQL
        oci_bind_by_name($stmt, ":dataInicio", $DATAP_FORMATADA_inicio);
        oci_bind_by_name($stmt, ":datafim", $DATAP_FORMATADA_fim);
        oci_bind_by_name($stmt, ":filial", $filial);

        // Executa a consulta
        if (!oci_execute($stmt)) {
            $e = oci_error($stmt);
            $this->logger->error("Erro ao executar a consulta SQL: " . $e['message']);
            oci_free_statement($stmt);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro ao executar a consulta SQL'], 500);
        }

        // Coletar os resultados
        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) !== false) {
            $filiais[] = $row;
        }

        // Fechar a conexão
        oci_free_statement($stmt);
        oci_close($conexao);

        // Verificar se há resultados
        if (empty($filiais)) {
            $this->logger->info("Nenhum resultado encontrado.");
            return $response->withJson(['message' => 'Nenhum dado encontrado'], 404);
        }

        // Convertendo resultados para UTF-8
        foreach ($filiais as &$filial) {
            array_walk_recursive($filial, function (&$item) {
                if (!mb_detect_encoding($item, 'utf-8', true)) {
                    $item = utf8_encode($item);
                }
            });
        }

        $this->logger->info("Consulta executada com sucesso.");

        // Retornar resultados em JSON
        return $response->withJson($filiais);
    });
    $this->get('/estoque/lista/{dataInicio}/{datafim}/{filial}', function (Request $request, Response $response) {

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

        // Obtém parâmetros da rota
        $dataInicio = $request->getAttribute('dataInicio');
        $datafim = $request->getAttribute('datafim');
        $filial = $request->getAttribute('filial');

        // Função para converter data para formato 'YYYY-MM-DD'
        function formatarData($data)
        {
            // Tenta interpretar a data no formato 'DD-MMM-YYYY'
            $dataConvertida = DateTime::createFromFormat('d-M-Y', $data);
            if ($dataConvertida !== false) {
                return $dataConvertida->format('Y-m-d');
            }
            // Tenta interpretar a data no formato 'YYYY-MM-DD'
            $dataConvertida = DateTime::createFromFormat('Y-m-d', $data);
            if ($dataConvertida !== false) {
                return $dataConvertida->format('Y-m-d');
            }
            // Se o formato não for reconhecido, retorna a data original (ou você pode lançar um erro)
            return $data;
        }

        // Formata a data para 'YYYY-MM-DD'
        $DATAP_FORMATADA_inicio = formatarData($dataInicio);
        $DATAP_FORMATADA_fim = formatarData($datafim);

        $sql = "WITH ENTRADA AS (
                    SELECT PC.CODEPTO, SUM(M.QT) AS TOTAL_QT_ENTRADA, SUM(M.QT * M.PUNIT) AS VALOR_TOTAL_ENTRADA 
                    FROM PCMOV M, PCPEDIDO P, PCPRODUT PC
                    WHERE M.CODOPER IN ('E' )
                    AND M.CODPROD = PC.CODPROD
                    AND M.NUMPED = P.NUMPED
                    AND M.CODFILIAL = :filial
                    AND P.TIPOBONIFIC = 'N'
                    AND M.DTMOV BETWEEN TO_DATE(:dataInicio, 'YYYY-MM-DD') AND TO_DATE(:datafim, 'YYYY-MM-DD')
                   
                    
                    GROUP BY PC.CODEPTO
                ),
                SAIDA AS (
                    SELECT PC.CODEPTO, SUM(M.QT) AS TOTAL_QT_SAIDA, SUM(M.QT * M.PUNIT) AS VALOR_TOTAL_SAIDA 
                    FROM PCMOV M, PCPRODUT PC
                    WHERE M.CODOPER IN  ('S')
                    AND M.CODPROD = PC.CODPROD
                    AND M.CODFILIAL = :filial
                   AND M.DTMOV BETWEEN TO_DATE(:dataInicio, 'YYYY-MM-DD') AND TO_DATE(:datafim, 'YYYY-MM-DD')
                    GROUP BY PC.CODEPTO
                ),
                PRODUTO AS (
                    SELECT DISTINCT P.CODEPTO, D.DESCRICAO AS DEPARTAMENTO
                    FROM PCPRODUT P
                    JOIN PCDEPTO D ON P.CODEPTO = D.CODEPTO
                ),
                SAIDA_DEVOLUCAO AS (
                
                    SELECT PC.CODEPTO, SUM(M.QT) AS TOTAL_QT_DELV, SUM(M.QT * M.PUNIT) AS VALOR_TOTAL_DEVL 
                    FROM PCMOV M, PCPRODUT PC
                    WHERE M.CODOPER = 'SD'
                    AND M.CODPROD = PC.CODPROD
                    AND M.CODFILIAL = :filial
                    AND M.DTMOV BETWEEN TO_DATE(:dataInicio, 'YYYY-MM-DD') AND TO_DATE(:datafim, 'YYYY-MM-DD')
             
                    GROUP BY PC.CODEPTO
                
                )
               SELECT 
               P.CODEPTO,
                    P.DEPARTAMENTO,
                    CASE 
			        WHEN SD.VALOR_TOTAL_DEVL IS NULL THEN E.VALOR_TOTAL_ENTRADA
			        ELSE (E.VALOR_TOTAL_ENTRADA - SD.VALOR_TOTAL_DEVL)
			        END AS VALOR_TOTAL_ENTRADA,
                    SD.VALOR_TOTAL_DEVL as VALOR_DEVLUCAO,

                        CASE 
                        WHEN P.DEPARTAMENTO = 'ACOUGUE' THEN 25
                        WHEN P.DEPARTAMENTO = 'BAZAR' THEN 35
                        WHEN P.DEPARTAMENTO = 'BEBIDAS' THEN 20
                        WHEN P.DEPARTAMENTO = 'HORTIFRUTI' THEN 31
                        WHEN P.DEPARTAMENTO = 'LIMPEZA' THEN 20
                        WHEN P.DEPARTAMENTO = 'MATINAIS' THEN 17
                        WHEN P.DEPARTAMENTO = 'MERCEARIA DE ALTO GIRO' THEN 18
                        WHEN P.DEPARTAMENTO = 'MERCEARIA DOCE' THEN 25
                        WHEN P.DEPARTAMENTO = 'PADARIA' THEN 50
                        WHEN P.DEPARTAMENTO = 'PERECIVEIS CONG/RESF' THEN 23
                        WHEN P.DEPARTAMENTO = 'PERECIVEIS LACTEOS' THEN 25
                        WHEN P.DEPARTAMENTO = 'PERFUMARIA' THEN 25
                        WHEN P.DEPARTAMENTO = 'PET SHOP' THEN 26


                        -- Adicione outros departamentos e margens ideais conforme necessário
                        ELSE 0 -- Valor padrão para departamentos sem especificação
                    END AS MARGEM_IDEAL,
                        
                    S.VALOR_TOTAL_SAIDA,
                    ROUND((((S.VALOR_TOTAL_SAIDA - E.VALOR_TOTAL_ENTRADA) / S.VALOR_TOTAL_SAIDA) * 100), 2) AS MARGEM,
                    CASE 
                        WHEN E.VALOR_TOTAL_ENTRADA > S.VALOR_TOTAL_SAIDA THEN 'PREJUIZO POTENCIAL'
                        ELSE 'LUCRO POTENCIAL'
                    END AS RELATORIO_VALOR
                FROM SAIDA S 
                LEFT JOIN ENTRADA E ON S.CODEPTO = E.CODEPTO 
                 JOIN PRODUTO P ON S.CODEPTO = P.CODEPTO
                LEFT JOIN SAIDA_DEVOLUCAO SD ON SD.CODEPTO = E.CODEPTO
                WHERE P.DEPARTAMENTO != 'USO INTERNO'
                ORDER BY P.DEPARTAMENTO
                
                ";

        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        // Associar a data formatada ao placeholder SQL
        oci_bind_by_name($stmt, ":dataInicio", $DATAP_FORMATADA_inicio);
        oci_bind_by_name($stmt, ":datafim", $DATAP_FORMATADA_fim);
        oci_bind_by_name($stmt, ":filial", $filial);

        // Executa a consulta
        if (!oci_execute($stmt)) {
            $e = oci_error($stmt);
            $this->logger->error("Erro ao executar a consulta SQL: " . $e['message']);
            oci_free_statement($stmt);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro ao executar a consulta SQL'], 500);
        }

        // Coletar os resultados
        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) !== false) {
            $filiais[] = $row;
        }

        // Fechar a conexão
        oci_free_statement($stmt);
        oci_close($conexao);

        // Verificar se há resultados
        if (empty($filiais)) {
            $this->logger->info("Nenhum resultado encontrado.");
            return $response->withJson(['message' => 'Nenhum dado encontrado'], 404);
        }

        // Convertendo resultados para UTF-8
        foreach ($filiais as &$filial) {
            array_walk_recursive($filial, function (&$item) {
                if (!mb_detect_encoding($item, 'utf-8', true)) {
                    $item = utf8_encode($item);
                }
            });
        }

        $this->logger->info("Consulta executada com sucesso.");

        // Retornar resultados em JSON
        return $response->withJson($filiais);
    });

    $this->get('/estoque/lista/{dataInicio}/{datafim}', function (Request $request, Response $response) {

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

        // Obtém parâmetros da rota
        $dataInicio = $request->getAttribute('dataInicio');
        $datafim = $request->getAttribute('datafim');

        // Função para converter data para formato 'YYYY-MM-DD'
        function formatarData($data)
        {
            // Tenta interpretar a data no formato 'DD-MMM-YYYY'
            $dataConvertida = DateTime::createFromFormat('d-M-Y', $data);
            if ($dataConvertida !== false) {
                return $dataConvertida->format('Y-m-d');
            }
            // Tenta interpretar a data no formato 'YYYY-MM-DD'
            $dataConvertida = DateTime::createFromFormat('Y-m-d', $data);
            if ($dataConvertida !== false) {
                return $dataConvertida->format('Y-m-d');
            }
            // Se o formato não for reconhecido, retorna a data original (ou você pode lançar um erro)
            return $data;
        }

        // Formata a data para 'YYYY-MM-DD'
        $DATAP_FORMATADA_inicio = formatarData($dataInicio);
        $DATAP_FORMATADA_fim = formatarData($datafim);

        $sql = "WITH ENTRADA AS (
                    SELECT PC.CODEPTO, SUM(M.QT) AS TOTAL_QT_ENTRADA, SUM(M.QT * M.PUNIT) AS VALOR_TOTAL_ENTRADA 
                    FROM PCMOV M, PCPEDIDO P, PCPRODUT PC
                    WHERE M.CODOPER = 'E'
                    AND M.CODPROD = PC.CODPROD
                    AND M.NUMPED = P.NUMPED
                    AND P.TIPOBONIFIC = 'N'
                    AND M.DTMOV BETWEEN TO_DATE(:dataInicio, 'YYYY-MM-DD') AND TO_DATE(:datafim, 'YYYY-MM-DD')
                    AND M.CODFORNEC NOT IN (1,2,4,5,1285)
                    
                    GROUP BY PC.CODEPTO
                ),
                SAIDA AS (
                    SELECT PC.CODEPTO, SUM(M.QT) AS TOTAL_QT_SAIDA, SUM(M.QT * M.PUNIT) AS VALOR_TOTAL_SAIDA 
                    FROM PCMOV M, PCPRODUT PC
                    WHERE M.CODOPER = 'S'
                    AND M.CODPROD = PC.CODPROD
                   AND M.DTMOV BETWEEN TO_DATE(:dataInicio, 'YYYY-MM-DD') AND TO_DATE(:datafim, 'YYYY-MM-DD')
                    GROUP BY PC.CODEPTO
                ),
                PRODUTO AS (
                    SELECT DISTINCT P.CODEPTO, D.DESCRICAO AS DEPARTAMENTO
                    FROM PCPRODUT P
                    JOIN PCDEPTO D ON P.CODEPTO = D.CODEPTO
                ),
                SAIDA_DEVOLUCAO AS (
                
                    SELECT PC.CODEPTO, SUM(M.QT) AS TOTAL_QT_DELV, SUM(M.QT * M.PUNIT) AS VALOR_TOTAL_DEVL 
                    FROM PCMOV M, PCPRODUT PC
                    WHERE M.CODOPER = 'SD'
                    AND M.CODPROD = PC.CODPROD
                    AND M.DTMOV BETWEEN TO_DATE(:dataInicio, 'YYYY-MM-DD') AND TO_DATE(:datafim, 'YYYY-MM-DD')
                    AND M.CODFORNEC NOT IN (1,2,4,5,1285)
                    GROUP BY PC.CODEPTO
                
                )
               SELECT 
               P.CODEPTO,
                    P.DEPARTAMENTO,
                    CASE 
			        WHEN SD.VALOR_TOTAL_DEVL IS NULL THEN E.VALOR_TOTAL_ENTRADA
			        ELSE (E.VALOR_TOTAL_ENTRADA - SD.VALOR_TOTAL_DEVL)
			        END AS VALOR_TOTAL_ENTRADA,
                    SD.VALOR_TOTAL_DEVL as VALOR_DEVLUCAO,

                        CASE 
                        WHEN P.DEPARTAMENTO = 'ACOUGUE' THEN 25
                        WHEN P.DEPARTAMENTO = 'BAZAR' THEN 35
                        WHEN P.DEPARTAMENTO = 'BEBIDAS' THEN 20
                        WHEN P.DEPARTAMENTO = 'HORTIFRUTI' THEN 31
                        WHEN P.DEPARTAMENTO = 'LIMPEZA' THEN 20
                        WHEN P.DEPARTAMENTO = 'MATINAIS' THEN 17
                        WHEN P.DEPARTAMENTO = 'MERCEARIA DE ALTO GIRO' THEN 18
                        WHEN P.DEPARTAMENTO = 'MERCEARIA DOCE' THEN 25
                        WHEN P.DEPARTAMENTO = 'PADARIA' THEN 50
                        WHEN P.DEPARTAMENTO = 'PERECIVEIS CONG/RESF' THEN 23
                        WHEN P.DEPARTAMENTO = 'PERECIVEIS LACTEOS' THEN 25
                        WHEN P.DEPARTAMENTO = 'PERFUMARIA' THEN 25
                        WHEN P.DEPARTAMENTO = 'PET SHOP' THEN 26


                        -- Adicione outros departamentos e margens ideais conforme necessário
                        ELSE 0 -- Valor padrão para departamentos sem especificação
                    END AS MARGEM_IDEAL,
                        
                    S.VALOR_TOTAL_SAIDA,
                    ROUND((((S.VALOR_TOTAL_SAIDA - E.VALOR_TOTAL_ENTRADA) / S.VALOR_TOTAL_SAIDA) * 100), 2) AS MARGEM,
                    CASE 
                        WHEN E.VALOR_TOTAL_ENTRADA > S.VALOR_TOTAL_SAIDA THEN 'PREJUIZO POTENCIAL'
                        ELSE 'LUCRO POTENCIAL'
                    END AS RELATORIO_VALOR
                FROM SAIDA S 
                LEFT JOIN ENTRADA E ON S.CODEPTO = E.CODEPTO 
                 JOIN PRODUTO P ON S.CODEPTO = P.CODEPTO
                LEFT JOIN SAIDA_DEVOLUCAO SD ON SD.CODEPTO = E.CODEPTO
                WHERE P.DEPARTAMENTO != 'USO INTERNO'
                ORDER BY P.DEPARTAMENTO
                
                ";

        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        // Associar a data formatada ao placeholder SQL
        oci_bind_by_name($stmt, ":dataInicio", $DATAP_FORMATADA_inicio);
        oci_bind_by_name($stmt, ":datafim", $DATAP_FORMATADA_fim);

        // Executa a consulta
        if (!oci_execute($stmt)) {
            $e = oci_error($stmt);
            $this->logger->error("Erro ao executar a consulta SQL: " . $e['message']);
            oci_free_statement($stmt);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro ao executar a consulta SQL'], 500);
        }

        // Coletar os resultados
        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) !== false) {
            $filiais[] = $row;
        }

        // Fechar a conexão
        oci_free_statement($stmt);
        oci_close($conexao);

        // Verificar se há resultados
        if (empty($filiais)) {
            $this->logger->info("Nenhum resultado encontrado.");
            return $response->withJson(['message' => 'Nenhum dado encontrado'], 404);
        }

        // Convertendo resultados para UTF-8
        foreach ($filiais as &$filial) {
            array_walk_recursive($filial, function (&$item) {
                if (!mb_detect_encoding($item, 'utf-8', true)) {
                    $item = utf8_encode($item);
                }
            });
        }

        $this->logger->info("Consulta executada com sucesso.");

        // Retornar resultados em JSON
        return $response->withJson($filiais);
    });






    
    $this->get('/estoque/lista/produto/pes/{dataInicio}/{datafim}/{codprod}', function (Request $request, Response $response) {

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

        // Obtém parâmetros da rota
        $dataInicio = $request->getAttribute('dataInicio');
        $datafim = $request->getAttribute('datafim');
        $codprod = $request->getAttribute('codprod');

        // Função para converter data para formato 'YYYY-MM-DD'
        function formatarData($data)
        {
            try {
                $dataConvertida = DateTime::createFromFormat('d-M-Y', $data) ?: DateTime::createFromFormat('Y-m-d', $data);
                if ($dataConvertida !== false) {
                    return $dataConvertida->format('Y-m-d');
                }
                throw new Exception("Formato de data inválido: $data");
            } catch (Exception $e) {
                // Lança um erro ou registra o problema
                error_log($e->getMessage());
                return null;
            }
        }
        

        // Formata a data para 'YYYY-MM-DD'
        $DATAP_FORMATADA_inicio = formatarData($dataInicio);
        $DATAP_FORMATADA_fim = formatarData($datafim);

        $sql = "WITH VENDIDO AS (
            SELECT P.CODEPTO, P.CODPROD, P.DESCRICAO, P.CODAUXILIAR,  SUM(M.QT) QT_VENDIDO, M.PUNIT, SUM(M.QT * M.PUNIT) VL_TOTAL, M.CODFILIAL
            FROM PCPRODUT P, PCMOV M
            WHERE P.CODPROD = M.CODPROD
            AND M.CODOPER = 'S'
            AND M.DTMOV BETWEEN TO_DATE(:dataInicio, 'YYYY-MM-DD') AND TO_DATE(:datafim, 'YYYY-MM-DD')
            GROUP BY P.CODEPTO, P.CODPROD, P.DESCRICAO, P.CODAUXILIAR,  M.PUNIT, M.CODFILIAL

            ),

            CUSTO AS (

            SELECT
                DTMOV,
                CODPROD,
                PUNIT
            FROM (
                SELECT
                    M.DTMOV,
                    M.CODPROD,
                    M.PUNIT,
                    ROW_NUMBER() OVER (PARTITION BY M.CODPROD ORDER BY M.DTMOV DESC) AS RN
                FROM PCMOV M
                WHERE M.DTMOV > TO_DATE('01-JAN-2024', 'DD-MON-YYYY')
                AND M.CODOPER = 'E'
            ) WHERE RN = 1

            )


            SELECT 
                V.CODPROD,
                V.CODAUXILIAR,
                V.DESCRICAO,
                SUM(V.QT_VENDIDO) AS QT_VENDIDO,
                SUM(V.VL_TOTAL) AS VL_TOTAL_SAIDA,
                B.PVENDA,
                C.PUNIT,
                ROUND(
                    CASE 
                        WHEN SUM(B.PVENDA) = 0 THEN 0
                        ELSE ((SUM(B.PVENDA) - SUM(C.PUNIT)) / SUM(B.PVENDA)) * 100
                    END,
                    2
                ) AS MARGEM_PRODUTO,
                D.DESCRICAO AS DEPARTAMENTO
            FROM  
                VENDIDO V
       
                JOIN PCEMBALAGEM B ON V.CODPROD = B.CODPROD AND V.CODFILIAL = B.CODFILIAL
                JOIN CUSTO C ON V.CODPROD = C.CODPROD
                JOIN PCDEPTO D ON V.CODEPTO = D.CODEPTO
                WHERE V.CODPROD = :codprod
            GROUP BY 	
                V.CODPROD,
                V.CODAUXILIAR,
                V.DESCRICAO,
                B.PVENDA,
                C.PUNIT,
                D.DESCRICAO
                
                ";

        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        // Associar a data formatada ao placeholder SQL
        oci_bind_by_name($stmt, ":dataInicio", $DATAP_FORMATADA_inicio);
        oci_bind_by_name($stmt, ":datafim", $DATAP_FORMATADA_fim);
        oci_bind_by_name($stmt, ":codprod", $codprod);

        // Executa a consulta
        if (!oci_execute($stmt)) {
            $e = oci_error($stmt);
            $this->logger->error("Erro ao executar a consulta SQL: " . json_encode($e));
            oci_free_statement($stmt);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro ao executar a consulta SQL', 'details' => $e], 500);
        }
        

        // Coletar os resultados
        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) !== false) {
             // Conversão de tipos para garantir que os campos sejam numéricos
             $row['CODPROD'] = isset($row['CODPROD']) ? (int)$row['CODPROD'] : null;
             $row['CODAUXILIAR'] = isset($row['CODAUXILIAR']) ? (int)$row['CODAUXILIAR'] : null;
             
             $row['QT_VENDIDO'] = isset($row['QT_VENDIDO']) ? (float)$row['QT_VENDIDO'] : null;
             $row['VL_TOTAL_SAIDA'] = isset($row['VL_TOTAL_SAIDA']) ? (float)$row['VL_TOTAL_SAIDA'] : null;
             $row['PVENDA'] = isset($row['PVENDA']) ? (float)$row['PVENDA'] : null;
             $row['PUNIT'] = isset($row['PUNIT']) ? (float)$row['PUNIT'] : null;
             $row['MARGEM_PRODUTO'] = isset($row['MARGEM_PRODUTO']) ? (float)$row['MARGEM_PRODUTO'] : null;
 
             $filiais[] = $row;
            $filiais[] = $row;
        }

        // Fechar a conexão
        oci_free_statement($stmt);
        oci_close($conexao);

        // Verificar se há resultados
        if (empty($filiais)) {
            $this->logger->info("Nenhum resultado encontrado.");
            return $response->withJson(['message' => 'Nenhum dado encontrado'], 404);
        }

        // Convertendo resultados para UTF-8
        foreach ($filiais as &$filial) {
            array_walk_recursive($filial, function (&$item) {
                if (!mb_detect_encoding($item, 'utf-8', true)) {
                    $item = utf8_encode($item);
                }
            });
        }

        $this->logger->info("Consulta executada com sucesso.");

        // Retornar resultados em JSON
        return $response->withJson($filiais);
    });

    $this->get('/estoque/lista/secao/{dataInicio}/{datafim}/{codepto}', function (Request $request, Response $response) {

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

        // Obtém parâmetros da rota
        $dataInicio = $request->getAttribute('dataInicio');
        $datafim = $request->getAttribute('datafim');
        $codepto = $request->getAttribute('codepto');

        // Função para converter data para formato 'YYYY-MM-DD'
        function formatarData($data)
        {
            // Tenta interpretar a data no formato 'DD-MMM-YYYY'
            $dataConvertida = DateTime::createFromFormat('d-M-Y', $data);
            if ($dataConvertida !== false) {
                return $dataConvertida->format('Y-m-d');
            }
            // Tenta interpretar a data no formato 'YYYY-MM-DD'
            $dataConvertida = DateTime::createFromFormat('Y-m-d', $data);
            if ($dataConvertida !== false) {
                return $dataConvertida->format('Y-m-d');
            }
            // Se o formato não for reconhecido, retorna a data original (ou você pode lançar um erro)
            return $data;
        }

        // Formata a data para 'YYYY-MM-DD'
        $DATAP_FORMATADA_inicio = formatarData($dataInicio);
        $DATAP_FORMATADA_fim = formatarData($datafim);

        $sql = "WITH ENTRADA AS (
                    SELECT PC.CODSEC, SUM(M.QT) AS TOTAL_QT_ENTRADA, SUM(M.QT * M.PUNIT) AS VALOR_TOTAL_ENTRADA 
                    FROM PCMOV M, PCPEDIDO P, PCPRODUT PC
                    WHERE M.CODOPER = 'E'
                    AND M.CODPROD = PC.CODPROD
                    AND M.NUMPED = P.NUMPED
                    AND P.TIPOBONIFIC = 'N'
                    AND M.DTMOV BETWEEN TO_DATE(:dataInicio, 'YYYY-MM-DD') AND TO_DATE(:datafim, 'YYYY-MM-DD')
                    AND M.CODFORNEC NOT IN (1,2,4,5,1285)
                    
                    GROUP BY PC.CODSEC
                ),
                SAIDA AS (
                    SELECT PC.CODSEC, SUM(M.QT) AS TOTAL_QT_SAIDA, SUM(M.QT * M.PUNIT) AS VALOR_TOTAL_SAIDA 
                    FROM PCMOV M, PCPRODUT PC
                    WHERE M.CODOPER = 'S'
                    AND M.CODPROD = PC.CODPROD
                   AND M.DTMOV BETWEEN TO_DATE(:dataInicio, 'YYYY-MM-DD') AND TO_DATE(:datafim, 'YYYY-MM-DD')
                    GROUP BY PC.CODSEC
                ),
                PRODUTO AS (
                    SELECT DISTINCT P.CODSEC, D.DESCRICAO AS DEPARTAMENTO, S.DESCRICAO AS SECAO
                    FROM PCPRODUT P
                    JOIN PCDEPTO D ON P.CODEPTO = D.CODEPTO
                    JOIN PCSECAO S ON P.CODSEC = S.CODSEC
                    WHERE D.CODEPTO = :codepto
                ),
                SAIDA_DEVOLUCAO AS (
                
                    SELECT PC.CODSEC, SUM(M.QT) AS TOTAL_QT_DELV, SUM(M.QT * M.PUNIT) AS VALOR_TOTAL_DEVL 
                    FROM PCMOV M, PCPRODUT PC
                    WHERE M.CODOPER = 'SD'
                    AND M.CODPROD = PC.CODPROD
                    AND M.DTMOV BETWEEN TO_DATE(:dataInicio, 'YYYY-MM-DD') AND TO_DATE(:datafim, 'YYYY-MM-DD')
                    AND M.CODFORNEC NOT IN (1,2,4,5,1285)
                    GROUP BY PC.CODSEC
                
                )
                
                
                
                
               SELECT 
               P.SECAO,
                    P.DEPARTAMENTO,
                    CASE 
			        WHEN SD.VALOR_TOTAL_DEVL IS NULL THEN E.VALOR_TOTAL_ENTRADA
			        ELSE (E.VALOR_TOTAL_ENTRADA - SD.VALOR_TOTAL_DEVL)
			        END AS VALOR_TOTAL_ENTRADA,
                    SD.VALOR_TOTAL_DEVL as VALOR_DEVLUCAO,

                        CASE 
                        WHEN P.DEPARTAMENTO = 'ACOUGUE' THEN 30
                        WHEN P.DEPARTAMENTO = 'BAZAR' THEN 30
                        WHEN P.DEPARTAMENTO = 'BEBIDAS' THEN 17
                        WHEN P.DEPARTAMENTO = 'HORTIFRUTI' THEN 31
                        WHEN P.DEPARTAMENTO = 'LIMPEZA' THEN 20
                        WHEN P.DEPARTAMENTO = 'MATINAIS' THEN 17
                        WHEN P.DEPARTAMENTO = 'MERCEARIA DE ALTO GIRO' THEN 18
                        WHEN P.DEPARTAMENTO = 'MERCEARIA DOCE' THEN 25
                        WHEN P.DEPARTAMENTO = 'PADARIA' THEN 50
                        WHEN P.DEPARTAMENTO = 'PERECIVEIS CONG/RESF' THEN 23
                        WHEN P.DEPARTAMENTO = 'PERECIVEIS LACTEOS' THEN 25
                        WHEN P.DEPARTAMENTO = 'PERFUMARIA' THEN 25
                        WHEN P.DEPARTAMENTO = 'PET SHOP' THEN 26


                        -- Adicione outros departamentos e margens ideais conforme necessário
                        ELSE 0 -- Valor padrão para departamentos sem especificação
                    END AS MARGEM_IDEAL,
                        
                    S.VALOR_TOTAL_SAIDA,
                    ROUND((((S.VALOR_TOTAL_SAIDA - E.VALOR_TOTAL_ENTRADA) / S.VALOR_TOTAL_SAIDA) * 100), 2) AS MARGEM,
                    CASE 
                        WHEN E.VALOR_TOTAL_ENTRADA > S.VALOR_TOTAL_SAIDA THEN 'PREJUIZO POTENCIAL'
                        ELSE 'LUCRO POTENCIAL'
                    END AS RELATORIO_VALOR
                FROM SAIDA S 
                LEFT JOIN ENTRADA E ON S.CODSEC = E.CODSEC 
                 JOIN PRODUTO P ON S.CODSEC = P.CODSEC
                LEFT JOIN SAIDA_DEVOLUCAO SD ON SD.CODSEC = E.CODSEC
                WHERE P.DEPARTAMENTO != 'USO INTERNO'
                ORDER BY P.DEPARTAMENTO
                
                ";

        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        // Associar a data formatada ao placeholder SQL
        oci_bind_by_name($stmt, ":dataInicio", $DATAP_FORMATADA_inicio);
        oci_bind_by_name($stmt, ":datafim", $DATAP_FORMATADA_fim);
        oci_bind_by_name($stmt, ":codepto", $codepto);

        // Executa a consulta
        if (!oci_execute($stmt)) {
            $e = oci_error($stmt);
            $this->logger->error("Erro ao executar a consulta SQL: " . $e['message']);
            oci_free_statement($stmt);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro ao executar a consulta SQL'], 500);
        }

        // Coletar os resultados
        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) !== false) {
            $filiais[] = $row;
        }

        // Fechar a conexão
        oci_free_statement($stmt);
        oci_close($conexao);

        // Verificar se há resultados
        if (empty($filiais)) {
            $this->logger->info("Nenhum resultado encontrado.");
            return $response->withJson(['message' => 'Nenhum dado encontrado'], 404);
        }

        // Convertendo resultados para UTF-8
        foreach ($filiais as &$filial) {
            array_walk_recursive($filial, function (&$item) {
                if (!mb_detect_encoding($item, 'utf-8', true)) {
                    $item = utf8_encode($item);
                }
            });
        }

        $this->logger->info("Consulta executada com sucesso.");

        // Retornar resultados em JSON
        return $response->withJson($filiais);
    });

    $this->get('/relatorio/transferencia/{dataInicio}/{datafim}', function (Request $request, Response $response) {

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

        // Obtém parâmetros da rota
        $dataInicio = $request->getAttribute('dataInicio');
        $datafim = $request->getAttribute('datafim');

        // Função para converter data para formato 'YYYY-MM-DD'
        function formatarData($data)
        {
            $dataConvertida = DateTime::createFromFormat('d-M-Y', $data);
            if ($dataConvertida !== false) {
                return $dataConvertida->format('Y-m-d');
            }
            $dataConvertida = DateTime::createFromFormat('Y-m-d', $data);
            if ($dataConvertida !== false) {
                return $dataConvertida->format('Y-m-d');
            }
            return $data;
        }

        $DATAP_FORMATADA_inicio = formatarData($dataInicio);
        $DATAP_FORMATADA_fim = formatarData($datafim);

        $sql = " WITH RECEBIMENTO AS (
                    SELECT  
                        M.DTMOV,
                        M.CODFILIAL,
                         M.CODCLI AS RECEBEDOR,
                        M.QT,
                        M.CODOPER, 
                        M.NUMNOTA, 
                        M.CODPROD,
                        P.DESCRICAO AS PRODUTO,
                        M.QT AS QT_RECEBIDO, 
                        B.NUMBONUS, 
                        B.CODFUNCRM AS COLETOU_BONUS, 
                        M.CODFUNCLANC AS DEU_ENTRADA, 
                        B.CODFUNCBONUS AS USUARIO_MONTOU_BONUS, 
                        B.CODFUNCFECHA AS FECHOU_BONUS
                    FROM 
                        PCMOV M
                    JOIN 
                        PCPRODUT P ON M.CODPROD = P.CODPROD
                    JOIN 
                        PCFORNEC F ON M.CODFORNEC = F.CODFORNEC
                    JOIN 
                        PCDEPTO D ON M.CODEPTO = D.CODEPTO
                    LEFT JOIN 
                        PCBONUSC B ON M.NUMBONUS = B.NUMBONUS
                    WHERE 
                        M.DTMOV BETWEEN TO_DATE(:dataInicio, 'YYYY-MM-DD') AND TO_DATE(:datafim, 'YYYY-MM-DD')
                        AND M.CODOPER IN ('ET', 'ST')
                ),
    
                FUNCIONARIOS AS (
                    SELECT F.MATRICULA, F.NOME 
                    FROM PCEMPR F
                ),
    
                QTRECEBIDA AS (
                    SELECT NUMBONUS, CODPROD, MAX(QTENTRADA) AS QTENTRADA, MAX(QTNF) AS QTNF
                    FROM PCBONUSI
                    GROUP BY NUMBONUS, CODPROD
                )
    
                SELECT 
                        R.DTMOV AS DATA,
                         R.CODFILIAL,
                         R.RECEBEDOR,
                        R.NUMNOTA,
                        R.NUMBONUS,
                        R.CODPROD,
                        R.PRODUTO,
                        R.CODOPER,
                         R.QT AS QTNF,
                        COALESCE(QR.QTENTRADA, 0) AS QTENTRADA,
                        R.NUMNOTA,
                        CASE 
                            WHEN R.CODOPER = 'ET' THEN R.DEU_ENTRADA 
                            ELSE NULL 
                        END AS DEU_ENTRADA,
                        CASE 
                            WHEN R.CODOPER = 'ST' THEN R.DEU_ENTRADA 
                            ELSE NULL 
                        END AS FEZ_PEDIDO,
                        
                        CASE 
                            WHEN R.CODOPER = 'ET' THEN F2.NOME
                            ELSE NULL 
                        END AS NOME_DEU_ENTRADA,
                        CASE 
                            WHEN R.CODOPER = 'ST' THEN F1.NOME 
                            ELSE NULL 
                        END AS NOME_MONTOU_PEDIDO,

                        R.USUARIO_MONTOU_BONUS, 
                        F3.NOME AS NOME_USUARIO_MONTOU_BONUS,
                        R.COLETOU_BONUS, 
                        F4.NOME AS NOME_COLETOU_BONUS,
                        R.FECHOU_BONUS, 
                        F5.NOME AS NOME_FECHOU_BONUS
                    FROM 
                    RECEBIMENTO R
                LEFT JOIN 
                    FUNCIONARIOS F1 ON R.DEU_ENTRADA = F1.MATRICULA
                LEFT JOIN 
                    FUNCIONARIOS F2 ON R.DEU_ENTRADA = F2.MATRICULA
                LEFT JOIN 
                    FUNCIONARIOS F3 ON R.USUARIO_MONTOU_BONUS = F3.MATRICULA
                LEFT JOIN 
                    FUNCIONARIOS F4 ON R.COLETOU_BONUS = F4.MATRICULA
                LEFT JOIN 
                    FUNCIONARIOS F5 ON R.FECHOU_BONUS = F5.MATRICULA
                LEFT JOIN 
                    QTRECEBIDA QR ON R.NUMBONUS = QR.NUMBONUS AND R.CODPROD = QR.CODPROD
                ORDER BY R.NUMNOTA, R.CODPROD
                ";

        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        oci_bind_by_name($stmt, ":dataInicio", $DATAP_FORMATADA_inicio);
        oci_bind_by_name($stmt, ":datafim", $DATAP_FORMATADA_fim);

        if (!oci_execute($stmt)) {
            $e = oci_error($stmt);
            $this->logger->error("Erro ao executar a consulta SQL: " . $e['message']);
            oci_free_statement($stmt);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro ao executar a consulta SQL'], 500);
        }

        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) !== false) {
            $row['NUMNOTA'] = isset($row['NUMNOTA']) ? (int)$row['NUMNOTA'] : null;
            $row['NUMBONUS'] = isset($row['NUMBONUS']) ? (int)$row['NUMBONUS'] : null;
            $row['CODPROD'] = isset($row['CODPROD']) ? (int)$row['CODPROD'] : null;
            $row['CODFILIAL'] = isset($row['CODFILIAL']) ? (int)$row['CODFILIAL'] : null;
            $row['RECEBEDOR'] = isset($row['RECEBEDOR']) ? (int)$row['RECEBEDOR'] : null;
            $row['QTNF'] = isset($row['QTNF']) ? (float)$row['QTNF'] : null;
            $row['QTENTRADA'] = isset($row['QTENTRADA']) ? (float)$row['QTENTRADA'] : null;
            $row['DEU_ENTRADA'] = isset($row['DEU_ENTRADA']) ? (int)$row['DEU_ENTRADA'] : null;
            $row['FEZ_PEDIDO'] = isset($row['FEZ_PEDIDO']) ? (int)$row['FEZ_PEDIDO'] : null;
            $row['USUARIO_MONTOU_BONUS'] = isset($row['USUARIO_MONTOU_BONUS']) ? (int)$row['USUARIO_MONTOU_BONUS'] : null;
            $row['COLETOU_BONUS'] = isset($row['COLETOU_BONUS']) ? (int)$row['COLETOU_BONUS'] : null;
            $row['FECHOU_BONUS'] = isset($row['FECHOU_BONUS']) ? (int)$row['FECHOU_BONUS'] : null;

            $filiais[] = $row;
        }

        oci_free_statement($stmt);
        oci_close($conexao);

        if (empty($filiais)) {
            $this->logger->info("Nenhum resultado encontrado.");
            return $response->withJson(['message' => 'Nenhum dado encontrado'], 404);
        }

        foreach ($filiais as &$filial) {
            array_walk_recursive($filial, function (&$item) {
                if (!mb_detect_encoding($item, 'UTF-8', true)) {
                    $item = utf8_encode($item);
                }
            });
        }


        return $response->withJson($filiais);
    });

    $this->get('/relatorio/especificacao/tranferencia/{numnota}', function (Request $request, Response $response) {

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
    
        // Obtém parâmetros da rota
        $numnota = $request->getAttribute('numnota');
    
        // SQL sem data
        $sql = "WITH ENTRADA AS (
                SELECT F.NOME, M.DTMOV, M.NUMNOTA, M.CODPROD FROM PCMOV M, PCEMPR F
                WHERE M.CODFUNCLANC = F.MATRICULA
                AND M.NUMNOTA = :numnota
                AND M.CODOPER IN ('ET')
                ),
                
                SAIDA AS (
                
                SELECT F.NOME, M.DTMOV, M.NUMNOTA, M.CODPROD FROM PCMOV M, PCEMPR F
                WHERE M.CODFUNCLANC = F.MATRICULA
                AND M.NUMNOTA = :numnota
                AND M.CODOPER IN ( 'ST')
                
                )
                
                SELECT S.NUMNOTA, S.DTMOV AS DATA_SAIDA,  S.NOME AS FEZ_PEDIDO, E.DTMOV AS DATA_ENTRADA, E.NOME AS DEU_ENTRADA
                FROM  ENTRADA E, SAIDA S
                WHERE E.CODPROD = S.CODPROD
        ";
    
        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }
    
        oci_bind_by_name($stmt, ":numnota", $numnota);
    
        if (!oci_execute($stmt)) {
            $e = oci_error($stmt);
            $this->logger->error("Erro ao executar a consulta SQL: " . $e['message']);
            oci_free_statement($stmt);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro ao executar a consulta SQL'], 500);
        }
    
        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) !== false) {
            $filiais[] = $row;
        }
    
        oci_free_statement($stmt);
        oci_close($conexao);
    
        // Encodificando para UTF-8, caso necessário
        foreach ($filiais as &$filial) {
            array_walk_recursive($filial, function (&$item) {
                if (!mb_detect_encoding($item, 'UTF-8', true)) {
                    $item = utf8_encode($item);
                }
            });
        }
    
        return $response->withJson($filiais);
    });
    

    $this->get('/relatorio/entrada/{dataInicio}/{datafim}', function (Request $request, Response $response) {

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

        // Obtém parâmetros da rota
        $dataInicio = $request->getAttribute('dataInicio');
        $datafim = $request->getAttribute('datafim');

        // Função para converter data para formato 'YYYY-MM-DD'
        function formatarData($data)
        {
            $dataConvertida = DateTime::createFromFormat('d-M-Y', $data);
            if ($dataConvertida !== false) {
                return $dataConvertida->format('Y-m-d');
            }
            $dataConvertida = DateTime::createFromFormat('Y-m-d', $data);
            if ($dataConvertida !== false) {
                return $dataConvertida->format('Y-m-d');
            }
            return $data;
        }

        $DATAP_FORMATADA_inicio = formatarData($dataInicio);
        $DATAP_FORMATADA_fim = formatarData($datafim);

        $sql = " WITH RECEBIMENTO AS (
                    SELECT  
                        M.DTMOV,
                        M.CODFILIAL,
                        M.CODCLI AS RECEBEDOR,
                        M.QT,
                        M.CODOPER, 
                        M.NUMNOTA,
                        M.CODFORNEC, 
                        F.FORNECEDOR,
                        M.CODPROD,
                        P.DESCRICAO AS PRODUTO,
                        M.QT AS QT_RECEBIDO, 
                        B.NUMBONUS, 
                        B.CODFUNCRM AS COLETOU_BONUS, 
                        M.CODFUNCLANC AS DEU_ENTRADA, 
                        B.CODFUNCBONUS AS USUARIO_MONTOU_BONUS, 
                        B.CODFUNCFECHA AS FECHOU_BONUS
                    FROM 
                        PCMOV M
                    JOIN 
                        PCPRODUT P ON M.CODPROD = P.CODPROD
                    JOIN 
                        PCFORNEC F ON M.CODFORNEC = F.CODFORNEC
                    JOIN 
                        PCDEPTO D ON M.CODEPTO = D.CODEPTO
                    LEFT JOIN 
                        PCBONUSC B ON M.NUMBONUS = B.NUMBONUS
                    WHERE 
                        M.DTMOV  BETWEEN TO_DATE(:dataInicio, 'YYYY-MM-DD') AND TO_DATE(:datafim, 'YYYY-MM-DD')
                        AND M.CODOPER IN ('E')
                ),
                FUNCIONARIOS AS (
                    SELECT F.MATRICULA, F.NOME 
                    FROM PCEMPR F
                ),
                QTRECEBIDA AS (
                    SELECT NUMBONUS, CODPROD, MAX(QTENTRADA) AS QTENTRADA, MAX(QTNF) AS QTNF
                    FROM PCBONUSI
                    GROUP BY NUMBONUS, CODPROD
                )

                SELECT 
                    R.DTMOV AS DATA,
                    R.CODFILIAL,
                    R.NUMNOTA,
                    R.CODFORNEC, 
                    R.FORNECEDOR,
                    R.NUMBONUS,
                    SUM(R.QT) AS TOTAL_QTNF,
                    SUM(COALESCE(QR.QTENTRADA, 0)) AS TOTAL_QTENTRADA,
                    CASE 
                        WHEN R.CODOPER = 'E' THEN R.DEU_ENTRADA 
                        ELSE NULL 
                    END AS DEU_ENTRADA,
                    CASE 
                        WHEN R.CODOPER = 'E' THEN F2.NOME
                        ELSE NULL 
                    END AS NOME_DEU_ENTRADA,
                
                    R.USUARIO_MONTOU_BONUS, 
                    F3.NOME AS NOME_USUARIO_MONTOU_BONUS,
                    R.COLETOU_BONUS, 
                    F4.NOME AS NOME_COLETOU_BONUS,
                    R.FECHOU_BONUS, 
                    F5.NOME AS NOME_FECHOU_BONUS
                FROM 
                    RECEBIMENTO R

                LEFT JOIN 
                    FUNCIONARIOS F2 ON R.DEU_ENTRADA = F2.MATRICULA AND R.CODOPER = 'E'
                LEFT JOIN 
                    FUNCIONARIOS F3 ON R.USUARIO_MONTOU_BONUS = F3.MATRICULA
                LEFT JOIN 
                    FUNCIONARIOS F4 ON R.COLETOU_BONUS = F4.MATRICULA
                LEFT JOIN 
                    FUNCIONARIOS F5 ON R.FECHOU_BONUS = F5.MATRICULA
                LEFT JOIN 
                    QTRECEBIDA QR ON R.NUMBONUS = QR.NUMBONUS AND R.CODPROD = QR.CODPROD

                GROUP BY 
                    R.NUMNOTA, 
                    R.DTMOV, 
                    R.CODFILIAL,
                    R.CODFORNEC, 
                    R.FORNECEDOR,
                    R.NUMBONUS,
                    R.CODOPER,      -- Incluído para evitar erros de agrupamento
                    R.DEU_ENTRADA,
                    R.USUARIO_MONTOU_BONUS, 
                    R.COLETOU_BONUS, 
                    R.FECHOU_BONUS,

                    F2.NOME, 
                    F3.NOME, 
                    F4.NOME, 
                    F5.NOME
                ORDER BY 
                    R.NUMNOTA

                ";

        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        oci_bind_by_name($stmt, ":dataInicio", $DATAP_FORMATADA_inicio);
        oci_bind_by_name($stmt, ":datafim", $DATAP_FORMATADA_fim);

        if (!oci_execute($stmt)) {
            $e = oci_error($stmt);
            $this->logger->error("Erro ao executar a consulta SQL: " . $e['message']);
            oci_free_statement($stmt);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro ao executar a consulta SQL'], 500);
        }

        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) !== false) {
            $row['NUMNOTA'] = isset($row['NUMNOTA']) ? (int)$row['NUMNOTA'] : null;
            $row['NUMBONUS'] = isset($row['NUMBONUS']) ? (int)$row['NUMBONUS'] : null;
            $row['CODFILIAL'] = isset($row['CODFILIAL']) ? (int)$row['CODFILIAL'] : null;
            $row['CODFORNEC'] = isset($row['CODFORNEC']) ? (int)$row['CODFORNEC'] : null;

            $row['TOTAL_QTNF'] = isset($row['TOTAL_QTNF']) ? (float)$row['TOTAL_QTNF'] : null;
            $row['TOTAL_QTENTRADA'] = isset($row['TOTAL_QTENTRADA']) ? (float)$row['TOTAL_QTENTRADA'] : null;
            $row['DEU_ENTRADA'] = isset($row['DEU_ENTRADAV']) ? (float)$row['DEU_ENTRADA'] : null;


            $row['USUARIO_MONTOU_BONUS'] = isset($row['USUARIO_MONTOU_BONUS']) ? (int)$row['USUARIO_MONTOU_BONUS'] : null;
            $row['COLETOU_BONUS'] = isset($row['COLETOU_BONUS']) ? (int)$row['COLETOU_BONUS'] : null;
            $row['FECHOU_BONUS'] = isset($row['FECHOU_BONUS']) ? (int)$row['FECHOU_BONUS'] : null;

            $filiais[] = $row;
        }

        oci_free_statement($stmt);
        oci_close($conexao);

        if (empty($filiais)) {
            $this->logger->info("Nenhum resultado encontrado.");
            return $response->withJson(['message' => 'Nenhum dado encontrado'], 404);
        }

        foreach ($filiais as &$filial) {
            array_walk_recursive($filial, function (&$item) {
                if (!mb_detect_encoding($item, 'UTF-8', true)) {
                    $item = utf8_encode($item);
                }
            });
        }


        return $response->withJson($filiais);
    });
});
