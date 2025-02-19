<?php

use Slim\Http\Request;
use Slim\Http\Response;

use Symfony\Component\Console\Descriptor\Descriptor;

// Rota para listar os dados de PCFILIAL
$app->group('/api/v1', function () {

    // TELEGRAM
    $this->get('/financeiro/venda/atual', function (Request $request, Response $response) {
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

        // Consulta SQL com GROUP BY
        $sql = "SELECT
                ROUND(SUM(M.QT * M.CUSTOFIN), 2) AS CUSTO,
                ROUND(SUM(M.QT * M.PUNIT), 2) AS VENDA,

                COUNT(DISTINCT M.NUMTRANSVENDA) AS NUMVENDAS,
                
                ROUND(SUM(M.QT * M.PUNIT) - SUM(M.QT * M.CUSTOFIN), 2) AS LUCRO,
                
                ROUND(SUM(M.QT * M.PUNIT) / COUNT(DISTINCT M.NUMTRANSVENDA), 2) AS TICKET_MEDIO,
                
                CASE 
                    WHEN SUM(M.QT * M.PUNIT) = 0 THEN 0 
                    ELSE ROUND((((SUM(M.QT * M.PUNIT) - SUM(M.QT * M.CUSTOFIN)) * 100) / SUM(M.QT * M.PUNIT)), 2) 
                END AS MARGEM

            FROM
                PCMOV M
               
            WHERE M.DTMOV >= TRUNC(SYSDATE)
  				AND M.DTMOV < TRUNC(SYSDATE) + 1

                AND M.CODOPER IN ('S', 'SB')
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
          
            $row['CUSTO'] = isset($row['CUSTO']) ? (float)$row['CUSTO'] : null;
            $row['VENDA'] = isset($row['VENDA']) ? (float)$row['VENDA'] : null;
            $row['NUMVENDAS'] = isset($row['NUMVENDAS']) ? (float)$row['NUMVENDAS'] : null;
            $row['LUCRO'] = isset($row['LUCRO']) ? (float)$row['LUCRO'] : null;
            $row['TICKET_MEDIO'] = isset($row['TICKET_MEDIO']) ? (float)$row['TICKET_MEDIO'] : null;
            $row['MARGEM'] = isset($row['MARGEM']) ? (float)$row['MARGEM'] : null;

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
    $this->get('/financeiro/relatorio/atual', function (Request $request, Response $response) {
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

        // Consulta SQL com GROUP BY
        $sql = "WITH VENDA AS (

                SELECT
                                M.CODFILIAL,
                                ROUND(SUM(M.QT * M.CUSTOFIN), 2) AS CUSTO,
                                ROUND(SUM(M.QT * M.PUNIT), 2) AS VENDA,

                                COUNT(DISTINCT M.NUMTRANSVENDA) AS NUMVENDAS,
                                
                                ROUND(SUM(M.QT * M.PUNIT) - SUM(M.QT * M.CUSTOFIN), 2) AS LUCRO,
                                
                                ROUND(SUM(M.QT * M.PUNIT) / COUNT(DISTINCT M.NUMTRANSVENDA), 2) AS TICKET_MEDIO,
                                
                                CASE 
                                    WHEN SUM(M.QT * M.PUNIT) = 0 THEN 0 
                                    ELSE ROUND((((SUM(M.QT * M.PUNIT) - SUM(M.QT * M.CUSTOFIN)) * 100) / SUM(M.QT * M.PUNIT)), 2) 
                                END AS MARGEM

                            FROM
                                PCMOV M
                            
                            WHERE M.DTMOV >= TRUNC(SYSDATE)
                                AND M.DTMOV < TRUNC(SYSDATE) + 1

                                AND M.CODOPER IN ('S', 'SB')
                GROUP BY M.CODFILIAL
                ORDER BY M.CODFILIAL

                ),
                META AS (

                SELECT 
                                    
                                    PCMETASUP.CODFILIAL,
                                    SUM(PCMETASUP.VLVENDAPREV) META
                                FROM 
                                    PCMETASUP
                                WHERE 
                                    PCMETASUP.DATA >= TRUNC(SYSDATE)
                                AND 
                                    PCMETASUP.DATA < TRUNC(SYSDATE) + 1
                                GROUP BY 
                                    PCMETASUP.CODFILIAL
                                ORDER BY 
                                    PCMETASUP.CODFILIAL
                                    
                )


                SELECT 
                    V.*,
                    M.META,
                    ROUND((V.VENDA / NULLIF(M.META, 0)) * 100, 2) AS PERCENTUAL_CARREGADO
                FROM 
                    VENDA V
                JOIN 
                    META M ON V.CODFILIAL = M.CODFILIAL

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
          
            $row['CUSTO'] = isset($row['CUSTO']) ? (float)$row['CUSTO'] : null;
            $row['VENDA'] = isset($row['VENDA']) ? (float)$row['VENDA'] : null;
            $row['NUMVENDAS'] = isset($row['NUMVENDAS']) ? (float)$row['NUMVENDAS'] : null;
            $row['LUCRO'] = isset($row['LUCRO']) ? (float)$row['LUCRO'] : null;
            $row['TICKET_MEDIO'] = isset($row['TICKET_MEDIO']) ? (float)$row['TICKET_MEDIO'] : null;
            $row['MARGEM'] = isset($row['MARGEM']) ? (float)$row['MARGEM'] : null;
            $row['META'] = isset($row['META']) ? (float)$row['META'] : null;
            $row['PERCENTUAL_CARREGADO'] = isset($row['PERCENTUAL_CARREGADO']) ? (float)$row['PERCENTUAL_CARREGADO'] : null;

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

    $this->get('/financeiro/relatorio/detalhado/{datainicio}/{datafim}', function (Request $request, Response $response) {
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

        $datainicio = $request->getAttribute('datainicio');
        $datafim = $request->getAttribute('datafim');

        // Consulta SQL com GROUP BY
        $sql = "WITH VENDA AS (

                SELECT
                                M.CODFILIAL,
                                ROUND(SUM(M.QT * M.CUSTOFIN), 2) AS CUSTO,
                                ROUND(SUM(M.QT * M.PUNIT), 2) AS VENDA,

                                COUNT(DISTINCT M.NUMTRANSVENDA) AS NUMVENDAS,
                                
                                ROUND(SUM(M.QT * M.PUNIT) - SUM(M.QT * M.CUSTOFIN), 2) AS LUCRO,
                                
                                ROUND(SUM(M.QT * M.PUNIT) / COUNT(DISTINCT M.NUMTRANSVENDA), 2) AS TICKET_MEDIO,
                                
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
                                AND M.DTMOV BETWEEN TO_DATE(:datainicio, 'DD-MM-YYYY') AND TO_DATE(:datafim, 'DD-MM-YYYY')
                                AND M.CODOPER IN ('S', 'SB')
                            GROUP BY
                                M.CODFILIAL              
                                ORDER BY M.CODFILIAL

                ),


                META AS (
                    SELECT 
                        PCMETASUP.CODFILIAL,
                        SUM(PCMETASUP.VLVENDAPREV) META
                        FROM PCMETASUP
                        WHERE PCMETASUP.DATA BETWEEN TO_DATE(:datainicio, 'DD-MM-YYYY') AND TO_DATE(:datafim, 'DD-MM-YYYY')
                        GROUP BY 
                                                    PCMETASUP.CODFILIAL
                                                ORDER BY 
                                                    PCMETASUP.CODFILIAL
                                                    
                                )



                SELECT * FROM VENDA V, META M
                WHERE V.CODFILIAL = M.CODFILIAL
        ";



        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            // Log de erro na preparação da consulta SQL
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        oci_bind_by_name($stmt, ":datainicio", $datainicio);
        oci_bind_by_name($stmt, ":datafim", $datafim);

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
            $row['CODFILIAL'] = isset($row['CODFILIAL']) ? (int)$row['CODFILIAL'] : null;
            $row['CUSTO'] = isset($row['CUSTO']) ? (float)$row['CUSTO'] : null;
            $row['VENDA'] = isset($row['VENDA']) ? (float)$row['VENDA'] : null;
            $row['NUMVENDAS'] = isset($row['NUMVENDAS']) ? (float)$row['NUMVENDAS'] : null;
            $row['LUCRO'] = isset($row['LUCRO']) ? (float)$row['LUCRO'] : null;
            $row['TICKET_MEDIO'] = isset($row['TICKET_MEDIO']) ? (float)$row['TICKET_MEDIO'] : null;
            $row['MARGEM'] = isset($row['MARGEM']) ? (float)$row['MARGEM'] : null;
            $row['META'] = isset($row['META']) ? (float)$row['META'] : null;

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


    $this->get('/financeiro/mes/venda/atual', function (Request $request, Response $response) {
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

        // Consulta SQL com GROUP BY
        $sql = "SELECT
                ROUND(SUM(M.QT * M.CUSTOFIN), 2) AS CUSTO,
                ROUND(SUM(M.QT * M.PUNIT), 2) AS VENDA,

                COUNT(DISTINCT M.NUMTRANSVENDA) AS NUMVENDAS,
                
                ROUND(SUM(M.QT * M.PUNIT) - SUM(M.QT * M.CUSTOFIN), 2) AS LUCRO,
                
                ROUND(SUM(M.QT * M.PUNIT) / COUNT(DISTINCT M.NUMTRANSVENDA), 2) AS TICKET_MEDIO,
                
                CASE 
                    WHEN SUM(M.QT * M.PUNIT) = 0 THEN 0 
                    ELSE ROUND((((SUM(M.QT * M.PUNIT) - SUM(M.QT * M.CUSTOFIN)) * 100) / SUM(M.QT * M.PUNIT)), 2) 
                END AS MARGEM

            FROM
                PCMOV M
               
            WHERE M.DTMOV >= TRUNC(SYSDATE, 'MM') 
                    AND M.DTMOV < ADD_MONTHS(TRUNC(SYSDATE, 'MM'), 1)


                AND M.CODOPER IN ('S', 'SB')
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
          
            $row['CUSTO'] = isset($row['CUSTO']) ? (float)$row['CUSTO'] : null;
            $row['VENDA'] = isset($row['VENDA']) ? (float)$row['VENDA'] : null;
            $row['NUMVENDAS'] = isset($row['NUMVENDAS']) ? (float)$row['NUMVENDAS'] : null;
            $row['LUCRO'] = isset($row['LUCRO']) ? (float)$row['LUCRO'] : null;
            $row['TICKET_MEDIO'] = isset($row['TICKET_MEDIO']) ? (float)$row['TICKET_MEDIO'] : null;
            $row['MARGEM'] = isset($row['MARGEM']) ? (float)$row['MARGEM'] : null;

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
    $this->get('/financeiro/mes/relatorio/atual', function (Request $request, Response $response) {
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

        // Consulta SQL com GROUP BY
        $sql = "WITH VENDA AS (

                SELECT
                                M.CODFILIAL,
                                ROUND(SUM(M.QT * M.CUSTOFIN), 2) AS CUSTO,
                                ROUND(SUM(M.QT * M.PUNIT), 2) AS VENDA,

                                COUNT(DISTINCT M.NUMTRANSVENDA) AS NUMVENDAS,
                                
                                ROUND(SUM(M.QT * M.PUNIT) - SUM(M.QT * M.CUSTOFIN), 2) AS LUCRO,
                                
                                ROUND(SUM(M.QT * M.PUNIT) / COUNT(DISTINCT M.NUMTRANSVENDA), 2) AS TICKET_MEDIO,
                                
                                CASE 
                                    WHEN SUM(M.QT * M.PUNIT) = 0 THEN 0 
                                    ELSE ROUND((((SUM(M.QT * M.PUNIT) - SUM(M.QT * M.CUSTOFIN)) * 100) / SUM(M.QT * M.PUNIT)), 2) 
                                END AS MARGEM

                            FROM
                                PCMOV M
                            
                            WHERE M.DTMOV >= TRUNC(SYSDATE, 'MM') 
                             AND M.DTMOV < ADD_MONTHS(TRUNC(SYSDATE, 'MM'), 1)


                                AND M.CODOPER IN ('S', 'SB')
                GROUP BY M.CODFILIAL
                ORDER BY M.CODFILIAL

                ),
                META AS (

                SELECT 
                                    
                                    PCMETASUP.CODFILIAL,
                                    SUM(PCMETASUP.VLVENDAPREV) META
                                FROM 
                                    PCMETASUP
                               WHERE 
                                    PCMETASUP.DATA >= TRUNC(SYSDATE, 'MM') 
                                AND 
                                    PCMETASUP.DATA < ADD_MONTHS(TRUNC(SYSDATE, 'MM'), 1)

                                GROUP BY 
                                    PCMETASUP.CODFILIAL
                                ORDER BY 
                                    PCMETASUP.CODFILIAL
                                    
                )


                SELECT 
                    V.*,
                    M.META,
                    ROUND((V.VENDA / NULLIF(M.META, 0)) * 100, 2) AS PERCENTUAL_CARREGADO
                FROM 
                    VENDA V
                JOIN 
                    META M ON V.CODFILIAL = M.CODFILIAL

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
          
            $row['CUSTO'] = isset($row['CUSTO']) ? (float)$row['CUSTO'] : null;
            $row['VENDA'] = isset($row['VENDA']) ? (float)$row['VENDA'] : null;
            $row['NUMVENDAS'] = isset($row['NUMVENDAS']) ? (float)$row['NUMVENDAS'] : null;
            $row['LUCRO'] = isset($row['LUCRO']) ? (float)$row['LUCRO'] : null;
            $row['TICKET_MEDIO'] = isset($row['TICKET_MEDIO']) ? (float)$row['TICKET_MEDIO'] : null;
            $row['MARGEM'] = isset($row['MARGEM']) ? (float)$row['MARGEM'] : null;
            $row['META'] = isset($row['META']) ? (float)$row['META'] : null;
            $row['PERCENTUAL_CARREGADO'] = isset($row['PERCENTUAL_CARREGADO']) ? (float)$row['PERCENTUAL_CARREGADO'] : null;

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


    $this->get('/financeiro/telegram/{datainicio}/{datafim}', function (Request $request, Response $response) {
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

        $datainicio = $request->getAttribute('datainicio');
        $datafim = $request->getAttribute('datafim');

        // Consulta SQL com GROUP BY
        $sql = "SELECT
                M.CODFILIAL,
                ROUND(SUM(M.QT * M.CUSTOFIN), 2) AS CUSTO,
                ROUND(SUM(M.QT * M.PUNIT), 2) AS VENDA,

                COUNT(DISTINCT M.NUMTRANSVENDA) AS NUMVENDAS,
                
                ROUND(SUM(M.QT * M.PUNIT) - SUM(M.QT * M.CUSTOFIN), 2) AS LUCRO,
                
                ROUND(SUM(M.QT * M.PUNIT) / COUNT(DISTINCT M.NUMTRANSVENDA), 2) AS TICKET_MEDIO,
                
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
                AND M.DTMOV BETWEEN TO_DATE(:datainicio, 'DD-MM-YYYY') AND TO_DATE(:datafim, 'DD-MM-YYYY')
                AND M.CODOPER IN ('S', 'SB')
            GROUP BY
				M.CODFILIAL              
                ORDER BY M.CODFILIAL";



        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            // Log de erro na preparação da consulta SQL
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        oci_bind_by_name($stmt, ":datainicio", $datainicio);
        oci_bind_by_name($stmt, ":datafim", $datafim);

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
            $row['CODFILIAL'] = isset($row['CODFILIAL']) ? (int)$row['CODFILIAL'] : null;
            $row['CUSTO'] = isset($row['CUSTO']) ? (float)$row['CUSTO'] : null;
            $row['VENDA'] = isset($row['VENDA']) ? (float)$row['VENDA'] : null;
            $row['NUMVENDAS'] = isset($row['NUMVENDAS']) ? (float)$row['NUMVENDAS'] : null;
            $row['LUCRO'] = isset($row['LUCRO']) ? (float)$row['LUCRO'] : null;
            $row['TICKET_MEDIO'] = isset($row['TICKET_MEDIO']) ? (float)$row['TICKET_MEDIO'] : null;
            $row['MARGEM'] = isset($row['MARGEM']) ? (float)$row['MARGEM'] : null;

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

    $this->get('/financeiro/telegram/dia/{datainicio}/{datafim}', function (Request $request, Response $response) {
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

        $datainicio = $request->getAttribute('datainicio');
        $datafim = $request->getAttribute('datafim');


        // Consulta SQL com GROUP BY
        $sql = "SELECT
                M.CODFILIAL,
                ROUND(SUM(M.QT * M.CUSTOFIN), 2) AS CUSTO,
                ROUND(SUM(M.QT * M.PUNIT), 2) AS VENDA,

                COUNT(DISTINCT M.NUMTRANSVENDA) AS NUMVENDAS,
                
                ROUND(SUM(M.QT * M.PUNIT) - SUM(M.QT * M.CUSTOFIN), 2) AS LUCRO,
                
                ROUND(SUM(M.QT * M.PUNIT) / COUNT(DISTINCT M.NUMTRANSVENDA), 2) AS TICKET_MEDIO,
                
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
                AND M.DTMOV BETWEEN TO_DATE(:datainicio, 'DD-MM-YYYY') AND TO_DATE(:datafim, 'DD-MM-YYYY')
                AND M.CODOPER IN ('S', 'SB')
            GROUP BY
				M.CODFILIAL              
                ORDER BY M.CODFILIAL";



        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            // Log de erro na preparação da consulta SQL
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        oci_bind_by_name($stmt, ":datainicio", $datainicio);
        oci_bind_by_name($stmt, ":datafim", $datafim);
   

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
            $row['CODFILIAL'] = isset($row['CODFILIAL']) ? (int)$row['CODFILIAL'] : null;
            $row['CUSTO'] = isset($row['CUSTO']) ? (float)$row['CUSTO'] : null;
            $row['VENDA'] = isset($row['VENDA']) ? (float)$row['VENDA'] : null;
            $row['NUMVENDAS'] = isset($row['NUMVENDAS']) ? (float)$row['NUMVENDAS'] : null;
            $row['LUCRO'] = isset($row['LUCRO']) ? (float)$row['LUCRO'] : null;
            $row['TICKET_MEDIO'] = isset($row['TICKET_MEDIO']) ? (float)$row['TICKET_MEDIO'] : null;
            $row['MARGEM'] = isset($row['MARGEM']) ? (float)$row['MARGEM'] : null;

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

    $this->get('/financeiro/meta/telegram/{datainicio}/{datafim}', function (Request $request, Response $response) {
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

        $datainicio = $request->getAttribute('datainicio');
        $datafim = $request->getAttribute('datafim');

        // Consulta SQL com GROUP BY
        $sql = "SELECT 
                    SUM(PCMETASUP.VLVENDAPREV) META,
                    PCMETASUP.CODFILIAL
                FROM 
                    PCMETASUP
                WHERE 
                    PCMETASUP.DATA BETWEEN TO_DATE(:datainicio, 'DD-MM-YYYY') AND TO_DATE(:datafim, 'DD-MM-YYYY')
                GROUP BY 
                    PCMETASUP.CODFILIAL
                ORDER BY 
                    PCMETASUP.CODFILIAL
        ";



        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            // Log de erro na preparação da consulta SQL
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        oci_bind_by_name($stmt, ":datainicio", $datainicio);
        oci_bind_by_name($stmt, ":datafim", $datafim);

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
            $row['CODFILIAL'] = isset($row['CODFILIAL']) ? (int)$row['CODFILIAL'] : null;
            $row['META'] = isset($row['META']) ? (float)$row['META'] : null;
           

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
    $this->get('/financeiro/meta/telegram/dia/{datainicio}/{datafim}', function (Request $request, Response $response) {
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

        $datainicio = $request->getAttribute('datainicio');
        $datafim = $request->getAttribute('datafim');

        // Consulta SQL com GROUP BY
        $sql = "SELECT 
                    SUM(PCMETASUP.VLVENDAPREV) META,
                    PCMETASUP.CODFILIAL
                FROM 
                    PCMETASUP
                WHERE 
                    PCMETASUP.DATA BETWEEN TO_DATE(:datainicio, 'DD-MM-YYYY') AND TO_DATE(:datafim, 'DD-MM-YYYY')
                GROUP BY 
                    PCMETASUP.CODFILIAL
                ORDER BY 
                    PCMETASUP.CODFILIAL
        ";



        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            // Log de erro na preparação da consulta SQL
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        oci_bind_by_name($stmt, ":datainicio", $datainicio);
        oci_bind_by_name($stmt, ":datafim", $datafim);

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
            $row['CODFILIAL'] = isset($row['CODFILIAL']) ? (int)$row['CODFILIAL'] : null;
            $row['META'] = isset($row['META']) ? (float)$row['META'] : null;
           

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
    $this->get('/metas/{ano}/{mes}', function (Request $request, Response $response) {

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

        // Obtém o parâmetro 'ano' da URL
        $ano = $request->getAttribute('ano');
        $mes = $request->getAttribute('mes');

        // Query SQL com o ano como parâmetro
        $sql = "SELECT 
                    sum(PCMETASUP.VLVENDAPREV) meta
                FROM 
                    PCMETASUP
                WHERE 
                    TO_CHAR(PCMETASUP.DATA, 'YYYY') = :ano
                AND TO_NUMBER(TO_CHAR(PCMETASUP.DATA, 'MM')) = :mes

                ";

        $stmt = oci_parse($conexao, $sql);

        // Vincula o parâmetro 'ano' à consulta
        oci_bind_by_name($stmt, ":ano", $ano);
        oci_bind_by_name($stmt, ":mes", $mes);

        // Executa a consulta
        oci_execute($stmt);

        // Coletar os resultados em um array
        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) != false) {
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

        return $response->withJson($filiais);
    });

    // avista
    $this->get('/financeiro/telegram/avista/{datainicio}/{datafim}', function (Request $request, Response $response) {
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

        $datainicio = $request->getAttribute('datainicio');
        $datafim = $request->getAttribute('datafim');

        // Consulta SQL com GROUP BY
        $sql = "SELECT SUM(VLTOTAL) AVISTA
            FROM PCPEDCECF
            WHERE DATA BETWEEN TO_DATE(:datainicio, 'DD-MM-YYYY') AND TO_DATE(:datafim, 'DD-MM-YYYY')
            AND CODCOB IN (
            'VIDE',
            'VDEB',
            'VDSB',
            'VDEC',
            'PXVD',
            'PIXM',
            'PXPB',
            'PIX',
            'SAFR',
            'MDEB',
            'MDPB',
            'MADC',
            'ELDE',
            'EDER',
            'EDPB',
            'ELOD',
            'D',
            'DBVL',
            'ALEE',
            'ALEA',
            'ALPS'
            )
            ";



        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            // Log de erro na preparação da consulta SQL
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        oci_bind_by_name($stmt, ":datainicio", $datainicio);
        oci_bind_by_name($stmt, ":datafim", $datafim);

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

            $row['AVISTA'] = isset($row['AVISTA']) ? (float)$row['AVISTA'] : null;

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
    $this->get('/financeiro/telegram/dia/avista/{datainicio}/{datafim}', function (Request $request, Response $response) {
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

        $datainicio = $request->getAttribute('datainicio');
        $datafim = $request->getAttribute('datafim');


        // Consulta SQL com GROUP BY
        $sql = "SELECT SUM(VLTOTAL) AVISTA
        FROM PCPEDCECF
        WHERE DATA BETWEEN TO_DATE(:datainicio, 'DD-MM-YYYY') AND TO_DATE(:datafim, 'DD-MM-YYYY')
        AND CODCOB IN (
        'VIDE',
        'VDEB',
        'VDSB',
        'VDEC',
        'PXVD',
        'PIXM',
        'PXPB',
        'PIX',
        'SAFR',
        'MDEB',
        'MDPB',
        'MADC',
        'ELDE',
        'EDER',
        'EDPB',
        'ELOD',
        'D',
        'DBVL',
        'ALEE',
        'ALEA',
        'ALPS'
        )
        ";



        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            // Log de erro na preparação da consulta SQL
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        oci_bind_by_name($stmt, ":datainicio", $datainicio);
        oci_bind_by_name($stmt, ":datafim", $datafim);
   

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
           
            $row['AVISTA'] = isset($row['AVISTA']) ? (float)$row['AVISTA'] : null;

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

    //////////////////////// FINANCEIRO ALERTA

    $this->get('/financeiro/alerta/tranferencia', function (Request $request, Response $response) {
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


        // Consulta SQL com GROUP BY
        $sql = "SELECT                                
                    PCNFSAID.DTSAIDA                                                        
                    , PCNFSAID.NUMNOTA                                                        
                    , PCNFSAID.NUMTRANSVENDA    , 
                    CASE
                            WHEN PCCLIENT.CODCLI = 4 THEN 1
                            WHEN PCCLIENT.CODCLI = 317 THEN 2
                            WHEN PCCLIENT.CODCLI = 5 THEN 3
                            WHEN PCCLIENT.CODCLI = 8 THEN 4
                            WHEN PCCLIENT.CODCLI = 1674 THEN 5
                    END FILIAL_ENTRADA                                                     
                    , PCPEDC.CODFILIAL    AS FILIAL_SAIDA                                                                                              
                    , PCNFSAID.VLTOTAL                                                                                
                FROM PCNFSAID                                                                
                    , PCPEDC                                                                  
                    , PCCLIENT                                                                
                    , PCFORNEC                                                                
                    , PCFILIAL                                                                
                    , PCNFENT                                                                 
                    , PCFORNEC PCFORNECSTGUIA                                                 
                    , PCPARCELASC PCPARCELASTGUIA                                             
                WHERE PCNFSAID.NUMPED    = PCPEDC.NUMPED                                      
                    AND PCNFSAID.CODFILIAL = PCPEDC.CODFILIAL                                   
                    AND PCFILIAL.CODIGO    = PCPEDC.CODFILIAL                                   
                    AND PCFILIAL.CODFORNEC = PCFORNEC.CODFORNEC                                 
                    AND PCCLIENT.CODCLI    = PCPEDC.CODCLI                                      
                    AND NVL(PCNFSAID.NOTADUPLIQUESVC, 'N') = 'N'                            
                    AND PCNFSAID.CONDVENDA   IN (9, 10)                                           
                                                      
                    AND ((PCNFSAID.ESPECIE <> 'NE')                                           
                    OR ((PCNFSAID.ESPECIE =  'NE') AND (PCNFSAID.TIPOEMISSAO <> 1)))         
                    AND PCNFSAID.DTCANCEL IS NULL                                               
                    AND PCFORNEC.CODFORNECSTGUIA  = PCFORNECSTGUIA.CODFORNEC(+)                 
                    AND PCFORNEC.CODPARCELASTGUIA = PCPARCELASTGUIA.CODPARCELA(+)               
                    AND PCNFSAID.NUMTRANSVENDA = PCNFENT.NUMTRANSVENDAORIG(+)                   
                    AND (PCNFENT.VLTOTAL(+) > 0)                                                
                    AND NOT EXISTS(SELECT 1                                                 
                                    FROM PCESTCOM                                          
                                    WHERE PCESTCOM.NUMTRANSVENDA = PCNFSAID.NUMTRANSVENDA   
                                    AND PCESTCOM.NUMTRANSENT >= (SELECT MIN(NUMTRANSENT) FROM PCESTCOM) AND NVL(PCESTCOM.VLDEVOLUCAO,0) > 0)  
                    AND PCNFENT.NUMTRANSVENDAORIG IS NULL                                   
                AND PCNFSAID.DTSAIDA <= SYSDATE -3
                ORDER BY PCNFSAID.DTSAIDA         
                        , PCNFSAID.NUMTRANSVENDA 
               
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
            $row['NUMNOTA'] = isset($row['NUMNOTA']) ? (int)$row['NUMNOTA'] : null;
            $row['NUMTRANSVENDA'] = isset($row['NUMTRANSVENDA']) ? (float)$row['NUMTRANSVENDA'] : null;
            $row['FILIAL_ENTRADA'] = isset($row['FILIAL_ENTRADA']) ? (float)$row['FILIAL_ENTRADA'] : null;
            $row['FILIAL_SAIDA'] = isset($row['FILIAL_SAIDA']) ? (float)$row['FILIAL_SAIDA'] : null;
            $row['VLTOTAL'] = isset($row['VLTOTAL']) ? (float)$row['VLTOTAL'] : null;
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











    $this->get('/financeiro/alerta/titulosvencidos', function (Request $request, Response $response) {
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


        // Consulta SQL com GROUP BY
        $sql = "WITH TITULOS_VENCIDOS AS (
                SELECT
                    P.CODFILIALNF AS FILIAL  ,
                    TRUNC(P.DTVENC) AS DTVENC,
                    P.CODCLI,
                    P.VALOR,
                    P.CODCOB
                FROM PCPREST P
                    , PCOPERADORACARTAO
                    , PCCOB B
                    , PCCLIENT C
                    , PCUSUARI U
                    , PCSUPERV S
                    , PCCOB COBORIG 
                    , PCREDECLIENTE R 
                    , PCFILIAL F 
                    ,SITEPRAZOFINANCEIRO SITE
                WHERE B.CODCOB = P.CODCOB 
                AND COBORIG.CODCOB(+) = P.CODCOBORIG 
                AND C.CODCLI = P.CODCLI 
                AND P.CODUSUR = U.CODUSUR  
                AND S.CODSUPERVISOR = U.CODSUPERVISOR   
                AND B.CODOPERADORACARTAO = PCOPERADORACARTAO.CODIGO(+)
                AND C.CODREDE = R.CODREDE(+) 
                AND P.CODFILIAL = F.CODIGO 
                AND SITE.TITLE = 'TITULOS VENCIDOS'
                AND       
                EXISTS( SELECT 1                                                       
                        FROM PCLIB                                                   
                        WHERE CODTABELA = TO_CHAR(8)                                 
                            AND (CODIGOA = NVL(P.CODCOB, CODIGOA) OR CODIGOA = '9999')                  
                            AND CODFUNC   = 1                                          
                            AND PCLIB.CODIGOA IS NOT NULL)                              
                AND       
                EXISTS( SELECT 1                                                       
                        FROM PCLIB                                                   
                        WHERE CODTABELA = TO_CHAR(1)                                 
                            AND (CODIGOA = NVL(P.CODFILIAL, CODIGOA) OR CODIGOA = '99')                  
                            AND CODFUNC   = 1                                          
                            AND PCLIB.CODIGOA IS NOT NULL)                              
                AND (P.CODFILIAL IN ( '1','2','3','4','5' )) 
                AND P.DTVENC <= SYSDATE - SITE.DAYS
                AND P.DTPAG IS NULL  
                AND P.CODCOB NOT IN ('DESD','CRED','DEVT','ESTR', 'CANC') 
                AND P.DTCANCEL IS NULL 
                ORDER BY P.DTVENC, P.CODCLI 
                )
                SELECT 

                    C.CLIENTE,
                    SUM(TV.VALOR) AS TOTAL
                    
                FROM TITULOS_VENCIDOS TV, PCCOB B, PCCLIENT C
                WHERE 1=1
                AND TV.CODCOB = B.CODCOB 
                AND TV.CODCLI = C.CODCLI 
                GROUP BY 
                    C.CLIENTE
                ORDER BY TOTAL DESC
               
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
            $row['CODCLI'] = isset($row['CODCLI']) ? (int)$row['CODCLI'] : null;
            $row['TOTAL'] = isset($row['TOTAL']) ? (float)$row['TOTAL'] : null;
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

    $this->get('/financeiro/alerta/conciliacao', function (Request $request, Response $response) {
        $settings = $this->get('settings')['db'];
        $dsn = $settings['dsn'];
        $username = $settings['username'];
        $password = $settings['password'];

        // Conectando ao Oracles
        $conexao = oci_connect($username, $password, $dsn);
        if (!$conexao) {
            $e = oci_error();
            // Log de erro no banco de dados
            $this->logger->error("Erro de conexão ao Oracle: " . $e['message']);
            return $response->withJson(['error' => 'Erro de conexão ao banco de dados'], 500);
        }


        // Consulta SQL com GROUP BY
        $sql = "WITH CONCILIACAO AS (
                SELECT 
                    PCMOVCR.DATA,
                    PCMOVCR.CODBANCO,
                    PCMOVCR.CODCOB,
                    PCMOVCR.VALOR,
                    ROW_NUMBER() OVER (PARTITION BY PCMOVCR.CODBANCO, PCMOVCR.CODCOB ORDER BY PCMOVCR.DATA ASC) AS RN
                FROM PCMOVCR, SITEPRAZOFINANCEIRO SITE
                WHERE 
                    ((CONCILIACAO <> 'OK') OR (CONCILIACAO IS NULL))
                    AND ((OPERACAO <> 99) OR (OPERACAO IS NULL))
                    AND SITE.TITLE = 'CONCILIAÇAO'
                    AND (NUMTRANS BETWEEN 0.000000 AND 9999999999999999999999999999999.000000)
                    AND (TRUNC(DATA) <= SYSDATE - SITE.DAYS)
                    AND DECODE(TIPO, 'D', VALOR, VALOR * -1) BETWEEN -9999999999999999999999999999999.000000 AND 9999999999999999999999999999999.000000
            )
            SELECT 
                C.DATA, 
                C.CODBANCO, 
                C.CODCOB, 
                C.VALOR, 
                B.NOME 
            FROM CONCILIACAO C
            JOIN PCBANCO B ON C.CODBANCO = B.CODBANCO
            WHERE C.RN = 1
            AND C.CODCOB NOT IN ('DNI', 'VALE', 'SANG')
             ORDER BY C.DATA ASC
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
            $row['CODBANCO'] = isset($row['CODBANCO']) ? (int)$row['CODBANCO'] : null;
            $row['VALOR'] = isset($row['VALOR']) ? (float)$row['VALOR'] : null;
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

    $this->get('/financeiro/alerta/contasapagar', function (Request $request, Response $response) {
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

    
        // Consulta SQL com GROUP BY
        $sql = "WITH CONTAS_A_PAGAR AS (
                    SELECT 
                        PCLANC.DTVENC,
                        
                        
                        (DECODE(PCLANC.TIPOPARCEIRO,
                                'F',(SELECT FORNECEDOR FROM PCFORNEC  WHERE CODFORNEC=PCLANC.CODFORNEC),
                                'R',(SELECT NOME FROM PCUSUARI WHERE CODUSUR=PCLANC.CODFORNEC),
                                'C',(SELECT CLIENTE FROM PCCLIENT WHERE CODCLI=PCLANC.CODFORNEC),
                                'M',(SELECT NOME FROM PCEMPR WHERE MATRICULA=PCLANC.CODFORNEC),
                                'L',(SELECT NOME FROM PCEMPR WHERE MATRICULA=PCLANC.CODFORNEC),
                                PCLANC.FORNECEDOR ) )as calcNomeParceiro,
                        
                            PCLANC.CODCONTA, 
                            PCCONTA.CONTA,
                            PCCONTA.GRUPOCONTA,
                            PCGRUPO.GRUPO,
                            PCLANC.HISTORICO,
                            PCLANC.CODFORNEC, 
                        
                            NVL(PCLANC.VALOR,0) AS VALOR,
                        
                        
                            PCLANC.NUMBANCO 
                        
                    FROM  SITEPRAZOFINANCEIRO SITE,  PCLANC , PCCONTA , PCGRUPO, PCFORNEC, PCPEDIDO, PCRATEIOPADRAOCONTA, PCRATEIOCONTAS, PCPRESTACAOCONTA P ,PCADIANTFUNC A  
                    WHERE   PCLANC.CODCONTA = PCCONTA.CODCONTA(+)
                    AND    PCLANC.CODFORNEC = PCFORNEC.CODFORNEC(+)
                    AND    PCLANC.NUMPRESTACAOCONTA = P.NUMPRESTACAOCONTA(+)
                    AND    PCLANC.NUMADIANTAMENTO = A.NUMADIANTAMENTO(+)
                    AND    PCLANC.IDCONTROLEEMBARQUE = PCPEDIDO.IDCONTROLEEMBARQUE(+)
                    AND PCLANC.RECNUM = PCRATEIOCONTAS.RECNUM(+)
                    AND PCRATEIOCONTAS.CODRATEIOCONTA = PCRATEIOPADRAOCONTA.CODRATEIOCONTA(+)
					AND SITE.TITLE = 'CONTAS A PAGAR'
                    AND  (TRUNC(PCLANC.DTVENC) <= SYSDATE - SITE.DAYS)
                    AND --Script para retornar apenas registros com permissão rotina 131  
                    EXISTS( SELECT 1                                                 
                            FROM PCLIB                                             
                            WHERE CODTABELA = TO_CHAR(1)                           
                                AND (CODIGOA  = NVL(PCLANC.CODFILIAL, CODIGOA) OR CODIGOA = '99') 
                                AND CODFUNC   = 1                                    
                                AND PCLIB.CODIGOA IS NOT NULL)                        
                    AND ((PCLANC.DTPAGTO IS NULL))
                    AND PCCONTA.GRUPOCONTA = PCGRUPO.CODGRUPO(+) 
                    AND ((PCCONTA.CODCONTA NOT IN  ( SELECT NVL(CODCONTANTPAG,0) FROM PCCONSUM ) )
                    and (PCCONTA.CODCONTA NOT IN  ( SELECT NVL(CODCONTRECJUR,0) FROM PCCONSUM ) )
                    and (PCCONTA.CODCONTA NOT IN  ( SELECT NVL(CODCONTPAGJUR,0) FROM PCCONSUM ) ) )
                    and NVL(PCLANC.TIPOLANC,'C') Like 'C'
                    ORDER BY PCLANC.DTVENC, calcNomeParceiro, PCLANC.CODFORNEC, PCLANC.CODCONTA,PCLANC.RECNUM 

                    )

                    SELECT DTVENC, calcNomeParceiro,  SUM(VALOR) VALOR
                    FROM CONTAS_A_PAGAR
                    GROUP BY
                    DTVENC, calcNomeParceiro
                    ORDER BY DTVENC ASC
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
            $row['VALOR'] = isset($row['VALOR']) ? (float)$row['VALOR'] : null;
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

    $this->get('/financeiro/alerta/carregamentozero', function (Request $request, Response $response) {
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


        // Consulta SQL com GROUP BY
        $sql = "WITH CARREGAMENTO_ZERO AS (
                SELECT 
                    PCPREST.CODFILIAL, 
                    (NVL(PCPREST.VALOR,0) - NVL(PCPREST.VALORESTORNO,0)) VALOR, 
                    DECODE(PCPREST.CODCLI, 1, NVL(PCVENDACONSUM.CLIENTE, PCCLIENT.CLIENTE), 
                        2, NVL(PCVENDACONSUM.CLIENTE, PCCLIENT.CLIENTE), 
                        PCCLIENT.CLIENTE) CLIENTE, 
                    
                    PCCLIENT.CGCENT AS CNPJ, 

                    (CASE WHEN (PCPREST.CODEMITENTEPEDIDO IS NULL) THEN 
                        (NVL((SELECT PCPEDC.CODEMITENTE 
                            FROM PCPEDC 
                            WHERE PCPEDC.NUMTRANSVENDA = PCNFSAID.NUMTRANSVENDA AND ROWNUM = 1),0)) 
                    ELSE 
                        (PCPREST.CODEMITENTEPEDIDO) 
                    END) AS CODEMITENTE, 
                    
                    PCCOB.COBRANCA
                    
                FROM PCPREST, PCNFSAID, PCCLIENT, PCCARREG, PCCOB, PCVENDACONSUM, PCFILIAL, PCCOB CORIG, SITEPRAZOFINANCEIRO SITE
                WHERE PCPREST.NUMTRANSVENDA = PCNFSAID.NUMTRANSVENDA(+) 
                AND PCPREST.CODFILIAL   = PCFILIAL.CODIGO         
                AND PCPREST.CODCLI      = PCCLIENT.CODCLI         
                AND PCPREST.CODCOB      = PCCOB.CODCOB            
                AND PCNFSAID.NUMPED     = PCVENDACONSUM.NUMPED(+) 
                AND PCPREST.CODCOBORIG  = CORIG.CODCOB(+)         
                AND PCPREST.DTCANCEL IS NULL                      
                AND PCPREST.NUMCAR = PCCARREG.NUMCAR 
                AND PCPREST.NUMCAR = 0
                AND SITE.TITLE = 'CARREGAMENTO ZERO'
                AND NVL(PCPREST.CODFUNCCHECKOUT, 0) = 0 
                AND NVL(PCPREST.NUMCHECKOUT, 0) = 0 
                AND NVL(PCPREST.CODFUNCVEND, 0) = 0 

                AND DECODE(PCPREST.PERMITEESTORNO, 'S', PCPREST.DTEMISSAOORIG, PCPREST.DTEMISSAO) <= SYSDATE - SITE.DAYS
                AND PCPREST.CODCOB NOT IN ('DESD','CANC','ESTR','CRED') 
                AND PCPREST.DTPAG IS NULL AND PCPREST.DTFECHA IS NULL 
                
                )

                SELECT C.CODFILIAL, C.CLIENTE, C.CNPJ, U.NOME, C.COBRANCA, C.VALOR FROM CARREGAMENTO_ZERO C, PCEMPR U
                WHERE C.CODEMITENTE = U.MATRICULA
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
            $row['CODFILIAL'] = isset($row['CODFILIAL']) ? (float)$row['CODFILIAL'] : null;
            $row['VALOR'] = isset($row['VALOR']) ? (float)$row['VALOR'] : null;
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

    $this->get('/financeiro/alerta/valeemaberto', function (Request $request, Response $response) {
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


        // Consulta SQL com GROUP BY
        $sql = "WITH VALEEMABERTO AS (
                    SELECT 
                        
                            DECODE(C.TIPOFUNC,'F',
                                    (SELECT DECODE(SITUACAO,'I',E.NOME ||' (INATIVO)',E.NOME) AS NOME FROM PCEMPR E WHERE E.TIPO = 'F' AND E.MATRICULA = C.CODFUNC),
                                    'M',
                                    (SELECT DECODE(SITUACAO,'I',E.NOME ||' (INATIVO)',E.NOME) AS NOME FROM PCEMPR E WHERE E.CODSETOR IN (SELECT CS.CODSETORMOTORISTA FROM PCCONSUM CS WHERE ROWNUM = 1) AND E.TIPO = 'M' AND E.MATRICULA = C.CODFUNC),
                                    (SELECT U1.NOME FROM PCUSUARI U1 WHERE U1.CODUSUR = C.CODFUNC)) AS NOMEPACEIRO,
                        
                            NVL(VALOR,0) AS VALOR
                        
                    FROM PCCORREN C, SITEPRAZOFINANCEIRO SITE
                    ,(SELECT C2.TIPOFUNC, C2.CODFUNC,
                            COUNT(C2.RECNUM) AS QTDTOTALVALES,
                            SUM(DECODE(C2.TIPOLANC,'D',C2.VALOR,0)) AS VLTOTALDEBITOS,
                            SUM(DECODE(C2.TIPOLANC,'C',C2.VALOR,0)) AS VLTOTALCREDITOS,
                            SUM(DECODE(C2.TIPOLANC,'D',C2.VALOR,0) - DECODE(C2.TIPOLANC,'C',C2.VALOR,0)) AS SALDOFINAL
                            FROM PCCORREN C2
                            GROUP BY C2.TIPOFUNC, C2.CODFUNC) S
                    WHERE C.DTBAIXAVALE IS NULL
                    AND C.TIPOFUNC = S.TIPOFUNC
                    AND C.CODFUNC  = S.CODFUNC
                    AND SITE.TITLE = 'VALES EM ABERTO'
                    AND C.DTLANC <= SYSDATE - SITE.DAYS
                    AND (C.TIPOFUNC IN ('F','M') AND (C.CODFUNC NOT IN (SELECT MATRICULA FROM PCEMPR WHERE (NVL(SITUACAO,'A') <> 'A') ))
                    OR (C.TIPOFUNC NOT IN ('F','M')) AND ( C.CODFUNC NOT IN (SELECT CODUSUR FROM PCUSUARI WHERE DTTERMINO IS NOT NULL)))
                    AND C.CODFUNC > 0
                    AND C.CODFILIAL = '1'
                    AND C.TIPOLANC IN ('C','D')
                    )
                    SELECT NOMEPACEIRO as NOME, SUM(VALOR) VALOR
                    FROM VALEEMABERTO
                    GROUP BY NOMEPACEIRO
                    ORDER BY VALOR DESC
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
            $row['VALOR'] = isset($row['VALOR']) ? (float)$row['VALOR'] : null;
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

    $this->get('/financeiro/alerta/cobranca', function (Request $request, Response $response) {
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

        // Consulta SQL com GROUP BY
        $sql = "WITH COBRANCA AS (
                SELECT 
                    P.VALOR 
                    , TRUNC(P.DTVENC) DTVENC 
                   , C.CLIENTE  AS FANTASIA 
                    , NVL(P.CODSTATUSCOB,C.CODSTATUSCOB) CODSTATUSCOB 
                    , C.CGCENT 
                FROM PCPREST P 
                    , PCCOB B 
                    , PCCLIENT C 
                    , PCUSUARI U 
                    , PCSUPERV S 
                    , PCEMPR E   , SITEPRAZOFINANCEIRO SITE  
                    , (SELECT P1.CODCLI,                                                
                            SUM(                                                      
                                CASE WHEN P1.DTPAG IS NULL THEN 1                      
                                    WHEN P1.DTPAG IS NOT NULL                         
                                            AND P1.CODCOB IN ('PERD', 'CANO') THEN 1 
                                    ELSE 0 END                                        
                            ) QTDTITVENC,                                             
                            SUM(                                                      
                                CASE WHEN P1.DTPAG IS NULL THEN NVL(P1.VALOR, 0)       
                                    WHEN P1.DTPAG IS NOT NULL                         
                                            AND P1.CODCOB IN ('PERD', 'CANO')        
                                            THEN NVL(P1.VALOR, 0)                        
                                    ELSE 0 END                                        
                            ) VLTOTTITVENC,                                           
                                                        
                            MIN(                                                      
                                CASE WHEN P1.DTPAG IS NULL THEN NVL(P1.DTVENC, '')   
                                    WHEN P1.DTPAG IS NOT NULL                         
                                            AND P1.CODCOB IN ('PERD', 'CANO')        
                                            THEN NVL(P1.DTVENC, '')                   
                                    ELSE TRUNC(SYSDATE) END                           
                            ) DTMAIORATRASO                                           
                        FROM PCPREST P1                                                
                            , PCCLIENT C2                                           
                        WHERE P1.CODCLI = C2.CODCLI                                     
                        AND P1.CODCOB NOT IN ('ESTR', 'DESD', 'CANC')           
                AND P1.CODFILIAL IN ('1','2','3','4','5','99')
                        GROUP BY P1.CODCLI) TOTAL                                       
                    , (SELECT NVL(C1.dtproxcontatocob,                              
                            (SELECT MAX(NVL(H.DTPROXCONTATO, ''))               
                                FROM PCHISTCOB H                                   
                                WHERE H.CODCLI = C1.CODCLI                          
                                AND H.DTPROXCONTATO IS NOT NULL)) DTPROXCONTATO   
                            , C1.CODCLI                                             
                        FROM PCCLIENT C1                                           
                    ) DT                                                            
                    WHERE ( B.CODCOB = P.CODCOB)
                    AND (C.CODCLI = P.CODCLI)
                    AND (P.CODCLI = TOTAL.CODCLI(+) )
                AND   ((P.DTPAG IS NULL) OR (P.DTPAG IS NOT NULL AND ((P.CODCOB = 'PERD') OR (P.CODCOB = 'CANO')) AND NVL(P.PERMITEESTORNO, 'S') = 'S'))
                    AND (P.CODUSUR = U.CODUSUR )
                    AND (S.CODSUPERVISOR = U.CODSUPERVISOR )
                    AND (P.CODAGENTECOBRANCA = E.MATRICULA(+)) 
                    AND DT.CODCLI(+) = P.CODCLI
                    AND SITE.TITLE = 'COBRANÇA'
                AND P.DTVENC <= SYSDATE - SITE.DAYS
                AND P.CODCOB IN ('748')
                AND P.CODFILIAL IN ('1','2','3','4','5','99')
                )
                SELECT DTVENC, FANTASIA AS NOME, CGCENT AS CPF, VALOR FROM COBRANCA
                WHERE CODSTATUSCOB IS NULL
                 ORDER BY VALOR DESC
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
            $row['VALOR'] = isset($row['VALOR']) ? (float)$row['VALOR'] : null;
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

    $this->get('/financeiro/alerta/dni', function (Request $request, Response $response) {
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

        // Consulta SQL com GROUP BY
        $sql = "SELECT B.NOME, SUM(c.VALOR) VALOR FROM PCESTCR C, PCBANCO B, SITEPRAZOFINANCEIRO SITE
                WHERE C.CODBANCO = B.CODBANCO
                AND C.CODCOB = 'DNI'
                AND SITE.TITLE = 'DNI'
                AND C.VALOR !=0
                AND C.DTULTCONCILIA <= SYSDATE - SITE.DAYS
                GROUP BY B.NOME
                ORDER BY VALOR ASC
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
            $row['VALOR'] = isset($row['VALOR']) ? (float)$row['VALOR'] : null;
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

    $this->get('/financeiro/alerta/bordero', function (Request $request, Response $response) {
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

        // Consulta SQL com GROUP BY
        $sql = "SELECT 
                PCLANC.HISTORICO, 
                PCLANC.DTVENC
            FROM  PCLANC, PCCONTA,PCBANCO , SITEPRAZOFINANCEIRO SITE
            WHERE PCCONTA.CODCONTA = PCLANC.CODCONTA 
            AND PCLANC.NUMBANCO=PCBANCO.CODBANCO
            AND PCLANC.DTPAGTO IS NULL AND DTESTORNOBAIXA IS NULL
            AND NVL(PCLANC.NUMBORDERO,0)>0 
            AND SITE.TITLE = 'BORDERO'
            AND PCLANC.DTVENC <= SYSDATE 
            AND PCLANC.CODFILIAL='1' 
            AND PCLANC.DTLANC <= SYSDATE - SITE.DAYS
            ORDER BY  PCLANC.DTVENC ASC
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

    $this->get('/financeiro/alerta/adiantamento/aberto', function (Request $request, Response $response) {
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

        // Consulta SQL com GROUP BY
        $sql = "WITH ADIANTAMENTO AS (
                SELECT                                                              
                        C.CONTA,                                                                                                                                                                     
                        L.VALOR,                                                                    
                        L.DTVENC,                                                                   
                        L.VPAGO,                                                                                                                
                        L.FORNECEDOR AS NOMEFUNC                                                                                                 
                FROM PCLANC L , PCFORNEC F, PCCONTA C, PCCOTACAOMOEDAC MC ,SITEPRAZOFINANCEIRO SITE                      
                WHERE L.CODCONTA = C.CODCONTA(+)                                                  
                    AND L.VALOR >= 0                                                                
                    AND L.DTCANCEL IS NULL     
                    AND SITE.TITLE = 'ADIANTAMENTO FORNECEDOR'                                                     
                    AND --Script para retornar apenas registros com permissão rotina 131  
                EXISTS( SELECT 1                                                 
                        FROM PCLIB                                             
                        WHERE CODTABELA = TO_CHAR(1)                           
                            AND (CODIGOA  = NVL(L.CODFILIAL, CODIGOA) OR CODIGOA = '99') 
                            AND CODFUNC   = 1                                    
                            AND PCLIB.CODIGOA IS NOT NULL)                        
                    AND L.MOEDAESTRANGEIRA = MC.CODIGO (+)                                           
                AND L.CODFORNEC = F.CODFORNEC 
                AND L.DTVENC <= SYSDATE - SITE.DAYS
                    AND ((NVL(L.CODROTINABAIXA, 0) <> 737)  
                    AND                        
                ((NVL(L.CODROTINABAIXA, 0) <> 750) OR (L.CODCONTA = (SELECT CODCONTAADIANTFOR FROM PCCONSUM)))) 
                AND (L.CODCONTA = (SELECT CODCONTAADIANTFOR FROM PCCONSUM)) 
                AND F.DTEXCLUSAO IS NULL 
                AND L.DTPAGTO IS NULL       
                AND NVL(L.VPAGO, 0) = 0     
                AND NVL(L.CODFILIAL, '99') = '1' 
                AND L.DTESTORNOBAIXA IS NULL     
                ORDER BY L.CODFORNEC, L.RECNUM 

                )

                SELECT SUM(VALOR) VALOR, DTVENC, NOMEFUNC
                FROM ADIANTAMENTO
                WHERE VPAGO IS NULL
                AND NOMEFUNC != 'REMERSON EMER FIGUEIREDO SILVA'
                GROUP BY
                DTVENC, NOMEFUNC
                ORDER BY DTVENC ASC
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
            $row['VALOR'] = isset($row['VALOR']) ? (float)$row['VALOR'] : null;
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

    $this->get('/financeiro/alerta/caixa/aberto', function (Request $request, Response $response) {
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

        // Consulta SQL com GROUP BY
        $sql = "WITH CAIXAABERTO AS (
                SELECT 
                    
                    PCMOVCR.DATA,
                    PCMOVCR.CODBANCO,
                    PCMOVCR.VALOR
                    
                FROM PCMOVCR, SITEPRAZOFINANCEIRO SITE
                WHERE ((((CONCILIACAO <> 'OK') OR (CONCILIACAO IS NULL))
                AND (CODCOB = 'D')
                AND CODBANCO IN (14,2,3,4,45)
                AND SITE.TITLE = 'CAIXA EM ABERTO'
                AND ((OPERACAO <> 99) OR (OPERACAO IS NULL))
                AND (NUMTRANS BETWEEN 0.000000 AND 9999999999.000000)) AND (TRUNC(DATA) <  SYSDATE - SITE.DAYS)) AND DECODE(TIPO, 'D', VALOR, VALOR * -1) BETWEEN -9999999.000000 AND 9999999.000000 
                )


                SELECT DATA, CODBANCO , SUM(VALOR) VALOR
                FROM CAIXAABERTO
                GROUP BY DATA, CODBANCO
                ORDER BY DATA ASC, CODBANCO
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
            $row['VALOR'] = isset($row['VALOR']) ? (float)$row['VALOR'] : null;
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

    $this->get('/financeiro/alerta/credito/aberto', function (Request $request, Response $response) {
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

        // Consulta SQL com GROUP BY
        $sql = "WITH CREDITOABERTO AS (
                Select
                B.Cliente,
                A.Valor,
                A.historico
                From
                PcClient B,
                PcCreCli A,PCFILIAL F,PCDEVCONSUM D, SITEPRAZOFINANCEIRO SITE
                Where
                A.CodCli=B.CodCli(+)
                AND A.NUMTRANSENTDEVCLI=D.NUMTRANSENT(+)
                AND F.CODIGO=A.CODFILIAL
                AND SITE.TITLE ='CREDITO EM ABERTO'
               
                And  A.dtlanc <= SYSDATE - SITE.DAYS
                AND (A.VALOR > 0 OR A.DTESTORNO IS NOT NULL)

                )

                SELECT CLIENTE, SUM(VALOR) VALOR
                FROM CREDITOABERTO
                GROUP BY CLIENTE
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
            $row['VALOR'] = isset($row['VALOR']) ? (float)$row['VALOR'] : null;
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




















    


    $this->get('/metas/{ano}', function (Request $request, Response $response) {

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

        // Obtém o parâmetro 'ano' da URL
        $ano = $request->getAttribute('ano');

        // Query SQL com o ano como parâmetro
        $sql = "SELECT 
                    PCMETASUP.DATA, 
                    TO_CHAR(PCMETASUP.DATA, 'DD') DIA, 
                    PCMETASUP.VLVENDAPREV,
                    PCMETASUP.CODFILIAL
                FROM 
                    PCMETASUP
                WHERE 
                    TO_CHAR(PCMETASUP.DATA, 'YYYY') = :ano
                ORDER BY 
                    PCMETASUP.CODFILIAL, 
                    PCMETASUP.DATA";

        $stmt = oci_parse($conexao, $sql);

        // Vincula o parâmetro 'ano' à consulta
        oci_bind_by_name($stmt, ":ano", $ano);

        // Executa a consulta
        oci_execute($stmt);

        // Coletar os resultados em um array
        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) != false) {
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

        return $response->withJson($filiais);
    });

    $this->post('/metas/update', function (Request $request, Response $response) {

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
        $filial = $params['filial'] ?? null;
        $valor = $params['valor'] ?? null;
        $data = $params['data'] ?? null;
        $supervisor = $params['supervisor'] ?? null;

        // Valida se os parâmetros necessários foram enviados
        if (!$filial || !$data || !$supervisor) {
            return $response->withJson(['error' => 'Parâmetros inválidos'], 400);
        }

        // Formata a data para garantir que tenha dois dígitos no mês
        $dataParts = explode('-', $data);
        $mes = str_pad($dataParts[0], 2, '0', STR_PAD_LEFT);
        $ano = $dataParts[1];

        // Calcula o número de dias no mês
        $ultimoDiaMes = cal_days_in_month(CAL_GREGORIAN, intval($mes), intval($ano));

        // Insere ou atualiza uma linha para cada dia do mês
        for ($dia = 1; $dia <= $ultimoDiaMes; $dia++) {
            $diaFormatado = str_pad($dia, 2, '0', STR_PAD_LEFT); // Formata o dia com dois dígitos
            $dataCompleta = $diaFormatado . '-' . $mes . '-' . $ano; // Formata a data completa

            // Verifica se o registro já existe
            $sqlSelect = "SELECT COUNT(*) AS TOTAL FROM PCMETASUP
                          WHERE CODFILIAL = :filial
                          AND TO_CHAR(DATA, 'DD-MM-YYYY') = :dataCompleta";
            $stmtSelect = oci_parse($conexao, $sqlSelect);
            oci_bind_by_name($stmtSelect, ":filial", $filial);
            oci_bind_by_name($stmtSelect, ":dataCompleta", $dataCompleta);
            oci_execute($stmtSelect);
            $row = oci_fetch_assoc($stmtSelect);
            $total = $row['TOTAL'];
            oci_free_statement($stmtSelect);

            if ($total > 0) {
                // Se já existir, faz o UPDATE
                $sqlUpdate = "UPDATE PCMETASUP SET VLVENDAPREV = :valor, CODSUPERVISOR = :supervisor
                              WHERE CODFILIAL = :filial
                              AND TO_CHAR(DATA, 'DD-MM-YYYY') = :dataCompleta";
                $stmtUpdate = oci_parse($conexao, $sqlUpdate);
                oci_bind_by_name($stmtUpdate, ":filial", $filial);
                oci_bind_by_name($stmtUpdate, ":dataCompleta", $dataCompleta);
                oci_bind_by_name($stmtUpdate, ":valor", $valor);
                oci_bind_by_name($stmtUpdate, ":supervisor", $supervisor);

                if (!oci_execute($stmtUpdate)) {
                    $e = oci_error($stmtUpdate);
                    return $response->withJson(['error' => $e['message']], 500);
                }
                oci_free_statement($stmtUpdate);
            } else {
                // Se não existir, faz o INSERT
                $sqlInsert = "INSERT INTO PCMETASUP (CODFILIAL, DATA, VLVENDAPREV, CODSUPERVISOR)
                              VALUES (:filial, TO_DATE(:dataCompleta, 'DD-MM-YYYY'), :valor, :supervisor)";
                $stmtInsert = oci_parse($conexao, $sqlInsert);
                oci_bind_by_name($stmtInsert, ":filial", $filial);
                oci_bind_by_name($stmtInsert, ":dataCompleta", $dataCompleta);
                oci_bind_by_name($stmtInsert, ":valor", $valor);
                oci_bind_by_name($stmtInsert, ":supervisor", $supervisor);

                if (!oci_execute($stmtInsert)) {
                    $e = oci_error($stmtInsert);
                    return $response->withJson(['error' => $e['message']], 500);
                }
                oci_free_statement($stmtInsert);
            }
        }

        return $response->withJson(['message' => 'Inserções/Atualizações bem-sucedidas para todos os dias do mês'], 200);

        oci_close($conexao);
    });

    $this->get('/grupos/{ano}', function (Request $request, Response $response) {
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

        // Obtém o parâmetro de ano
        $ano = $request->getAttribute('ano');

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
                    GROUP BY 
                        TO_CHAR(PCLANC.DTVENC, 'MM'),  
                        PCCONTA.GRUPOCONTA, 
                        PCGRUPO.GRUPO
                ),
                VLREALIZADO AS (
                    SELECT 
                        MES, 
                        SUM(VLREALIZADO) AS VLREALIZADO,
                        GRUPOCONTA
                    FROM (
                        SELECT 
                            NVL(SUM(
                                (DECODE(nvl(PCLANC.VPAGO, 0), 0,
                                        DECODE(PCLANC.DESCONTOFIN, PCLANC.VALOR, PCLANC.VALOR, 
                                            DECODE(PCLANC.VALORDEV, PCLANC.VALOR, PCLANC.VALOR, 0)),
                                        nvl(PCLANC.VPAGO, 0) + NVL(PCLANC.VALORDEV, 0)) * (1))), 0) * (-1) AS VLREALIZADO, 
                            PCLANC.CODCONTA,
                            TO_CHAR(PCLANC.DTPAGTO, 'MM') AS MES,
                            PCCONTA.GRUPOCONTA
                        FROM 
                            PCLANC
                            JOIN PCCONTA ON PCLANC.CODCONTA = PCCONTA.CODCONTA
                            LEFT JOIN PCNFSAID ON PCLANC.NUMTRANSVENDA = PCNFSAID.NUMTRANSVENDA
                        WHERE 
                            NVL(PCNFSAID.CONDVENDA, 0) NOT IN (10, 20, 98, 99)
                            AND NVL(PCNFSAID.CODFISCAL, 0) NOT IN (522, 622, 722, 532, 632, 732)
                            AND NVL(PCLANC.CODROTINABAIXA, 0) <> 737
                            AND EXTRACT(YEAR FROM PCLANC.DTPAGTO) = :ano
                            AND NVL(PCLANC.VPAGO, 0) <> 0
                        GROUP BY 
                            PCLANC.CODCONTA, 
                            TO_CHAR(PCLANC.DTPAGTO, 'MM'), 
                            PCCONTA.GRUPOCONTA
                    )
                    GROUP BY MES, GRUPOCONTA
                )
               SELECT 
                    M.MES,
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
                WHERE 
                    NVL(R.VLREALIZADO, 0) <> 0 OR NVL(P.VALOR_PREVISTO, 0) <> 0
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
            // Log de erro na preparação da consulta SQL
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        // Associar a variável de ano ao placeholder SQL
        oci_bind_by_name($stmt, ":ano", $ano);

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
            // Log de retorno vazio
            $this->logger->info("Nenhum resultado encontrado.");
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
        $this->logger->info("Consulta executada com sucesso.");

        // Retornar resultados em JSON
        return $response->withJson($filiais);
    });

    $this->get('/financeiro/{ano}', function (Request $request, Response $response) {
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

        // Obtém o parâmetro de ano
        $ano = $request->getAttribute('ano');

        // Consulta SQL com GROUP BY
        $sql = "SELECT
                TO_CHAR(M.DTMOV, 'MM') AS MES,
                ROUND(SUM(M.QT * M.CUSTOFIN), 2) AS CUSTO,
                ROUND(SUM(M.QT * M.PUNIT), 2) AS VENDA,

                COUNT(DISTINCT M.NUMTRANSVENDA) AS NUMVENDAS,
                
                ROUND(SUM(M.QT * M.PUNIT) - SUM(M.QT * M.CUSTOFIN), 2) AS LUCRO,
                
                CASE 
                    WHEN SUM(M.QT * M.PUNIT) = 0 THEN 0 -- Evita divisão por zero
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
                AND M.CODOPER IN ('S', 'SB')
            GROUP BY

                TO_CHAR(M.DTMOV, 'MM')  ";



        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            // Log de erro na preparação da consulta SQL
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        // Associar a variável de ano ao placeholder SQL
        oci_bind_by_name($stmt, ":ano", $ano);

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
            // Log de retorno vazio
            $this->logger->info("Nenhum resultado encontrado.");
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
        $this->logger->info("Consulta executada com sucesso.");

        // Retornar resultados em JSON
        return $response->withJson($filiais);
    });

    $this->get('/metas/grupo/{ano}', function (Request $request, Response $response, $args) {

        $settings = $this->get('settings')['db'];
        $dsn = $settings['dsn'];
        $username = $settings['username'];
        $password = $settings['password'];

        // Parâmetro dinâmico do mês vindo da URL
        $ano = $args['ano'];

        // Conectando ao Oracle
        $conexao = oci_connect($username, $password, $dsn);

        if (!$conexao) {
            $e = oci_error();
            throw new Exception($e['message']);
        }

        // Query SQL ajustada para usar o parâmetro dinâmico do mês
        $sql = "SELECT 
            TO_CHAR(PCMETASUP.DATA, 'MM') AS MES, 
            SUM(PCMETASUP.VLVENDAPREV) AS VALOR_META
            FROM 
                PCMETASUP
            WHERE 
                PCMETASUP.DATA >= TO_DATE('01-JAN-2024', 'DD-MON-YYYY')
                AND TO_CHAR(PCMETASUP.DATA, 'YYYY') = :ano
            GROUP BY 
                TO_CHAR(PCMETASUP.DATA, 'MM')
            ORDER BY  
                TO_CHAR(PCMETASUP.DATA, 'MM')";

        // Preparando a consulta com o parâmetro dinâmico
        $stmt = oci_parse($conexao, $sql);

        // Bind do parâmetro do mês
        oci_bind_by_name($stmt, ':ano', $ano);

        // Executa a consulta
        oci_execute($stmt);

        // Coletar os resultados em um array
        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) != false) {
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

        return $response->withJson($filiais);
    });
    $this->post('/porcentagem/insert', function (Request $request, Response $response) {
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
        $grupo = $params['grupo'] ?? null;
        $porcentagem = $params['porcentagem'] ?? null;

        // Valida se os parâmetros necessários foram enviados
        if (!$grupo || !$porcentagem) {
            return $response->withJson(['error' => 'Parâmetros inválidos. Grupo e porcentagem são obrigatórios.'], 400);
        }

        // Verifica se o grupo já existe na tabela SITEPORCENTAGEMGRUPO
        $sqlSelect = "SELECT COUNT(*) AS TOTAL FROM SITEPORCENTAGEMGRUPO WHERE GRUPO = :grupo";
        $stmtSelect = oci_parse($conexao, $sqlSelect);
        oci_bind_by_name($stmtSelect, ":grupo", $grupo);
        oci_execute($stmtSelect);
        $row = oci_fetch_assoc($stmtSelect);
        $total = $row['TOTAL'];
        oci_free_statement($stmtSelect);

        if ($total > 0) {
            // Se o grupo já existir, faz o UPDATE
            $sqlUpdate = "UPDATE SITEPORCENTAGEMGRUPO SET PORCENTAGEM = :porcentagem WHERE GRUPO = :grupo";
            $stmtUpdate = oci_parse($conexao, $sqlUpdate);
            oci_bind_by_name($stmtUpdate, ":grupo", $grupo);
            oci_bind_by_name($stmtUpdate, ":porcentagem", $porcentagem);

            if (!oci_execute($stmtUpdate)) {
                $e = oci_error($stmtUpdate);
                oci_free_statement($stmtUpdate);
                oci_close($conexao);
                return $response->withJson(['error' => 'Erro ao atualizar o grupo: ' . $e['message']], 500);
            }

            oci_free_statement($stmtUpdate);
            $message = 'Grupo atualizado com sucesso.';
        } else {
            // Se o grupo não existir, faz o INSERT
            $sqlInsert = "INSERT INTO SITEPORCENTAGEMGRUPO (GRUPO, PORCENTAGEM) VALUES (:grupo, :porcentagem)";
            $stmtInsert = oci_parse($conexao, $sqlInsert);
            oci_bind_by_name($stmtInsert, ":grupo", $grupo);
            oci_bind_by_name($stmtInsert, ":porcentagem", $porcentagem);

            if (!oci_execute($stmtInsert)) {
                $e = oci_error($stmtInsert);
                oci_free_statement($stmtInsert);
                oci_close($conexao);
                return $response->withJson(['error' => 'Erro ao inserir grupo: ' . $e['message']], 500);
            }

            oci_free_statement($stmtInsert);
            $message = 'Grupo inserido com sucesso.';
        }

        // Fechar a conexão
        oci_close($conexao);

        return $response->withJson(['message' => $message], 200);
    });
    $this->get('/porcentagem/pesquisa', function (Request $request, Response $response) {
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
        $sql = "SELECT GRUPO, PORCENTAGEM FROM SITEPORCENTAGEMGRUPO";

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
        $grupos = [];
        while (($row = oci_fetch_assoc($stmt)) !== false) {
            // Garantir que a porcentagem seja um número
            $row['PORCENTAGEM'] = (float) $row['PORCENTAGEM'];
            $grupos[] = $row;
        }

        // Fechar a conexão
        oci_free_statement($stmt);
        oci_close($conexao);

        // Verifica se há resultados
        if (empty($grupos)) {
            // Log de retorno vazio
            $this->logger->info("Nenhum resultado encontrado para a tabela SITEPORCENTAGEMGRUPO.");
            return $response->withJson(['message' => 'Nenhum dado encontrado'], 404);
        }

        // Convertendo para UTF-8 os resultados, se necessário
        foreach ($grupos as &$grupo) {
            array_walk_recursive($grupo, function (&$item) {
                if (!mb_detect_encoding($item, 'utf-8', true)) {
                    $item = utf8_encode($item);
                }
            });
        }

        // Log de sucesso
        $this->logger->info("Consulta executada com sucesso.");

        // Retornar resultados em JSON
        return $response->withJson($grupos);
    });
    $this->post('/porcentagem/mes/insert', function (Request $request, Response $response) {
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
        $mes = $params['mes'] ?? null;
        $porcentagem = $params['porcentagem'] ?? null;

        // Valida se os parâmetros necessários foram enviados
        if (!$mes || !$porcentagem) {
            return $response->withJson(['error' => 'Parâmetros inválidos. mes e porcentagem são obrigatórios.'], 400);
        }

        // Verifica se o MES já existe na tabela SITEPORCENTAGEMGRUPO
        $sqlSelect = "SELECT COUNT(*) AS TOTAL FROM SITEPORCENTAGEMMES WHERE MES = :mes";
        $stmtSelect = oci_parse($conexao, $sqlSelect);
        oci_bind_by_name($stmtSelect, ":mes", $mes);
        oci_execute($stmtSelect);
        $row = oci_fetch_assoc($stmtSelect);
        $total = $row['TOTAL'];
        oci_free_statement($stmtSelect);

        if ($total > 0) {
            // Se o MES já existir, faz o UPDATE
            $sqlUpdate = "UPDATE SITEPORCENTAGEMMES SET PORCENTAGEM = :porcentagem WHERE MES = :mes";
            $stmtUpdate = oci_parse($conexao, $sqlUpdate);
            oci_bind_by_name($stmtUpdate, ":mes", $mes);
            oci_bind_by_name($stmtUpdate, ":porcentagem", $porcentagem);

            if (!oci_execute($stmtUpdate)) {
                $e = oci_error($stmtUpdate);
                oci_free_statement($stmtUpdate);
                oci_close($conexao);
                return $response->withJson(['error' => 'Erro ao atualizar o mes: ' . $e['message']], 500);
            }

            oci_free_statement($stmtUpdate);
            $message = 'mes atualizado com sucesso.';
        } else {
            // Se o MES não existir, faz o INSERT
            $sqlInsert = "INSERT INTO SITEPORCENTAGEMMES (MES, PORCENTAGEM) VALUES (:mes, :porcentagem)";
            $stmtInsert = oci_parse($conexao, $sqlInsert);
            oci_bind_by_name($stmtInsert, ":mes", $mes);
            oci_bind_by_name($stmtInsert, ":porcentagem", $porcentagem);

            if (!oci_execute($stmtInsert)) {
                $e = oci_error($stmtInsert);
                oci_free_statement($stmtInsert);
                oci_close($conexao);
                return $response->withJson(['error' => 'Erro ao inserir mes: ' . $e['message']], 500);
            }

            oci_free_statement($stmtInsert);
            $message = 'mes inserido com sucesso.';
        }

        // Fechar a conexão
        oci_close($conexao);

        return $response->withJson(['message' => $message], 200);
    });
    $this->get('/porcentagem/mes/pesquisa', function (Request $request, Response $response) {
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
        $sql = "SELECT MES, PORCENTAGEM FROM SITEPORCENTAGEMMES";

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
        $grupos = [];
        while (($row = oci_fetch_assoc($stmt)) !== false) {
            // Garantir que a porcentagem seja um número
            $row['PORCENTAGEM'] = (float) $row['PORCENTAGEM'];
            $grupos[] = $row;
        }

        // Fechar a conexão
        oci_free_statement($stmt);
        oci_close($conexao);

        // Verifica se há resultados
        if (empty($grupos)) {
            // Log de retorno vazio
            $this->logger->info("Nenhum resultado encontrado para a tabela SITEPORCENTAGEMGRUPO.");
            return $response->withJson(['message' => 'Nenhum dado encontrado'], 404);
        }

        // Convertendo para UTF-8 os resultados, se necessário
        foreach ($grupos as &$grupo) {
            array_walk_recursive($grupo, function (&$item) {
                if (!mb_detect_encoding($item, 'utf-8', true)) {
                    $item = utf8_encode($item);
                }
            });
        }

        // Log de sucesso
        $this->logger->info("Consulta executada com sucesso.");

        // Retornar resultados em JSON
        return $response->withJson($grupos);
    });
});
