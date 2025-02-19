<?php

use Slim\Http\Request;
use Slim\Http\Response;

use Symfony\Component\Console\Descriptor\Descriptor;

// Rota para listar os dados de PCFILIAL
$app->group('/api/v1', function () {

    $this->get('/agenda/fornecedor', function (Request $request, Response $response) {
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
            $consulta = "SELECT CODFORNEC, FORNECEDOR FROM PCFORNEC";
    
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
    $this->post('/agenda/inserir/usuario', function (Request $request, Response $response) {
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
        $CODFORNECEDOR = $params['CODFORNECEDOR'] ?? null;
        $DATA = $params['DATA'] ?? null;
        $FORNECEDOR = $params['FORNECEDOR'] ?? null;
        $TIME = $params['TIME'] ?? null;
        $USUARIO = $params['USUARIO'] ?? null;
        $CODUSUARIO = $params['CODUSUARIO'] ?? null; // Removido espaço extra
    
        // Valida se os parâmetros necessários foram enviados
        if (!$CODFORNECEDOR || !$DATA || !$FORNECEDOR || !$TIME || !$USUARIO || !$CODUSUARIO) {
            return $response->withJson(['error' => 'Parâmetros inválidos. Todos os campos são obrigatórios.'], 400);
        }
    
        // Formata a DATA para o padrão Oracle
        $DATA = date('Y-m-d', strtotime($DATA));
    
        // Comando SQL para inserção
        $atualizarBanco = "
            INSERT INTO SITEAGENDAFORNECEDOR (
                CODFORNECEDOR, DATA, FORNECEDOR, TIME, USUARIO, CODUSUARIO
            ) VALUES (
                :CODFORNECEDOR, TO_DATE(:DATA, 'YYYY-MM-DD'), :FORNECEDOR, :TIME, :USUARIO, :CODUSUARIO
            )
        ";
    
        // Preparando e executando o comando de inserção
        $bancoSaldo = oci_parse($conexao, $atualizarBanco);
        oci_bind_by_name($bancoSaldo, ":CODFORNECEDOR", $CODFORNECEDOR);
        oci_bind_by_name($bancoSaldo, ":DATA", $DATA);
        oci_bind_by_name($bancoSaldo, ":FORNECEDOR", $FORNECEDOR);
        oci_bind_by_name($bancoSaldo, ":TIME", $TIME);
        oci_bind_by_name($bancoSaldo, ":USUARIO", $USUARIO);
        oci_bind_by_name($bancoSaldo, ":CODUSUARIO", $CODUSUARIO); // Removido espaço extra
    
        $resultbanco = oci_execute($bancoSaldo);
    
        // Verifica se a inserção foi bem-sucedida
        if ($resultbanco) {
            oci_free_statement($bancoSaldo);
            oci_close($conexao);
            return $response->withJson(['message' => 'Inserção bem-sucedida'], 200);
        } else {
            $e = oci_error($bancoSaldo);
            oci_free_statement($bancoSaldo);
            oci_close($conexao);
            return $response->withJson(['error' => $e['message']], 500);
        }
    });
    $this->post('/agenda/inserir/pedido', function (Request $request, Response $response) {
        $settings = $this->get('settings')['db'];
        $dsn = $settings['dsn'];
        $username = $settings['username'];
        $password = $settings['password'];

        $conexao = oci_connect($username, $password, $dsn);

        if (!$conexao) {
            $e = oci_error();
            error_log("Erro ao conectar ao Oracle: " . $e['message']);
            throw new Exception($e['message']);
        }

        $params = $request->getParsedBody();
        $NUMPED = $params['NUMPED'] ?? null;
        $CODPROD = $params['CODPROD'] ?? null;
        $QT = $params['QT'] ?? null;
        $CUSTO = $params['CUSTO'] ?? null;
        $CODUSUARIO = $params['CODUSUARIO'] ?? null;
        $CODFORNECEDOR = $params['CODFORNECEDOR'] ?? null;
        $CODFILIAL = $params['CODFILIAL'] ?? null;

        if (!$NUMPED || !$CODPROD || !$QT || !$CUSTO || !$CODUSUARIO || !$CODFORNECEDOR || !$CODFILIAL) {
            error_log("Parâmetros inválidos: " . print_r($params, true));
            return $response->withJson(['error' => 'Parâmetros inválidos. Todos os campos são obrigatórios.'], 400);
        }

        $atualizarBanco = "
             INSERT INTO SITEPREPEDIDOCOMPRA (
                NUMPED, CODPROD, QT, CUSTO, CODUSUARIO, CODFORNECEDOR, CODFILIAL
            ) VALUES (
                :NUMPED, :CODPROD, :QT, :CUSTO, :CODUSUARIO, :CODFORNECEDOR, :CODFILIAL
            )
        ";

        $bancoSaldo = oci_parse($conexao, $atualizarBanco);
        oci_bind_by_name($bancoSaldo, ":NUMPED", $NUMPED);
        oci_bind_by_name($bancoSaldo, ":CODPROD", $CODPROD);
        oci_bind_by_name($bancoSaldo, ":QT", $QT);
        oci_bind_by_name($bancoSaldo, ":CUSTO", $CUSTO);
        oci_bind_by_name($bancoSaldo, ":CODUSUARIO", $CODUSUARIO);
        oci_bind_by_name($bancoSaldo, ":CODFORNECEDOR", $CODFORNECEDOR);
        oci_bind_by_name($bancoSaldo, ":CODFILIAL", $CODFILIAL);

        $resultbanco = oci_execute($bancoSaldo);

        if ($resultbanco) {
            oci_free_statement($bancoSaldo);
            oci_close($conexao);
            return $response->withJson(['message' => 'Inserção bem-sucedida'], 200);
        } else {
            $e = oci_error($bancoSaldo);
            error_log("Erro ao executar o comando SQL: " . $e['message']);
            oci_free_statement($bancoSaldo);
            oci_close($conexao);
            return $response->withJson(['error' => $e['message']], 500);
        }
    });
    $this->get('/agenda/usuario/{CODUSUARIO}', function (Request $request, Response $response) {
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
                return $response->withJson(['error' => 'Falha na conexão com o banco de dados.', 'details' => $e['message']], 500);
            }
    
            // Obtendo o parâmetro CODUSUARIO da rota
            $CODUSUARIO = $request->getAttribute('CODUSUARIO');
    
            // Valida se CODUSUARIO foi enviado
            if (!$CODUSUARIO) {
                return $response->withJson(['error' => 'O parâmetro CODUSUARIO é obrigatório.'], 400);
            }
    
            // Comando SQL para consultar os registros
            $consulta = "SELECT * FROM SITEAGENDAFORNECEDOR WHERE CODUSUARIO = :CODUSUARIO ORDER BY DATA DESC, TIME DESC";
    
            // Preparando e executando a consulta
            $statement = oci_parse($conexao, $consulta);
            oci_bind_by_name($statement, ':CODUSUARIO', $CODUSUARIO);
    
            if (!oci_execute($statement)) {
                $e = oci_error($statement);
                return $response->withJson(['error' => 'Erro ao executar consulta.', 'details' => $e['message']], 500);
            }
    
            // Manipulando os resultados e forçando a codificação UTF-8
            $dados = [];
            while ($row = oci_fetch_assoc($statement)) {
                $dados[] = array_map(function ($value) {
                    return is_string($value) ? utf8_encode($value) : $value;
                }, $row);
            }
    
            // Liberando os recursos
            oci_free_statement($statement);
            oci_close($conexao);
    
            // Retornando os dados em JSON
            return $response->withJson($dados, 200);
    
        } catch (Exception $e) {
            // Captura de exceções
            return $response->withJson(['error' => 'Erro no servidor.', 'details' => $e->getMessage()], 500);
        }
    });
    $this->delete('/agenda/usuario/{ID}', function (Request $request, Response $response) {
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
                return $response->withJson(['error' => 'Falha na conexão com o banco de dados.', 'details' => $e['message']], 500);
            }
    
            // Obtendo o parâmetro ID da rota
            $ID = $request->getAttribute('ID');
    
            // Valida se ID foi enviado
            if (!$ID) {
                return $response->withJson(['error' => 'O parâmetro ID é obrigatório.'], 400);
            }
    
            // Comando SQL para deletar o registro
            $deleteQuery = "DELETE FROM SITEAGENDAFORNECEDOR WHERE ID = :ID";
    
            // Preparando e executando a consulta
            $statement = oci_parse($conexao, $deleteQuery);
            oci_bind_by_name($statement, ':ID', $ID);
    
            if (!oci_execute($statement)) {
                $e = oci_error($statement);
                return $response->withJson(['error' => 'Erro ao executar exclusão.', 'details' => $e['message']], 500);
            }
    
            // Verifica se algum registro foi afetado
            $rowsAffected = oci_num_rows($statement);
    
            // Liberando os recursos
            oci_free_statement($statement);
            oci_close($conexao);
    
            if ($rowsAffected > 0) {
                return $response->withJson(['message' => 'Registro deletado com sucesso.'], 200);
            } else {
                return $response->withJson(['error' => 'Nenhum registro encontrado para o ID informado.'], 404);
            }
        } catch (Exception $e) {
            // Captura de exceções
            return $response->withJson(['error' => 'Erro no servidor.', 'details' => $e->getMessage()], 500);
        }
    });
    $this->delete('/agenda/delete/pedido/{NUMPED}', function (Request $request, Response $response) {
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
                return $response->withJson(['error' => 'Falha na conexão com o banco de dados.', 'details' => $e['message']], 500);
            }
    
            // Obtendo o parâmetro ID da rota
            $NUMPED = $request->getAttribute('NUMPED');
    
            // Valida se ID foi enviado
            if (!$NUMPED) {
                return $response->withJson(['error' => 'O parâmetro ID é obrigatório.'], 400);
            }
    
            // Comando SQL para deletar o registro
            $deleteQuery = "DELETE FROM SITEPREPEDIDOCOMPRA WHERE NUMPED = :NUMPED";
    
            // Preparando e executando a consulta
            $statement = oci_parse($conexao, $deleteQuery);
            oci_bind_by_name($statement, ':NUMPED', $NUMPED);
    
            if (!oci_execute($statement)) {
                $e = oci_error($statement);
                return $response->withJson(['error' => 'Erro ao executar exclusão.', 'details' => $e['message']], 500);
            }
    
            // Verifica se algum registro foi afetado
            $rowsAffected = oci_num_rows($statement);
    
            // Liberando os recursos
            oci_free_statement($statement);
            oci_close($conexao);
    
            if ($rowsAffected > 0) {
                return $response->withJson(['message' => 'Registro deletado com sucesso.'], 200);
            } else {
                return $response->withJson(['error' => 'Nenhum registro encontrado para o ID informado.'], 404);
            }
        } catch (Exception $e) {
            // Captura de exceções
            return $response->withJson(['error' => 'Erro no servidor.', 'details' => $e->getMessage()], 500);
        }
    });
    $this->get('/agenda/fornecedor/produto/{fornecedor}/{codfilial}', function (Request $request, Response $response) {
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

        $fornecedor = $request->getAttribute('fornecedor');
        $codfilial = $request->getAttribute('codfilial');

        // Consulta SQL com GROUP BY e formato de data correto
        $sql = "WITH ENTRADAS AS (
                SELECT M.CODFILIAL, M.DTMOV AS ULTIMA_ENTRADA, M.CODPROD, P.DESCRICAO AS PRODUTO, F.CODFORNEC, F.FORNECEDOR, D.CODEPTO, D.DESCRICAO AS DEPARTAMENTO,
                ROW_NUMBER() OVER (PARTITION BY P.CODPROD,  M.CODFILIAL ORDER BY M.DTMOV DESC ) AS RN,
                EF.ESTOQUEMIN AS ESTMIN, EF.ESTOQUEMAX AS ESTMAX, 
                ( E.QTESTGER - E.QTBLOQUEADA - E.QTRESERV ) AS ESTOQUE, 
                E.QTGIRODIA,
                 CASE 
                        WHEN (E.QTGIRODIA * (F.PRAZOENTREGA + P.TEMREPOS)) > E.ESTMIN THEN (E.QTGIRODIA * (F.PRAZOENTREGA + P.TEMREPOS))
                        ELSE E.ESTMIN
                    END AS ESTOQUE_IDEAL,
                    
                 GREATEST(
		            E.QTGIRODIA * (F.PRAZOENTREGA + P.TEMREPOS),
		            E.ESTMIN
		        ) - (
		            (E.QTESTGER - E.QTRESERV - E.QTBLOQUEADA) + COALESCE(E.QTPEDIDA, 0)
		        ) AS SUGESTAO_COMPRA,
                F.PRAZOENTREGA,
                P.CLASSEVENDA,
                S.DESCRICAO AS SECAO,
                M.CUSTOULTENT,
                M.VALORULTENT,
                MC.MARCA,
                P.TEMREPOS,
               	E.QTPERDADIA AS PEDIDO_PENDENTE
               	
                FROM PCMOV M, PCPRODUT P, PCFORNEC F, PCDEPTO D, PCPRODFILIAL PF, PCEST E, PCPRODFILIAL EF, PCSECAO S, PCMARCA MC
                WHERE 1=1
                AND M.CODPROD = P.CODPROD
                AND M.CODFORNEC = F.CODFORNEC
                AND M.CODPROD = PF.CODPROD
                AND M.CODFILIAL = PF.CODFILIAL
                AND M.CODPROD = E.CODPROD
                AND M.CODFILIAL = E.CODFILIAL
                AND M.CODPROD = EF.CODPROD
                AND M.CODFILIAL = EF.CODFILIAL
                AND P.CODMARCA = MC.CODMARCA
                AND P.CODEPTO = D.CODEPTO
                AND P.CODSEC = S.CODSEC
                AND M.CODOPER IN ('E','EB')
                AND P.REVENDA = 'S'
                AND M.DTMOV > ADD_MONTHS(SYSDATE, -24)
                AND EF.REVENDA = 'S'
                AND EF.FORALINHA = 'N'
                AND P.OBS2 != 'FL'
                AND F.CODFORNEC = :fornecedor
                AND M.CODFILIAL = :codfilial
            ),
            F1 AS (
			    SELECT
			        E.CODPROD,
			        GREATEST(
			            E.QTGIRODIA * (F.PRAZOENTREGA + P.TEMREPOS),
			            E.ESTMIN
			        ) AS B1
			    FROM
			        PCEST E
			        JOIN PCPRODUT P ON E.CODPROD = P.CODPROD
			        JOIN PCFORNEC F ON F.CODFORNEC = P.CODFORNEC
			    WHERE
			        E.CODFILIAL IN (1)
			),
			F2 AS (
			    SELECT
			        E.CODPROD,
			        GREATEST(
			            E.QTGIRODIA * (F.PRAZOENTREGA + P.TEMREPOS),
			            E.ESTMIN
			        ) AS B2
			    FROM
			        PCEST E
			        JOIN PCPRODUT P ON E.CODPROD = P.CODPROD
			        JOIN PCFORNEC F ON F.CODFORNEC = P.CODFORNEC
			    WHERE
			        E.CODFILIAL IN (2)
			),
			F3 AS (
			    SELECT
			        E.CODPROD,
			        GREATEST(
			            E.QTGIRODIA * (F.PRAZOENTREGA + P.TEMREPOS),
			            E.ESTMIN
			        ) AS B3
			    FROM
			        PCEST E
			        JOIN PCPRODUT P ON E.CODPROD = P.CODPROD
			        JOIN PCFORNEC F ON F.CODFORNEC = P.CODFORNEC
			    WHERE
			        E.CODFILIAL IN (3)
			),
			F4 AS (
			    SELECT
			        E.CODPROD,
			        GREATEST(
			            E.QTGIRODIA * (F.PRAZOENTREGA + P.TEMREPOS),
			            E.ESTMIN
			        ) AS B4
			    FROM
			        PCEST E
			        JOIN PCPRODUT P ON E.CODPROD = P.CODPROD
			        JOIN PCFORNEC F ON F.CODFORNEC = P.CODFORNEC
			    WHERE
			        E.CODFILIAL IN (4)
			),
			F5 AS (
			    SELECT
			        E.CODPROD,
			        GREATEST(
			            E.QTGIRODIA * (F.PRAZOENTREGA + P.TEMREPOS),
			            E.ESTMIN
			        ) AS B5
			    FROM
			        PCEST E
			        JOIN PCPRODUT P ON E.CODPROD = P.CODPROD
			        JOIN PCFORNEC F ON F.CODFORNEC = P.CODFORNEC
			    WHERE
			        E.CODFILIAL IN (5)
			),
			
			GIRO_MES AS (
				SELECT M.CODPROD,
				ROUND(SUM(M.QT) / 3, 2) AS GIRO_3_MESES
				FROM PCMOV M
				WHERE M.CODOPER = 'S'
				AND TRUNC(M.DTMOV) > TRUNC(ADD_MONTHS(SYSDATE, -3))
				GROUP BY M.CODPROD
			),
			
			VENDAS_3_MESES AS (
				SELECT 
				    M.CODPROD,
				    ROUND(SUM(CASE 
				        WHEN TO_CHAR(M.DTMOV, 'YYYY-MM') = TO_CHAR(ADD_MONTHS(SYSDATE, -1), 'YYYY-MM') THEN M.QT
				        ELSE 0
				    END), 2) AS MES_ANT_1,
				    ROUND(SUM(CASE 
				        WHEN TO_CHAR(M.DTMOV, 'YYYY-MM') = TO_CHAR(ADD_MONTHS(SYSDATE, -2), 'YYYY-MM') THEN M.QT
				        ELSE 0
				    END), 2) AS MES_ANT_2,
				        ROUND(SUM(CASE 
				        WHEN TO_CHAR(M.DTMOV, 'YYYY-MM') = TO_CHAR(ADD_MONTHS(SYSDATE, -3), 'YYYY-MM') THEN M.QT
				        ELSE 0
				    END), 2) AS MES_ANT_3,
				    ROUND(SUM(CASE 
				        WHEN TO_CHAR(M.DTMOV, 'YYYY-MM') = TO_CHAR(SYSDATE, 'YYYY-MM') THEN M.QT
				        ELSE 0
				    END), 2) AS MES_ATUAL
				FROM 
				    PCMOV M
				WHERE 
				    M.CODOPER = 'S'
				    AND TRUNC(M.DTMOV) > TRUNC(ADD_MONTHS(SYSDATE, -3))
				GROUP BY 
				    M.CODPROD
				),
				
				
				FINAL AS (
				
				  SELECT
            A.*,
             (F1.B1 + F2.B2 + F3.B3 + F4.B4 + F5.B5) AS ESTOQUE_IDEAL_MULTI_FILIAL,
             
             
             
             (
	            SELECT
	                SUM(QTESTGER) AS ESTOQUE_TOTAL
	            FROM
	                PCEST
	            WHERE
	                CODFILIAL IN (1, 2, 3, 4, 5)
	                AND PCEST.CODPROD = A.CODPROD
	        ) AS ESTOQUE_MULTI_FILIAL,
	        
	        (
	            SELECT
	                SUM(ESTOQUEMIN) AS ESTOQUE_TOTAL
	            FROM
	                PCPRODFILIAL
	            WHERE
	                CODFILIAL IN (1, 2, 3, 4, 5)
	                AND PCPRODFILIAL.CODPROD = A.CODPROD
	        ) AS ESTOQUE_MIN_MULTI_FILIAL,
	        
	        (
	            SELECT
	                SUM(ESTOQUEMAX) AS ESTOQUE_TOTAL
	            FROM
	                PCPRODFILIAL
	            WHERE
	                CODFILIAL IN (1, 2, 3, 4, 5)
	                AND PCPRODFILIAL.CODPROD = A.CODPROD
	        ) AS ESTOQUE_MAX_MULTI_FILIAL,
	        
	        
		        GM.GIRO_3_MESES,
		        VM.MES_ANT_1,
		        VM.MES_ANT_2,
		        VM.MES_ANT_3,
		        VM.MES_ATUAL,
	        
	         CASE
                    WHEN A.ESTOQUE < A.ESTMIN THEN 'RUPTURA'
                    WHEN A.ESTOQUE >=  A.ESTMIN AND A.ESTOQUE < A.ESTOQUE_IDEAL THEN 'BAIXO'
                    WHEN A.ESTOQUE = A.ESTOQUE_IDEAL THEN 'IDEAL'
                    WHEN A.ESTOQUE > A.ESTOQUE_IDEAL AND A.ESTOQUE < A.ESTMAX THEN 'ALTO'
                    WHEN A.ESTOQUE >= A.ESTMAX THEN 'EXCESSO'
                    ELSE 'ERRO'
                END AS STATUS_ESTOQUE

            FROM ENTRADAS A
            LEFT JOIN F1 ON F1.CODPROD = A.CODPROD
		    LEFT JOIN F2 ON F2.CODPROD = A.CODPROD
		    LEFT JOIN F3 ON F3.CODPROD = A.CODPROD
		    LEFT JOIN F4 ON F4.CODPROD = A.CODPROD
		    LEFT JOIN F5 ON F5.CODPROD = A.CODPROD
		    LEFT JOIN GIRO_MES GM ON GM.CODPROD = A.CODPROD
		    LEFT JOIN VENDAS_3_MESES VM ON VM.CODPROD = A.CODPROD
            WHERE A.RN = 1
				
				)
			SELECT 
			F.*,
			
			((ESTOQUE_IDEAL_MULTI_FILIAL) - (F.ESTOQUE_MULTI_FILIAL)) AS QTD_SUGERIDA_MULTI_FILIAL_MENOS_ESTOQUE,
			CASE
                    WHEN F.ESTOQUE_MULTI_FILIAL < F.ESTOQUE_MIN_MULTI_FILIAL THEN 'RUPTURA'
                    WHEN F.ESTOQUE_MULTI_FILIAL >=  F.ESTOQUE_MIN_MULTI_FILIAL AND F.ESTOQUE_MULTI_FILIAL < F.ESTOQUE_IDEAL_MULTI_FILIAL THEN 'BAIXO'
                    WHEN F.ESTOQUE_MULTI_FILIAL = F.ESTOQUE_IDEAL_MULTI_FILIAL THEN 'IDEAL'
                    WHEN F.ESTOQUE_MULTI_FILIAL > F.ESTOQUE_IDEAL_MULTI_FILIAL AND F.ESTOQUE_MULTI_FILIAL < F.ESTOQUE_MAX_MULTI_FILIAL THEN 'ALTO'
                    WHEN F.ESTOQUE_MULTI_FILIAL >= F.ESTOQUE_MAX_MULTI_FILIAL THEN 'EXCESSO'
                    ELSE 'ERRO'
             END AS STATUS_ESTOQUE_MULT_FILIAL
			
			
			FROM FINAL F

         ";

        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        // Associar parâmetros ao placeholder SQL

        oci_bind_by_name($stmt, ":fornecedor", $fornecedor);
        oci_bind_by_name($stmt, ":codfilial", $codfilial);

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
            // Conversão de tipos para garantir que os campos sejam numéricos
            $row['CODFILIAL'] = isset($row['CODFILIAL']) ? (int)$row['CODFILIAL'] : null;
            $row['CODPROD'] = isset($row['CODPROD']) ? (int)$row['CODPROD'] : null;
            $row['CODFORNEC'] = isset($row['CODFORNEC']) ? (int)$row['CODFORNEC'] : null;
            $row['CODEPTO'] = isset($row['CODEPTO']) ? (int)$row['CODEPTO'] : null;
            $row['ESTMIN'] = isset($row['ESTMIN']) ? (int)$row['ESTMIN'] : null;
            $row['ESTMAX'] = isset($row['ESTMAX']) ? (int)$row['ESTMAX'] : null;
            $row['ESTOQUE'] = isset($row['ESTOQUE']) ? (int)$row['ESTOQUE'] : null;
            $row['QTGIRODIA'] = isset($row['QTGIRODIA']) ? (int)$row['QTGIRODIA'] : null;
            $row['ESTOQUE_IDEAL'] = isset($row['ESTOQUE_IDEAL']) ? (int)$row['ESTOQUE_IDEAL'] : null;
            $row['PRAZOENTREGA'] = isset($row['PRAZOENTREGA']) ? (int)$row['PRAZOENTREGA'] : null;
            $row['CUSTOULTENT'] = isset($row['CUSTOULTENT']) ? (int)$row['CUSTOULTENT'] : null;
            $row['VALORULTENT'] = isset($row['VALORULTENT']) ? (int)$row['VALORULTENT'] : null;
            $row['TEMREPOS'] = isset($row['TEMREPOS']) ? (int)$row['TEMREPOS'] : null;
            $row['ESTOQUE_IDEAL_MULTI_FILIAL'] = isset($row['ESTOQUE_IDEAL_MULTI_FILIAL']) ? (int)$row['ESTOQUE_IDEAL_MULTI_FILIAL'] : null;
            $row['ESTOQUE_MULTI_FILIAL'] = isset($row['ESTOQUE_MULTI_FILIAL']) ? (int)$row['ESTOQUE_MULTI_FILIAL'] : null;
            $row['ESTOQUE_MIN_MULTI_FILIAL'] = isset($row['ESTOQUE_MIN_MULTI_FILIAL']) ? (int)$row['ESTOQUE_MIN_MULTI_FILIAL'] : null;
            $row['ESTOQUE_MAX_MULTI_FILIAL'] = isset($row['ESTOQUE_MAX_MULTI_FILIAL']) ? (int)$row['ESTOQUE_MAX_MULTI_FILIAL'] : null;
            $row['GIRO_3_MESES'] = isset($row['GIRO_3_MESES']) ? (int)$row['GIRO_3_MESES'] : null;

            $row['MES_ANT_1'] = isset($row['MES_ANT_1']) ? (int)$row['MES_ANT_1'] : null;
            $row['MES_ANT_2'] = isset($row['MES_ANT_2']) ? (int)$row['MES_ANT_2'] : null;
            $row['MES_ANT_3'] = isset($row['MES_ANT_3']) ? (int)$row['MES_ANT_3'] : null;
            $row['MES_ATUAL'] = isset($row['MES_ATUAL']) ? (int)$row['MES_ATUAL'] : null;
            $row['PEDIDO_PENDENTE'] = isset($row['PEDIDO_PENDENTE']) ? (int)$row['PEDIDO_PENDENTE'] : null;
            $row['SUGESTAO_COMPRA'] = isset($row['SUGESTAO_COMPRA']) ? (int)$row['SUGESTAO_COMPRA'] : null;
            $row['QTD_SUGERIDA_MULTI_FILIAL_MENOS_ESTOQUE'] = isset($row['QTD_SUGERIDA_MULTI_FILIAL_MENOS_ESTOQUE']) ? (int)$row['QTD_SUGERIDA_MULTI_FILIAL_MENOS_ESTOQUE'] : null;
          
            $filiais[] = $row;
        }

        // Fechar a conexão
        oci_free_statement($stmt);
        oci_close($conexao);

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
    $this->get('/agenda/pedido/produto/{NUMPED}', function (Request $request, Response $response) {
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

        $NUMPED = $request->getAttribute('NUMPED');

        // Consulta SQL com GROUP BY e formato de data correto
        $sql = "SELECT 
                PE.*, 
                F.FORNECEDOR,
                P.DESCRICAO AS PRODUTO
            FROM SITEPREPEDIDOCOMPRA PE,  PCFORNEC F, PCPRODUT P, PCEST E
            WHERE PE.CODFORNECEDOR = F.CODFORNEC
            AND PE.CODPROD = P.CODPROD
            AND PE.CODPROD = E.CODPROD
            AND PE.CODFILIAL = E.CODFILIAL
            AND PE.NUMPED = :NUMPED
         ";




        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        // Associar parâmetros ao placeholder SQL

        oci_bind_by_name($stmt, ":NUMPED", $NUMPED);

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
            // Conversão de tipos para garantir que os campos sejam numéricos
         
          
            $filiais[] = $row;
        }

        // Fechar a conexão
        oci_free_statement($stmt);
        oci_close($conexao);

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
    $this->get('/agenda/lista/pedido', function (Request $request, Response $response) {
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

        $fornecedor = $request->getAttribute('fornecedor');

        // Consulta SQL com GROUP BY e formato de data correto
        $sql = "SELECT 
                PE.NUMPED, 
                F.CODFORNEC,
                F.FORNECEDOR,
                SUM(QT * CUSTO) AS VALOR
            FROM SITEPREPEDIDOCOMPRA PE,  PCFORNEC F
            WHERE PE.CODFORNECEDOR = F.CODFORNEC
            GROUP BY PE.NUMPED, F.CODFORNEC, F.FORNECEDOR
         ";

        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        // Associar parâmetros ao placeholder SQL


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
            // Conversão de tipos para garantir que os campos sejam numéricos
            $row['NUMPED'] = isset($row['NUMPED']) ? (int)$row['NUMPED'] : null;
            $row['CODFORNEC'] = isset($row['CODFORNEC']) ? (int)$row['CODFORNEC'] : null;
            $row['VALOR'] = isset($row['VALOR']) ? (int)$row['VALOR'] : null;
          
            $filiais[] = $row;
        }

        // Fechar a conexão
        oci_free_statement($stmt);
        oci_close($conexao);

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
    

});
