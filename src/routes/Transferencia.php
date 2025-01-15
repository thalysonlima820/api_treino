<?php

use Slim\Http\Request;
use Slim\Http\Response;

use Symfony\Component\Console\Descriptor\Descriptor;

// Rota para listar os dados de PCFILIAL
$app->group('/api/v1', function () {

    $this->get('/transferencia/estrategica/lista/{filial_saida}/{filial_entrada}/{codepto}', function (Request $request, Response $response) {

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

        $filial_saida = $request->getAttribute('filial_saida');
        $filial_entrada = $request->getAttribute('filial_entrada');
        $codepto = $request->getAttribute('codepto');

        $sql = "WITH ESTOQUE_FIC AS (

            SELECT E.CODPROD, E.CODFILIAL
            FROM PCEST E

            ),      
                DIAS_DEPTO AS (

                SELECT 
                    D.CODEPTO, 
                    D.DESCRICAO,

                    CASE
                        WHEN D.CODEPTO = 17 THEN 5
                        WHEN D.CODEPTO = 18 THEN 15
                        WHEN D.CODEPTO = 19 THEN 10
                        WHEN D.CODEPTO = 20 THEN 5
                        WHEN D.CODEPTO = 21 THEN 10
                        WHEN D.CODEPTO = 22 THEN 10
                        WHEN D.CODEPTO = 23 THEN 10
                        WHEN D.CODEPTO = 24 THEN 10
                        WHEN D.CODEPTO = 25 THEN 5
                        WHEN D.CODEPTO = 26 THEN 5
                        WHEN D.CODEPTO = 27 THEN 5
                        WHEN D.CODEPTO = 28 THEN 10
                        WHEN D.CODEPTO = 116 THEN 15
                    END AS DIASESTOQUE
                
                FROM PCDEPTO D
                WHERE D.CODEPTO IN (17,18,19,20,21,22,23,24,25,26,27,28,116)
                ORDER BY D.CODEPTO

                ),

            FILIA_SAIR AS (

            SELECT 
                FIC.CODFILIAL AS CODFILIAL_S,
                FIC.CODPROD, 
                p.CODAUXILIAR,
                P.DESCRICAO AS PRODUTO,
                D.CODEPTO,
                D.DESCRICAO AS DEPARTAMENTO,
                C.CODSEC, 
                C.DESCRICAO,
                MC.MARCA,
                (E.QTESTGER - E.QTRESERV - E.QTBLOQUEADA) AS ESTOQUE_S,
                E.ESTMIN AS ESTMIN_S,
                E.ESTMAX AS ESTMAX_S,
                E.QTGIRODIA AS GIRODIA_S,
                P.TEMREPOS AS TEMREPOS_S,
                    CASE 
                        WHEN (E.QTGIRODIA * P.TEMREPOS) > E.ESTMIN THEN (E.QTGIRODIA * P.TEMREPOS)
                        ELSE E.ESTMIN
                    END AS ESTOQUE_IDEAL_S

            FROM ESTOQUE_FIC FIC, PCPRODUT P, PCEST E, PCDEPTO D, PCSECAO C, PCMARCA MC
            WHERE FIC.CODPROD = P.CODPROD
            AND FIC.CODPROD = E.CODPROD 
            AND FIC.CODFILIAL = E.CODFILIAL
            AND P.CODMARCA = MC.CODMARCA
            AND P.CODEPTO = D.CODEPTO
            AND P.CODSEC = C.CODSEC

            AND FIC.CODFILIAL = :filial_saida
            AND D.CODEPTO = :codepto

            ORDER BY FIC.CODFILIAL

            ),

            FILIA_ENTRAR AS (

            SELECT 
                FIC.CODFILIAL AS CODFILIAL_E,
                FIC.CODPROD, 
                E.QTBLOQUEADA AS QTBLOQUEADA_E,
                P.DESCRICAO AS PRODUTO,
                D.CODEPTO,
                D.DESCRICAO AS DEPARTAMENTO,
				C.CODSEC, 
                C.DESCRICAO,
                (E.QTESTGER - E.QTRESERV - E.QTBLOQUEADA) AS ESTOQUE_E,
                E.ESTMIN AS ESTMIN_E,
                E.ESTMAX AS ESTMAX_E,
                E.QTGIRODIA AS GIRODIA_E,
                P.TEMREPOS AS TEMREPOS_E,
                    CASE 
                        WHEN (E.QTGIRODIA * P.TEMREPOS) > E.ESTMIN THEN (E.QTGIRODIA * P.TEMREPOS)
                        ELSE E.ESTMIN
                    END AS ESTOQUE_IDEAL_E


            FROM ESTOQUE_FIC FIC, PCPRODUT P, PCEST E, PCDEPTO D,  PCSECAO C
            WHERE FIC.CODPROD = P.CODPROD
            AND FIC.CODPROD = E.CODPROD
            AND FIC.CODFILIAL = E.CODFILIAL
            AND P.CODEPTO = D.CODEPTO
            AND P.CODSEC = C.CODSEC

            AND FIC.CODFILIAL = :filial_entrada

            ORDER BY FIC.CODFILIAL

            ),

            RESUL AS (


            SELECT 

                S.CODFILIAL_S,
                s.CODAUXILIAR,
                S.CODPROD, 
                S.PRODUTO,
                S.CODEPTO,
                S.DEPARTAMENTO,
                S.CODSEC, 
                S.DESCRICAO AS SECAO,
                S.MARCA,
                S.ESTOQUE_S,
                S.ESTMIN_S,
                S.ESTMAX_S,
                S.GIRODIA_S,
                S.TEMREPOS_S,
                S.ESTOQUE_IDEAL_S,
                CASE
                    WHEN S.ESTOQUE_S < S.ESTMIN_S THEN 'RUPTURA'
                    WHEN S.ESTOQUE_S >= S.ESTMIN_S AND S.ESTOQUE_S < S.ESTOQUE_IDEAL_S THEN 'BAIXO'
                    WHEN S.ESTOQUE_S = S.ESTOQUE_IDEAL_S THEN 'IDEAL'
                    WHEN S.ESTOQUE_S > S.ESTOQUE_IDEAL_S AND S.ESTOQUE_S < S.ESTMAX_S THEN 'ALTO'
                    WHEN S.ESTOQUE_S >= S.ESTMAX_S THEN 'EXCESSO'
                    ELSE 'ERRO'
                END AS STATUS_ESTOQUE_S,
                
                E.CODFILIAL_E,
                E.QTBLOQUEADA_E,
                E.ESTOQUE_E,
                E.ESTMIN_E,
                E.ESTMAX_E,
                E.GIRODIA_E,
                E.TEMREPOS_E,
                E.ESTOQUE_IDEAL_E,
                CASE
                    WHEN E.ESTOQUE_E < E.ESTMIN_E THEN 'RUPTURA'
                    WHEN E.ESTOQUE_E >= E.ESTMIN_E AND E.ESTOQUE_E < E.ESTOQUE_IDEAL_E THEN 'BAIXO'
                    WHEN E.ESTOQUE_E = E.ESTOQUE_IDEAL_E THEN 'IDEAL'
                    WHEN E.ESTOQUE_E > E.ESTOQUE_IDEAL_E AND E.ESTOQUE_E < E.ESTMAX_E THEN 'ALTO'
                    WHEN E.ESTOQUE_E >= E.ESTMAX_E THEN 'EXCESSO'
                    ELSE 'ERRO'
                END AS STATUS_ESTOQUE_E,
                
                
                ROUND((E.ESTOQUE_IDEAL_E - E.ESTOQUE_E),0) SUGESTAO,
                ROUND((S.ESTOQUE_S - S.ESTOQUE_IDEAL_S),0) QT_MAX_TRANSF
                
            FROM FILIA_SAIR S, FILIA_ENTRAR E
            WHERE S.CODPROD = E.CODPROD

            AND S.ESTOQUE_S >= S.ESTMIN_S
            AND E.ESTOQUE_E <= E.ESTMAX_E

            ),
            RESULTADO_FD AS (
            
	            SELECT R.*, E.DIASESTOQUE AS TEMPO
	            FROM RESUL R, DIAS_DEPTO E
	            WHERE R.CODEPTO = E.CODEPTO
	            AND R.ESTMAX_E > 0
	            AND R.STATUS_ESTOQUE_S IN ('ALTO', 'EXCESSO', 'ERRO')
	            AND R.STATUS_ESTOQUE_E NOT IN ('ALTO', 'EXCESSO', 'IDEAL')
            ),
            CLIENTE AS (
            
	            SELECT 
					CASE 
						 WHEN F.CODIGO = 1 THEN 4
						 WHEN F.CODIGO = 2 THEN 317
						 WHEN F.CODIGO = 3 THEN 5
						 WHEN F.CODIGO = 4 THEN 8
						 WHEN F.CODIGO = 5 THEN 1674
					END AS CODCLIENTE
				
				FROM PCFILIAL F
				WHERE F.CODIGO = :filial_entrada
            
            ),
            TRANSITO AS (
            
            SELECT P.CODCLI, P.CODPROD, SUM(P.QT) AS QT FROM PCPEDI P
            WHERE P.POSICAO = 'M'
            GROUP BY P.CODCLI, P.CODPROD
            ),
            ULTIMA_ENTRADA AS (
            	
            	SELECT M.CODPROD, M.DTULTENTANT, ROW_NUMBER() OVER (PARTITION BY M.CODPROD ORDER BY M.DTULTENTANT DESC) AS RN
				FROM FILIA_ENTRAR E
				JOIN PCMOV M
				  ON E.CODPROD = M.CODPROD
				WHERE M.CODOPER IN ('E', 'ET')

            )
            
            
            SELECT 
			    R.*, 
			    COALESCE(P.QT, 0) AS QTTRANSITO ,
			    ULT.DTULTENTANT
			FROM 
			    RESULTADO_FD R
			LEFT JOIN 
			    TRANSITO P 
			ON R.CODPROD = P.CODPROD
			LEFT JOIN 
				CLIENTE C 
			ON P.CODCLI = C.CODCLIENTE
			
			JOIN ULTIMA_ENTRADA ULT
				ON R.CODPROD = ULT.CODPROD AND ULT.RN = 1
			WHERE R.ESTOQUE_S > 12
		  


        ";


        $stmt = oci_parse($conexao, $sql);

        // Associar a data formatada ao placeholder SQL
        oci_bind_by_name($stmt, ":filial_saida", $filial_saida);
        oci_bind_by_name($stmt, ":filial_entrada", $filial_entrada);
        oci_bind_by_name($stmt, ":codepto", $codepto);

        // Executa a consulta
        oci_execute($stmt);

        // Coletar os resultados em um array
        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) != false) {

            $row['CODFILIAL_S'] = isset($row['CODFILIAL_S']) ? (int)$row['CODFILIAL_S'] : null;
            $row['CODPROD'] = isset($row['CODPROD']) ? (int)$row['CODPROD'] : null;
            $row['CODEPTO'] = isset($row['CODEPTO']) ? (int)$row['CODEPTO'] : null;
            $row['CODSEC'] = isset($row['CODSEC']) ? (int)$row['CODSEC'] : null;
            $row['ESTOQUE_S'] = isset($row['ESTOQUE_S']) ? (float)$row['ESTOQUE_S'] : null;
            $row['ESTMIN_S'] = isset($row['ESTMIN_S']) ? (float)$row['ESTMIN_S'] : null;
            $row['ESTMAX_S'] = isset($row['ESTMAX_S']) ? (float)$row['ESTMAX_S'] : null;
            $row['GIRODIA_S'] = isset($row['GIRODIA_S']) ? (float)$row['GIRODIA_S'] : null;
            $row['ESTOQUE_IDEAL_S'] = isset($row['ESTOQUE_IDEAL_S']) ? (float)$row['ESTOQUE_IDEAL_S'] : null;

            $row['CODFILIAL_E'] = isset($row['CODFILIAL_E']) ? (int)$row['CODFILIAL_E'] : null;
            $row['ESTOQUE_E'] = isset($row['ESTOQUE_E']) ? (float)$row['ESTOQUE_E'] : null;
            $row['ESTMIN_E'] = isset($row['ESTMIN_E']) ? (float)$row['ESTMIN_E'] : null;
            $row['ESTMAX_E'] = isset($row['ESTMAX_E']) ? (float)$row['ESTMAX_E'] : null;
            $row['GIRODIA_E'] = isset($row['GIRODIA_E']) ? (float)$row['GIRODIA_E'] : null;
            $row['ESTOQUE_IDEAL_E'] = isset($row['ESTOQUE_IDEAL_E']) ? (float)$row['ESTOQUE_IDEAL_E'] : null;
            $row['QTBLOQUEADA_E'] = isset($row['QTBLOQUEADA_E']) ? (float)$row['QTBLOQUEADA_E'] : null;

            $row['SUGESTAO'] = isset($row['SUGESTAO']) ? (float)$row['SUGESTAO'] : null;
            $row['QT_MAX_TRANSF'] = isset($row['QT_MAX_TRANSF']) ? (float)$row['QT_MAX_TRANSF'] : null;
            $row['QTTRANSITO'] = isset($row['QTTRANSITO']) ? (float)$row['QTTRANSITO'] : null;
            $row['TEMPO'] = isset($row['TEMPO']) ? (int)$row['TEMPO'] : null;
            $row['CODAUXILIAR'] = isset($row['CODAUXILIAR']) ? (int)$row['CODAUXILIAR'] : null;

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

    $this->get('/transferencia/estrategica/geral/{filial_saida}/{filial_entrada}/{codepto}', function (Request $request, Response $response) {

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

        $filial_saida = $request->getAttribute('filial_saida');
        $filial_entrada = $request->getAttribute('filial_entrada');
        $codepto = $request->getAttribute('codepto');

        $sql = " WITH ESTOQUE_FIC AS (

            SELECT E.CODPROD, E.CODFILIAL
            FROM PCEST E

            ),      
                DIAS_DEPTO AS (

                SELECT 
                    D.CODEPTO, 
                    D.DESCRICAO,

                    CASE
                        WHEN D.CODEPTO = 17 THEN 5
                        WHEN D.CODEPTO = 18 THEN 15
                        WHEN D.CODEPTO = 19 THEN 10
                        WHEN D.CODEPTO = 20 THEN 5
                        WHEN D.CODEPTO = 21 THEN 10
                        WHEN D.CODEPTO = 22 THEN 10
                        WHEN D.CODEPTO = 23 THEN 10
                        WHEN D.CODEPTO = 24 THEN 10
                        WHEN D.CODEPTO = 25 THEN 5
                        WHEN D.CODEPTO = 26 THEN 5
                        WHEN D.CODEPTO = 27 THEN 5
                        WHEN D.CODEPTO = 28 THEN 10
                        WHEN D.CODEPTO = 116 THEN 15
                    END AS DIASESTOQUE
                
                FROM PCDEPTO D
                WHERE D.CODEPTO IN (17,18,19,20,21,22,23,24,25,26,27,28,116)
                ORDER BY D.CODEPTO

                ),

            FILIA_SAIR AS (

            SELECT 
                FIC.CODFILIAL AS CODFILIAL_S,
                FIC.CODPROD, 
                p.CODAUXILIAR,
                P.DESCRICAO AS PRODUTO,
                D.CODEPTO,
                D.DESCRICAO AS DEPARTAMENTO,
                C.CODSEC, 
                C.DESCRICAO,
                MC.MARCA,
                (E.QTESTGER - E.QTRESERV - E.QTBLOQUEADA) AS ESTOQUE_S,
                E.ESTMIN AS ESTMIN_S,
                E.ESTMAX AS ESTMAX_S,
                E.QTGIRODIA AS GIRODIA_S,
                P.TEMREPOS AS TEMREPOS_S,
                    CASE 
                        WHEN (E.QTGIRODIA * P.TEMREPOS) > E.ESTMIN THEN (E.QTGIRODIA * P.TEMREPOS)
                        ELSE E.ESTMIN
                    END AS ESTOQUE_IDEAL_S

            FROM ESTOQUE_FIC FIC, PCPRODUT P, PCEST E, PCDEPTO D, PCSECAO C, PCMARCA MC
            WHERE FIC.CODPROD = P.CODPROD
            AND FIC.CODPROD = E.CODPROD 
            AND FIC.CODFILIAL = E.CODFILIAL
            AND P.CODMARCA = MC.CODMARCA
            AND P.CODEPTO = D.CODEPTO
            AND P.CODSEC = C.CODSEC

            AND FIC.CODFILIAL = :filial_saida
            AND D.CODEPTO = :codepto

            ORDER BY FIC.CODFILIAL

            ),

            FILIA_ENTRAR AS (

            SELECT 
                FIC.CODFILIAL AS CODFILIAL_E,
                FIC.CODPROD, 
                E.QTBLOQUEADA AS QTBLOQUEADA_E,
                P.DESCRICAO AS PRODUTO,
                D.CODEPTO,
                D.DESCRICAO AS DEPARTAMENTO,
				C.CODSEC, 
                C.DESCRICAO,
                (E.QTESTGER - E.QTRESERV - E.QTBLOQUEADA) AS ESTOQUE_E,
                E.ESTMIN AS ESTMIN_E,
                E.ESTMAX AS ESTMAX_E,
                E.QTGIRODIA AS GIRODIA_E,
                P.TEMREPOS AS TEMREPOS_E,
                    CASE 
                        WHEN (E.QTGIRODIA * P.TEMREPOS) > E.ESTMIN THEN (E.QTGIRODIA * P.TEMREPOS)
                        ELSE E.ESTMIN
                    END AS ESTOQUE_IDEAL_E


            FROM ESTOQUE_FIC FIC, PCPRODUT P, PCEST E, PCDEPTO D,  PCSECAO C
            WHERE FIC.CODPROD = P.CODPROD
            AND FIC.CODPROD = E.CODPROD
            AND FIC.CODFILIAL = E.CODFILIAL
            AND P.CODEPTO = D.CODEPTO
            AND P.CODSEC = C.CODSEC

            AND FIC.CODFILIAL = :filial_entrada

            ORDER BY FIC.CODFILIAL

            ),

            RESUL AS (


            SELECT 

                S.CODFILIAL_S,
                s.CODAUXILIAR,
                S.CODPROD, 
                S.PRODUTO,
                S.CODEPTO,
                S.DEPARTAMENTO,
                S.CODSEC, 
                S.DESCRICAO AS SECAO,
                S.MARCA,
                S.ESTOQUE_S,
                S.ESTMIN_S,
                S.ESTMAX_S,
                S.GIRODIA_S,
                S.TEMREPOS_S,
                S.ESTOQUE_IDEAL_S,
                CASE
                    WHEN S.ESTOQUE_S < S.ESTMIN_S THEN 'RUPTURA'
                    WHEN S.ESTOQUE_S >= S.ESTMIN_S AND S.ESTOQUE_S < S.ESTOQUE_IDEAL_S THEN 'BAIXO'
                    WHEN S.ESTOQUE_S = S.ESTOQUE_IDEAL_S THEN 'IDEAL'
                    WHEN S.ESTOQUE_S > S.ESTOQUE_IDEAL_S AND S.ESTOQUE_S < S.ESTMAX_S THEN 'ALTO'
                    WHEN S.ESTOQUE_S >= S.ESTMAX_S THEN 'EXCESSO'
                    ELSE 'ERRO'
                END AS STATUS_ESTOQUE_S,
                
                E.CODFILIAL_E,
                E.QTBLOQUEADA_E,
                E.ESTOQUE_E,
                E.ESTMIN_E,
                E.ESTMAX_E,
                E.GIRODIA_E,
                E.TEMREPOS_E,
                E.ESTOQUE_IDEAL_E,
                CASE
                    WHEN E.ESTOQUE_E < E.ESTMIN_E THEN 'RUPTURA'
                    WHEN E.ESTOQUE_E >= E.ESTMIN_E AND E.ESTOQUE_E < E.ESTOQUE_IDEAL_E THEN 'BAIXO'
                    WHEN E.ESTOQUE_E = E.ESTOQUE_IDEAL_E THEN 'IDEAL'
                    WHEN E.ESTOQUE_E > E.ESTOQUE_IDEAL_E AND E.ESTOQUE_E < E.ESTMAX_E THEN 'ALTO'
                    WHEN E.ESTOQUE_E >= E.ESTMAX_E THEN 'EXCESSO'
                    ELSE 'ERRO'
                END AS STATUS_ESTOQUE_E,
                
                
                ROUND((E.ESTOQUE_IDEAL_E - E.ESTOQUE_E),0) SUGESTAO,
                ROUND((S.ESTOQUE_S - S.ESTOQUE_IDEAL_S),0) QT_MAX_TRANSF
                
            FROM FILIA_SAIR S, FILIA_ENTRAR E
            WHERE S.CODPROD = E.CODPROD

            ),
            RESULTADO_FD AS (
            
	            SELECT R.*, E.DIASESTOQUE AS TEMPO
	            FROM RESUL R, DIAS_DEPTO E
	            WHERE R.CODEPTO = E.CODEPTO
	            AND R.ESTMAX_E > 0
            ),
            CLIENTE AS (
            
	            SELECT 
					CASE 
						 WHEN F.CODIGO = 1 THEN 4
						 WHEN F.CODIGO = 2 THEN 317
						 WHEN F.CODIGO = 3 THEN 5
						 WHEN F.CODIGO = 4 THEN 8
						 WHEN F.CODIGO = 5 THEN 1674
					END AS CODCLIENTE
				
				FROM PCFILIAL F
				WHERE F.CODIGO = :filial_entrada
            
            ),
            TRANSITO AS (
            
            SELECT P.CODCLI, P.CODPROD, SUM(P.QT) AS QT FROM PCPEDI P
            WHERE P.POSICAO = 'M'
            GROUP BY P.CODCLI, P.CODPROD
            )
          
            
            
            SELECT 
			    R.*, 
			    COALESCE(P.QT, 0) AS QTTRANSITO 
			   
			FROM 
			    RESULTADO_FD R
			LEFT JOIN 
			    TRANSITO P 
			ON R.CODPROD = P.CODPROD
			LEFT JOIN 
				CLIENTE C 
			ON P.CODCLI = C.CODCLIENTE
        ";


        $stmt = oci_parse($conexao, $sql);

        // Associar a data formatada ao placeholder SQL
        oci_bind_by_name($stmt, ":filial_saida", $filial_saida);
        oci_bind_by_name($stmt, ":filial_entrada", $filial_entrada);
        oci_bind_by_name($stmt, ":codepto", $codepto);

        // Executa a consulta
        oci_execute($stmt);

        // Coletar os resultados em um array
        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) != false) {

            $row['CODFILIAL_S'] = isset($row['CODFILIAL_S']) ? (int)$row['CODFILIAL_S'] : null;
            $row['CODPROD'] = isset($row['CODPROD']) ? (int)$row['CODPROD'] : null;
            $row['CODEPTO'] = isset($row['CODEPTO']) ? (int)$row['CODEPTO'] : null;
            $row['CODSEC'] = isset($row['CODSEC']) ? (int)$row['CODSEC'] : null;
            $row['ESTOQUE_S'] = isset($row['ESTOQUE_S']) ? (float)$row['ESTOQUE_S'] : null;
            $row['ESTMIN_S'] = isset($row['ESTMIN_S']) ? (float)$row['ESTMIN_S'] : null;
            $row['ESTMAX_S'] = isset($row['ESTMAX_S']) ? (float)$row['ESTMAX_S'] : null;
            $row['GIRODIA_S'] = isset($row['GIRODIA_S']) ? (float)$row['GIRODIA_S'] : null;
            $row['ESTOQUE_IDEAL_S'] = isset($row['ESTOQUE_IDEAL_S']) ? (float)$row['ESTOQUE_IDEAL_S'] : null;

            $row['CODFILIAL_E'] = isset($row['CODFILIAL_E']) ? (int)$row['CODFILIAL_E'] : null;
            $row['ESTOQUE_E'] = isset($row['ESTOQUE_E']) ? (float)$row['ESTOQUE_E'] : null;
            $row['ESTMIN_E'] = isset($row['ESTMIN_E']) ? (float)$row['ESTMIN_E'] : null;
            $row['ESTMAX_E'] = isset($row['ESTMAX_E']) ? (float)$row['ESTMAX_E'] : null;
            $row['GIRODIA_E'] = isset($row['GIRODIA_E']) ? (float)$row['GIRODIA_E'] : null;
            $row['ESTOQUE_IDEAL_E'] = isset($row['ESTOQUE_IDEAL_E']) ? (float)$row['ESTOQUE_IDEAL_E'] : null;
            $row['QTBLOQUEADA_E'] = isset($row['QTBLOQUEADA_E']) ? (float)$row['QTBLOQUEADA_E'] : null;

            $row['SUGESTAO'] = isset($row['SUGESTAO']) ? (float)$row['SUGESTAO'] : null;
            $row['QT_MAX_TRANSF'] = isset($row['QT_MAX_TRANSF']) ? (float)$row['QT_MAX_TRANSF'] : null;
            $row['QTTRANSITO'] = isset($row['QTTRANSITO']) ? (float)$row['QTTRANSITO'] : null;
            $row['TEMPO'] = isset($row['TEMPO']) ? (int)$row['TEMPO'] : null;
            $row['CODAUXILIAR'] = isset($row['CODAUXILIAR']) ? (int)$row['CODAUXILIAR'] : null;

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

    $this->get('/transferencia/estrategica/mix/{filial_saida}/{filial_entrada}/{codepto}', function (Request $request, Response $response) {

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

        $filial_saida = $request->getAttribute('filial_saida');
        $filial_entrada = $request->getAttribute('filial_entrada');
        $codepto = $request->getAttribute('codepto');

        $sql = "WITH ESTOQUE_FIC AS (

            SELECT E.CODPROD, E.CODFILIAL
            FROM PCEST E

            ),      
                DIAS_DEPTO AS (

                SELECT 
                    D.CODEPTO, 
                    D.DESCRICAO,

                    CASE
                        WHEN D.CODEPTO = 17 THEN 5
                        WHEN D.CODEPTO = 18 THEN 15
                        WHEN D.CODEPTO = 19 THEN 10
                        WHEN D.CODEPTO = 20 THEN 5
                        WHEN D.CODEPTO = 21 THEN 10
                        WHEN D.CODEPTO = 22 THEN 10
                        WHEN D.CODEPTO = 23 THEN 10
                        WHEN D.CODEPTO = 24 THEN 10
                        WHEN D.CODEPTO = 25 THEN 5
                        WHEN D.CODEPTO = 26 THEN 5
                        WHEN D.CODEPTO = 27 THEN 5
                        WHEN D.CODEPTO = 28 THEN 10
                        WHEN D.CODEPTO = 116 THEN 15
                    END AS DIASESTOQUE
                
                FROM PCDEPTO D
                WHERE D.CODEPTO IN (17,18,19,20,21,22,23,24,25,26,27,28,116)
                ORDER BY D.CODEPTO

                ),

            FILIA_SAIR AS (

            SELECT 
                FIC.CODFILIAL AS CODFILIAL_S,
                FIC.CODPROD, 
                p.CODAUXILIAR,
                P.DESCRICAO AS PRODUTO,
                D.CODEPTO,
                D.DESCRICAO AS DEPARTAMENTO,
                C.CODSEC, 
                C.DESCRICAO,
                MC.MARCA,
                (E.QTESTGER - E.QTRESERV - E.QTBLOQUEADA) AS ESTOQUE_S,
                E.ESTMIN AS ESTMIN_S,
                E.ESTMAX AS ESTMAX_S,
                E.QTGIRODIA AS GIRODIA_S,
                P.TEMREPOS AS TEMREPOS_S,
                    CASE 
                        WHEN (E.QTGIRODIA * P.TEMREPOS) > E.ESTMIN THEN (E.QTGIRODIA * P.TEMREPOS)
                        ELSE E.ESTMIN
                    END AS ESTOQUE_IDEAL_S

            FROM ESTOQUE_FIC FIC, PCPRODUT P, PCEST E, PCDEPTO D, PCSECAO C, PCMARCA MC
            WHERE FIC.CODPROD = P.CODPROD
            AND FIC.CODPROD = E.CODPROD 
            AND FIC.CODFILIAL = E.CODFILIAL
            AND P.CODMARCA = MC.CODMARCA
            AND P.CODEPTO = D.CODEPTO
            AND P.CODSEC = C.CODSEC

            AND FIC.CODFILIAL = :filial_saida
            AND D.CODEPTO = :codepto

            ORDER BY FIC.CODFILIAL

            ),

            FILIA_ENTRAR AS (

            SELECT 
                FIC.CODFILIAL AS CODFILIAL_E,
                FIC.CODPROD, 
                P.DESCRICAO AS PRODUTO,
                D.CODEPTO,
                D.DESCRICAO AS DEPARTAMENTO,
				C.CODSEC, 
                C.DESCRICAO,
                (E.QTESTGER - E.QTRESERV - E.QTBLOQUEADA) AS ESTOQUE_E,
                E.ESTMIN AS ESTMIN_E,
                E.ESTMAX AS ESTMAX_E,
                E.QTGIRODIA AS GIRODIA_E,
                P.TEMREPOS AS TEMREPOS_E,
                    CASE 
                        WHEN (E.QTGIRODIA * P.TEMREPOS) > E.ESTMIN THEN (E.QTGIRODIA * P.TEMREPOS)
                        ELSE E.ESTMIN
                    END AS ESTOQUE_IDEAL_E


            FROM ESTOQUE_FIC FIC, PCPRODUT P, PCEST E, PCDEPTO D,  PCSECAO C
            WHERE FIC.CODPROD = P.CODPROD
            AND FIC.CODPROD = E.CODPROD
            AND FIC.CODFILIAL = E.CODFILIAL
            AND P.CODEPTO = D.CODEPTO
            AND P.CODSEC = C.CODSEC

            AND FIC.CODFILIAL = :filial_entrada

            ORDER BY FIC.CODFILIAL

            ),

            RESUL AS (


            SELECT 

                S.CODFILIAL_S,
                s.CODAUXILIAR,
                S.CODPROD, 
                S.PRODUTO,
                S.CODEPTO,
                S.DEPARTAMENTO,
                S.CODSEC, 
                S.DESCRICAO AS SECAO,
                S.MARCA,
                S.ESTOQUE_S,
                S.ESTMIN_S,
                S.ESTMAX_S,
                S.GIRODIA_S,
                S.TEMREPOS_S,
                S.ESTOQUE_IDEAL_S,
                CASE
                    WHEN S.ESTOQUE_S < S.ESTMIN_S THEN 'RUPTURA'
                    WHEN S.ESTOQUE_S >= S.ESTMIN_S AND S.ESTOQUE_S < S.ESTOQUE_IDEAL_S THEN 'BAIXO'
                    WHEN S.ESTOQUE_S = S.ESTOQUE_IDEAL_S THEN 'IDEAL'
                    WHEN S.ESTOQUE_S > S.ESTOQUE_IDEAL_S AND S.ESTOQUE_S < S.ESTMAX_S THEN 'ALTO'
                    WHEN S.ESTOQUE_S >= S.ESTMAX_S THEN 'EXCESSO'
                    ELSE 'ERRO'
                END AS STATUS_ESTOQUE_S,
                
                E.CODFILIAL_E,
                E.ESTOQUE_E,
                E.ESTMIN_E,
                E.ESTMAX_E,
                E.GIRODIA_E,
                E.TEMREPOS_E,
                E.ESTOQUE_IDEAL_E,
                CASE
                    WHEN E.ESTOQUE_E < E.ESTMIN_E THEN 'RUPTURA'
                    WHEN E.ESTOQUE_E >= E.ESTMIN_E AND E.ESTOQUE_E < E.ESTOQUE_IDEAL_E THEN 'BAIXO'
                    WHEN E.ESTOQUE_E = E.ESTOQUE_IDEAL_E THEN 'IDEAL'
                    WHEN E.ESTOQUE_E > E.ESTOQUE_IDEAL_E AND E.ESTOQUE_E < E.ESTMAX_E THEN 'ALTO'
                    WHEN E.ESTOQUE_E >= E.ESTMAX_E THEN 'EXCESSO'
                    ELSE 'ERRO'
                END AS STATUS_ESTOQUE_E,
                
                
                ROUND((E.ESTOQUE_IDEAL_E - E.ESTOQUE_E),0) SUGESTAO,
                ROUND((S.ESTOQUE_S - S.ESTOQUE_IDEAL_S),0) QT_MAX_TRANSF
                
            FROM FILIA_SAIR S, FILIA_ENTRAR E
            WHERE S.CODPROD = E.CODPROD

            AND S.ESTOQUE_S >= S.ESTMIN_S
            AND E.ESTOQUE_E <= E.ESTMAX_E

            ),
            RESULTADO_FD AS (
            
	            SELECT R.*, E.DIASESTOQUE AS TEMPO
	            FROM RESUL R, DIAS_DEPTO E
	            WHERE R.CODEPTO = E.CODEPTO
	            AND R.ESTMAX_E > 0
	            AND R.STATUS_ESTOQUE_S IN ('ALTO', 'EXCESSO', 'ERRO')
	            AND R.STATUS_ESTOQUE_E NOT IN ('ALTO', 'EXCESSO', 'IDEAL')
            ),
            CLIENTE AS (
            
	            SELECT 
					CASE 
						 WHEN F.CODIGO = 1 THEN 4
						 WHEN F.CODIGO = 2 THEN 317
						 WHEN F.CODIGO = 3 THEN 5
						 WHEN F.CODIGO = 4 THEN 8
						 WHEN F.CODIGO = 5 THEN 1674
					END AS CODCLIENTE
				
				FROM PCFILIAL F
				WHERE F.CODIGO = :filial_entrada
            
            ),
            TRANSITO AS (
            
            SELECT P.CODCLI, P.CODPROD, SUM(P.QT) AS QT FROM PCPEDI P
            WHERE P.POSICAO = 'M'
            GROUP BY P.CODCLI, P.CODPROD
            ),
            ULTIMA_ENTRADA AS (
            	
            	SELECT M.CODPROD, M.DTULTENTANT, ROW_NUMBER() OVER (PARTITION BY M.CODPROD ORDER BY M.DTULTENTANT DESC) AS RN
				FROM FILIA_ENTRAR E
				JOIN PCMOV M
				  ON E.CODPROD = M.CODPROD
				WHERE M.CODOPER IN ('E', 'ET')


            )
            
            
            SELECT 
			    R.*, 
			    COALESCE(P.QT, 0) AS QTTRANSITO ,
			    ULT.DTULTENTANT
			FROM 
			    RESULTADO_FD R
			LEFT JOIN 
			    TRANSITO P 
			ON R.CODPROD = P.CODPROD
			LEFT JOIN 
				CLIENTE C 
			ON P.CODCLI = C.CODCLIENTE
			
			JOIN ULTIMA_ENTRADA ULT
				ON R.CODPROD = ULT.CODPROD AND ULT.RN = 1
			WHERE R.ESTOQUE_S > 12
			AND ULT.DTULTENTANT < SYSDATE - 150



        ";


        $stmt = oci_parse($conexao, $sql);

        // Associar a data formatada ao placeholder SQL
        oci_bind_by_name($stmt, ":filial_saida", $filial_saida);
        oci_bind_by_name($stmt, ":filial_entrada", $filial_entrada);
        oci_bind_by_name($stmt, ":codepto", $codepto);

        // Executa a consulta
        oci_execute($stmt);

        // Coletar os resultados em um array
        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) != false) {

            $row['CODFILIAL_S'] = isset($row['CODFILIAL_S']) ? (int)$row['CODFILIAL_S'] : null;
            $row['CODPROD'] = isset($row['CODPROD']) ? (int)$row['CODPROD'] : null;
            $row['CODEPTO'] = isset($row['CODEPTO']) ? (int)$row['CODEPTO'] : null;
            $row['CODSEC'] = isset($row['CODSEC']) ? (int)$row['CODSEC'] : null;
            $row['ESTOQUE_S'] = isset($row['ESTOQUE_S']) ? (float)$row['ESTOQUE_S'] : null;
            $row['ESTMIN_S'] = isset($row['ESTMIN_S']) ? (float)$row['ESTMIN_S'] : null;
            $row['ESTMAX_S'] = isset($row['ESTMAX_S']) ? (float)$row['ESTMAX_S'] : null;
            $row['GIRODIA_S'] = isset($row['GIRODIA_S']) ? (float)$row['GIRODIA_S'] : null;
            $row['ESTOQUE_IDEAL_S'] = isset($row['ESTOQUE_IDEAL_S']) ? (float)$row['ESTOQUE_IDEAL_S'] : null;

            $row['CODFILIAL_E'] = isset($row['CODFILIAL_E']) ? (int)$row['CODFILIAL_E'] : null;
            $row['ESTOQUE_E'] = isset($row['ESTOQUE_E']) ? (float)$row['ESTOQUE_E'] : null;
            $row['ESTMIN_E'] = isset($row['ESTMIN_E']) ? (float)$row['ESTMIN_E'] : null;
            $row['ESTMAX_E'] = isset($row['ESTMAX_E']) ? (float)$row['ESTMAX_E'] : null;
            $row['GIRODIA_E'] = isset($row['GIRODIA_E']) ? (float)$row['GIRODIA_E'] : null;
            $row['ESTOQUE_IDEAL_E'] = isset($row['ESTOQUE_IDEAL_E']) ? (float)$row['ESTOQUE_IDEAL_E'] : null;

            $row['SUGESTAO'] = isset($row['SUGESTAO']) ? (float)$row['SUGESTAO'] : null;
            $row['QT_MAX_TRANSF'] = isset($row['QT_MAX_TRANSF']) ? (float)$row['QT_MAX_TRANSF'] : null;
            $row['QTTRANSITO'] = isset($row['QTTRANSITO']) ? (float)$row['QTTRANSITO'] : null;
            $row['TEMPO'] = isset($row['TEMPO']) ? (int)$row['TEMPO'] : null;
            $row['CODAUXILIAR'] = isset($row['CODAUXILIAR']) ? (int)$row['CODAUXILIAR'] : null;

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
    //gerar numero do pedido
    $this->get('/transferencia/gerar/numpedido', function (Request $request, Response $response) {
        // Configurações do banco
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
    
        try {
            // Consulta para obter o número do pedido atual
            $sqlSelect = "SELECT PROXNUMPED AS NUMPED FROM PCUSUARI WHERE CODUSUR = 1";
            $stmtSelect = oci_parse($conexao, $sqlSelect);
            oci_execute($stmtSelect);
    
            // Recupera o número do pedido
            $row = oci_fetch_assoc($stmtSelect);
            if (!$row) {
                return $response->withJson(['error' => 'Não foi possível gerar o número do pedido'], 500);
            }
    
            $numpedido = (int)$row['NUMPED'];
    
            // Atualiza o próximo número de pedido
            $sqlUpdate = "UPDATE PCUSUARI SET PROXNUMPED = NVL(PROXNUMPED, 1) + 1 WHERE CODUSUR = 1";
            $stmtUpdate = oci_parse($conexao, $sqlUpdate);
            $resultUpdate = oci_execute($stmtUpdate);
    
            if (!$resultUpdate) {
                $e = oci_error($stmtUpdate);
                return $response->withJson(['error' => 'Erro ao atualizar o próximo número do pedido: ' . $e['message']], 500);
            }
    
            // Fechar a conexão
            oci_free_statement($stmtSelect);
            oci_free_statement($stmtUpdate);
            oci_close($conexao);
    
            // Retornar o número do pedido gerado
            return $response->withJson(['NUMPED' => $numpedido]);
        } catch (Exception $e) {
            // Fechar a conexão em caso de erro
            if ($conexao) {
                oci_close($conexao);
            }
            return $response->withJson(['error' => $e->getMessage()], 500);
        }
    });
    
    $this->post('/transferencia/gravar/ped', function (Request $request, Response $response) {

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
        $NUMPED = $params['NUMPED'] ?? null;
        $codcliente = $params['codcliente'] ?? null;
        $codfilial = $params['codfilial'] ?? null;
        $codsupervisor = $params['codsupervisor'] ?? null;


        // Valida se os parâmetros necessários foram enviados
        if (!$NUMPED || !$codcliente || !$codfilial || !$codsupervisor) {
            return $response->withJson(['error' => 'Parâmetros inválidos '], 400);
        }


        //////////////////////////////////////////////////////////////////////////////////////

        // BUSCAR DADOS DA NOTA
        $DADOSBASE = " SELECT SUM(P.PTABELA * P.QT) vltabela, SUM(P.VLCUSTOREAL * P.QT) vlcustoreal, 
                SUM(P.VLCUSTOFIN * P.QT) vlcustofin,
                sum(p.vlcustorep * p.QT) vlcustorep, sum(p.vlcustocont * p.QT) vlcustocont, TO_CHAR(SYSDATE, 'HH24')  HORA, 
                TO_CHAR(SYSDATE, 'MI') MINUTO
                FROM pcorcavendai P
                WHERE P.NUMORCA = :NUMPED
            ";

        $stmtSaldo = oci_parse($conexao, $DADOSBASE);

        oci_bind_by_name($stmtSaldo, ":NUMPED", $NUMPED);

        oci_execute($stmtSaldo);
        $rowSaldo = oci_fetch_assoc($stmtSaldo);
        if (!$rowSaldo) {
            // Se não encontrar o saldo, retorna um erro
            return $response->withJson(['error' => 'Saldo não encontrado'], 500);
        }
        // Atribui o valor do saldo obtido para a variável
        $vltabela = $rowSaldo['VLTABELA'] ?? null;
        $vlcustoreal = $rowSaldo['VLCUSTOREAL'] ?? null;
        $vlcustofin = $rowSaldo['VLCUSTOFIN'] ?? null;
        $vlcustorep = $rowSaldo['VLCUSTOREP'] ?? null;
        $vlcustocont = $rowSaldo['VLCUSTOCONT'] ?? null;
        $HORA = $rowSaldo['HORA'] ?? null;
        $MINUTO = $rowSaldo['MINUTO'] ?? null;
        

        if (empty($vltabela) || empty($vlcustoreal) || empty($vlcustofin) || empty($vlcustorep) || empty($vlcustocont) || empty($HORA) || empty($MINUTO)) {
            return $response->withJson(['error' => 'Parâmetros inválidos nos valores'], 400);
        }
        

        /////////////////////////////////////////////////////////////////////////

        // BUSCAR DADOS DO CLIENTE
        $DADOSCLIENTE = "SELECT  
                    c.CLIENTE AS nomecliente, c.CGCENT AS cnpj, 
                    c.ENDERCOB AS endereco, c.BAIRROCOB AS bairro,
                    c.ESTCOB AS uf, c.MUNICCOB AS cidade,
                    c.TELCOB AS telefone, c.IEENT AS ie
                FROM PCCLIENT c
                WHERE c.CODCLI = :codcliente
            ";

        $stmtCLIENTE = oci_parse($conexao, $DADOSCLIENTE);

        oci_bind_by_name($stmtCLIENTE, ":codcliente", $codcliente);

        oci_execute($stmtCLIENTE);
        $rowCLIENTE = oci_fetch_assoc($stmtCLIENTE);
        if (!$rowCLIENTE) {
            // Se não encontrar o saldo, retorna um erro
            return $response->withJson(['error' => 'Saldo não encontrado'], 500);
        }
        // Atribui o valor do saldo obtido para a variável

        $nomecliente = $rowCLIENTE['NOMECLIENTE'] ?? null;
        $cnpj = $rowCLIENTE['CNPJ'] ?? null;
        $endereco = $rowCLIENTE['ENDERECO'] ?? null;
        $bairro = $rowCLIENTE['BAIRRO'] ?? null;
        $uf = $rowCLIENTE['UF'] ?? null;
        $cidade = $rowCLIENTE['CIDADE'] ?? null;
        $telefone = $rowCLIENTE['TELEFONE'] ?? null;
        $ie = $rowCLIENTE['IE'] ?? null;
        
        // Validação para garantir que os valores não sejam nulos ou vazios
        if (
            empty($nomecliente) ||
            empty($cnpj) ||
            empty($endereco) ||
            empty($bairro) ||
            empty($uf) ||
            empty($cidade) ||
            empty($telefone) ||
            empty($ie)
        ) {
            return $response->withJson(['error' => 'Parâmetros inválidos no cliente'], 400);
        }
        

        /////////////////////////////////////////////////////////////////////////

        // desdobrar vale e baixar notinha
        $gravarPedido  = "INSERT INTO pcorcavendac
                                    (numorca, DATA, codusur, codcli, numitens, vlatend,
                                    codpraca, posicao, numcar, codsupervisor, codfilial,
                                    codfilialnf, vltotal, vltabela, vlcustoreal, vlcustofin,
                                    totpeso, totvolume, codemitente, operacao, tipovenda, obs,
                                    codcob, hora, minuto, codplpag, numpedcli, percvenda,
                                    perdesc, vldesconto, vlfrete, vloutrasdesp, obs1, obs2,
                                    condvenda, dtentrega, numpedrca, fretedespacho,
                                    freteredespacho, codfornecfrete, tipocarga, prazo1, prazo2,
                                    prazo3, prazo4, prazo5, prazo6, prazo7, prazo8, prazo9,
                                    prazo10, prazo11, prazo12, prazomedio, obsentrega1,
                                    obsentrega2, obsentrega3, tipoembalagem, codepto, cliente,
                                    cnpj, endereco, bairro, uf, telefone, ie, codatv1,
                                    cidade, dtvalidade, codclirecebedor, numregiao, numnota,
                                    perdescfin, origemped, especiemanif, coddistrib,
                                    numnotamanif, seriemanif, numpedentfut, numcarmanif,
                                    codcontrato, datapedcli, numpedbnf, broker,
                                    codestabelecimento, numtabela, motivoposicao,
                                    codmotbloqueio, serieecf, numcupom,
                                    conciliaimportacao, restricaotransp, geracp, vendaassistida,
                                    usaintegracaowms, codfornecredespacho, campanha,
                                    vlcustocont, vlcustorep, 
                                    tipooper, codmotivo, LOG,
                                    usacfopvendanatv10, tipoprioridadeentrega, codusur2,
                                    codusur3, codusur4, codclinf, valordescfin, CodVisita, vendatriangular, vlentrada,
                                    ufdesembaraco, localdesembaraco, tipodocumento, placaveiculo, CODCONTATO,codendent, agrupamento,
                                    PAGCHEQUEMORADIA, VENDALOCESTRANG, 
                                    codclitv8, contaordem,OPERVENDAEXPINDIRETA, STATUSTRANSACAODIG, DTITERACAOTRANSACAODIG,
                                    JSONTPD_PAGAMENTO, CODBNF, CUSTOBONIFICACAO, CODFORNECBONIFIC
                        )
                        VALUES (:NUMPED, TRUNC(SYSDATE), 1.000000, :codcliente, 0.000000, :vltabela,
                                    1.000000, 'L', 0.000000, :codsupervisor, :codfilial,
                                    Null, :vltabela, :vltabela, :vlcustoreal, :vlcustofin,
                                    0.000, 0.000, 125.000000, 'N', 'VV', Null,
                                    'D', :HORA, :MINUTO, 1.000000, Null, 100.000000,
                                    0.000000, 0.000000, 0.000000, 0.000000, 'FEITO PELO SITE', Null,
                                    10.000000, TRUNC(SYSDATE), 0.000000, 'N',
                                    Null, 0.000000, Null, 0.000000, Null,
                                    Null, Null, Null, Null, Null, Null, Null,
                                    Null, Null, Null, 0.000000, Null,
                                    Null, Null, 'U', 0.000000, :nomecliente,
                                    :cnpj, :endereco, :bairro, :uf, :telefone, :ie, 1.000000,
                                    :cidade, TRUNC(SYSDATE), 0.000000, 1.000000, 0.000000,
                                    0.000000, 'R', Null, '1',
                                    0.000000, Null, 0.000000, 0.000000,
                                    0.000000, TRUNC(SYSDATE), 0.000000, Null,
                                    Null, Null, Null,
                                    0.000000, Null, 0.000000,
                                    'N', Null, 'S', Null,
                                    'N', 0.000000, Null,
                                    :vlcustocont, :vlcustorep, 
                                    Null, Null, Null,
                                    'N', Null, 0.000000,
                                    0.000000, 0.000000, Null, Null, Null, Null, 0.000000,
                                    Null, Null, 'A', Null, Null, 0.0000, 'N',
                                    'N', 'N',
                                    0.000000, 'N', 'N', Null, Null,
                                    Null, 0.000000, 'R', 0.000000
                        )
                ";
        //log desdobramento e baixa

        // Preparando e executando baixa de nota
        $baixa = oci_parse($conexao, $gravarPedido);

        oci_bind_by_name($baixa, ":NUMPED", $NUMPED);
        oci_bind_by_name($baixa, ":codcliente", $codcliente);
        oci_bind_by_name($baixa, ":vltabela", $vltabela);
        oci_bind_by_name($baixa, ":codsupervisor", $codsupervisor);
        oci_bind_by_name($baixa, ":codfilial", $codfilial);
        oci_bind_by_name($baixa, ":vlcustoreal", $vlcustoreal);
        oci_bind_by_name($baixa, ":vlcustofin", $vlcustofin);
        oci_bind_by_name($baixa, ":HORA", $HORA);
        oci_bind_by_name($baixa, ":MINUTO", $MINUTO);
        oci_bind_by_name($baixa, ":nomecliente", $nomecliente);
        oci_bind_by_name($baixa, ":cnpj", $cnpj);
        oci_bind_by_name($baixa, ":endereco", $endereco);
        oci_bind_by_name($baixa, ":bairro", $bairro);
        oci_bind_by_name($baixa, ":uf", $uf);
        oci_bind_by_name($baixa, ":telefone", $telefone);
        oci_bind_by_name($baixa, ":ie", $ie);
        oci_bind_by_name($baixa, ":cidade", $cidade);

        oci_bind_by_name($baixa, ":vlcustocont", $vlcustocont);
        oci_bind_by_name($baixa, ":vlcustorep", $vlcustorep);

        $resultbaixa = oci_execute($baixa);


        // Verifica se todos os updates foram bem-sucedidos
        if ($resultbaixa) {
            return $response->withJson(['message' => 'Dados inseridos com sucesso'], 200);
        } else {
            $e = oci_error($baixa);
            return $response->withJson(['error' => $e['message']], 500);
        }

        oci_free_statement($stmtSaldo);
        oci_free_statement($stmtCLIENTE);
        oci_free_statement($baixa);
        oci_close($conexao);
    });

    // gravar pedido item
    $this->post('/transferencia/gravar', function (Request $request, Response $response) {

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
        $CODPROD = $params['CODPROD'] ?? null;
        $codfilial = $params['codfilial'] ?? null;
        $numpedido = $params['numpedido'] ?? null;
        $codcliente = $params['codcliente'] ?? null;
        $qt = $params['qt'] ?? null;
        $sequencia = $params['sequencia'] ?? null;
        $codauxiliar = $params['codauxiliar'] ?? null;



        // Valida se os parâmetros necessários foram enviados
        if (!$CODPROD || !$codfilial || !$numpedido || !$codcliente || !$qt || !$sequencia) {
            return $response->withJson(['error' => 'Parâmetros inválidos ou ausentes'], 400);
        }

        //////////////////////////////////////////////////////////////////////////////////////

        // BUSCAR DADOS DE PREÇO
        $DADOSBASE = "WITH MovimentosOrdenados AS (
                        SELECT M.CODPROD,
                        ROUND(M.CUSTOULTENT, 2 ) PVENDA,
                        ROUND(M.CUSTOULTENT, 2 ) PTABELA,
                        ROUND(M.CUSTOFIN, 2)     VLCUSTOFIN,
                        ROUND(M.CUSTOREAL, 2)    VLCUSTOREAL,
                        ROUND(M.CUSTOFIN, 2)     CUSTOFINEST,
                        ROUND(M.CUSTOCONT, 2)    VLCUSTOCONT,
                        ROUND(M.CUSTOREP, 2)     VLCUSTOREP,
                        ROUND(M.PTABELA, 2)      PTABELAFABRICAZFM,
                        ROW_NUMBER() OVER (PARTITION BY M.CODPROD ORDER BY M.DTMOV DESC) AS RN
                        FROM PCMOV M
                        
                        WHERE M.CODPROD = :CODPROD 
                        AND M.CODFILIAL = :codfilial
                        AND M.CODOPER LIKE '%E%'
                    ),

                    PRECO AS (
                    SELECT
                    B.CODPROD,
                    B.PVENDA AS PVENDABASE
                    FROM PCEMBALAGEM B
                    WHERE B.CODPROD = :CODPROD
                    AND B.CODFILIAL = :codfilial
                    ),

                    TRIBUTACAO AS (

                    SELECT  B.CODPROD, T.CODST, T.SITTRIBUTTRANSF AS CODFISCAL, T.CODFISCALTRANSF AS SITTRIBUT
                    FROM PCTABTRIB B, PCTRIBUT T
                    WHERE B.CODST = T.CODST(+)
                    AND B.UFDESTINO = 'PA'
                    AND B.CODPROD = :CODPROD
                    AND B.CODFILIALNF = :codfilial

                    )

                    SELECT 
                            NVL(C.pvenda, 0) AS pvenda,
                            NVL(C.ptabela, 0) AS ptabela,
                            NVL(P.pvendabase, 0) AS pvendabase,
                            NVL(C.vlcustofin, 0) AS vlcustofin,
                            NVL(C.vlcustoreal, 0) AS vlcustoreal,
                            NVL(T.CODST, 0) AS CODST,
                            NVL(C.custofinest, 0) AS custofinest,
                            NVL(C.vlcustocont, 0) AS vlcustocont,
                            NVL(C.VLCUSTOREP, 0) AS VLCUSTOREP,
                            NVL(T.CODFISCAL, 0) AS SITTRIBUT,
                            NVL(T.SITTRIBUT, 0) AS CODFISCAL,
                            NVL(C.PTABELAFABRICAZFM, 0) AS PTABELAFABRICAZFM
                    FROM MovimentosOrdenados C, PRECO P, TRIBUTACAO T
                    WHERE C.CODPROD = P.CODPROD
                    AND C.CODPROD = T.CODPROD
                    AND C.RN = 1
        ";


        $stmtSaldo = oci_parse($conexao, $DADOSBASE);


        oci_bind_by_name($stmtSaldo, ":CODPROD", $CODPROD);
        oci_bind_by_name($stmtSaldo, ":codfilial", $codfilial);

        oci_execute($stmtSaldo);
        $rowSaldo = oci_fetch_assoc($stmtSaldo);
        if (!$rowSaldo) {
            // Se não encontrar o saldo, retorna um erro
            return $response->withJson(['error' => 'Saldo não encontrado'], 500);
        }
        // Atribui o valor do saldo obtido para a variável
        $pvenda = (float) $rowSaldo['PVENDA'];
        $ptabela = (float) $rowSaldo['PTABELA'];
        $pvendabase = (float) $rowSaldo['PVENDABASE'];
        $vlcustofin = (float) $rowSaldo['VLCUSTOFIN'];
        $vlcustoreal = (float) $rowSaldo['VLCUSTOREAL'];
        $codst = $rowSaldo['CODST'];
        $custofinest = (float) $rowSaldo['CUSTOFINEST'];
        $vlcustocont = (float) $rowSaldo['VLCUSTOCONT'];
        $vlcustorep = (float) $rowSaldo['VLCUSTOREP'];
        $CODFISCAL = $rowSaldo['CODFISCAL'];
        $SITTRIBUT = $rowSaldo['SITTRIBUT'];
        $PTABELAFABRICAZFM = $rowSaldo['PTABELAFABRICAZFM'];

        /////////////////////////////////////////////////////////////////////////

        // desdobrar vale e baixar notinha
        $gravarPedido  = "INSERT INTO pcorcavendai
                (numorca,
                DATA,
                codcli,
                codprod,
                codusur,
                qt,
                pvenda,
                ptabela,
                pvendabase,
                numcar,
                posicao,
                st,
                vlcustofin,
                vlcustoreal,
                percom,
                perdesc,
                qtfalta,
                numseq,
                codst,
                iva,
                pauta,
                aliqicms1,
                aliqicms2,
                percbasered,
                percbaseredst,
                percbaseredstfonte,
                perfretecmv,
                custofinest,
                txvenda,
                codicmtab,
                perdesccusto,
                perciss,
                vliss,
                percipi,
                vlipi,
                pbaserca,
                codauxiliar,
                numverbarebcmv,
                vlverbacmv,
                poriginal,
                vldescicmisencao,
                perdescisentoicms,
                pvenda1,
                vldesccustocmv,
                vldescsuframa,
                vldescreducaopis,
                vldescreducaocofins,   
                stclientegnre,
                brinde,
                baseicst,
                letracomiss,
                eancodprod,
                vlverbacmvcli,
                codfilialretira,
                vlcustocont,
                vlcustorep,
                vldescfin,
                complemento,
                qtcx,
                qtpecas,
                tipoentrega,
                geragnre_cnpjcliente,
                percdifaliquotas,
                basedifaliquotas,
                vldifaliquotas,
                perdescpolitica,
                pvendaanterior,
                proddescricaocontrato,
                truncaritem,
                qtunitemb,
                percom2,
                percom3,
                percom4,
                PoliticaPrioritaria,
                coddesconto,
                vldescpissuframa,
                vlredpvendasimplesna,
                vlredcmvsimplesnac,
                PERDESCPAUTA,
                ORIGEMST,
                QTDIASENTREGAITEM,
                codmoedaestrageira,
                vlrmoedaestrageira,
                CODPRODCESTA,
                NUMSEQCESTA,
                ALIQFCP,
                ALIQINTERNADEST,
                VLFCPPART,
                VLICMSPARTDEST,
                VLICMSPART,
                PERCPROVPART,
                VLICMSDIFALIQPART,
                PERCBASEREDPART,
                VLICMSPARTREM,
                ALIQINTERORIGPART,
                VLBASEPARTDEST,
                VLIPIPTABELA,
                VLIPIPBASERCA,
                STPTABELA,
                STPBASERCA,
                VLICMSPARTPTABELA,
                VLICMSPARTPBASERCA,
                NUMITEMPED,
                CODDESCONTOSIMULADOR,
                CODFIGVENDATRIANGULAR,
                CODFISCAL,
                SITTRIBUT,
                PTABELAFABRICAZFM,
                UNIDADE,
                VLBASEFCPST,
                VLFECP,
                ALIQICMSFECP,
                UTILIZOUMOTORCALCULO,
                CODCOMBO,
                VLIPISUSPENSO,
                VLIISUSPENSO)
                VALUES
                    (:numpedido,
                    TRUNC(SYSDATE),
                    :codcliente,
                    :CODPROD,
                    1.000000,
                    :qt,
                    :pvenda,
                    :ptabela,
                    :pvendabase,
                    0.000000,
                    'L',
                    0.000000,
                    :vlcustofin,
                    :vlcustoreal,
                    0.000000,
                    0.000000,
                    0.000000,
                    :sequencia,
                    :codst,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    :custofinest,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    :pvenda,
                    :codauxiliar,
                    0.000000,
                    0.000000,
                    :pvenda,
                    0.000000,
                    0.000000,
                    :pvenda,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,    
                    0.000000,
                    'N',
                    0.000000,
                    Null,
                    0.000000,
                    0.000000,
                    '1',
                    :vlcustocont,
                    :vlcustorep,
                    0.000000,
                    Null,
                    0.000000,
                    0.000000,
                    'RP',
                    Null,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    Null,
                    'N',
                    1.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    'N',
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    Null,
                    Null,
                    Null,
                    Null,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    0.000000,
                    :CODFISCAL,
                    :SITTRIBUT,
                    :PTABELAFABRICAZFM,
                    'UN',
                    0.000000,
                    0.000000,
                    0.000000,
                    'N',
                    0.000000,
                    0.000000,
                    0.000000) 
        ";
        //log desdobramento e baixa

        // Preparando e executando baixa de nota
        $baixa = oci_parse($conexao, $gravarPedido);

        oci_bind_by_name($baixa, ":CODPROD", $CODPROD);
        oci_bind_by_name($baixa, ":numpedido", $numpedido);
        oci_bind_by_name($baixa, ":codcliente", $codcliente);
        oci_bind_by_name($baixa, ":qt", $qt);
        oci_bind_by_name($baixa, ":sequencia", $sequencia);
        oci_bind_by_name($baixa, ":pvenda", $pvenda);
        oci_bind_by_name($baixa, ":codauxiliar", $codauxiliar);
        oci_bind_by_name($baixa, ":ptabela", $ptabela);
        oci_bind_by_name($baixa, ":pvendabase", $pvendabase);
        oci_bind_by_name($baixa, ":vlcustofin", $vlcustofin);
        oci_bind_by_name($baixa, ":vlcustoreal", $vlcustoreal);
        oci_bind_by_name($baixa, ":codst", $codst);
        oci_bind_by_name($baixa, ":custofinest", $custofinest);
        oci_bind_by_name($baixa, ":vlcustocont", $vlcustocont);
        oci_bind_by_name($baixa, ":vlcustorep", $vlcustorep);
        oci_bind_by_name($baixa, ":CODFISCAL", $CODFISCAL);
        oci_bind_by_name($baixa, ":SITTRIBUT", $SITTRIBUT);
        oci_bind_by_name($baixa, ":PTABELAFABRICAZFM", $PTABELAFABRICAZFM);

        $resultbaixa = oci_execute($baixa);


        // Verifica se todos os updates foram bem-sucedidos
        if ($resultbaixa) {
            return $response->withJson(['message' => 'Dados inseridos com sucesso'], 200);
        } else {
            $e = oci_error($baixa);
            return $response->withJson(['error' => $e['message']], 500);
        }

        oci_free_statement($stmtSaldo);
        oci_free_statement($baixa);
        oci_close($conexao);
    });

    $this->get('/transferencia/pesquisar/produto/{codbarra}/{filial_saida}/{filial_entrada}', function (Request $request, Response $response) {

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

        $filial_saida = $request->getAttribute('filial_saida');
        $filial_entrada = $request->getAttribute('filial_entrada');
        $codbarra = $request->getAttribute('codbarra');

        $sql = "WITH SAIDA AS (

                    SELECT E.CODFILIAL AS FILIAL_S, E.CODPROD,  (E.QTESTGER - E.QTRESERV - E.QTBLOQUEADA) AS ESTOQUE_S, E.ESTMIN AS ESTMIN_S, E.ESTMAX AS ESTMAX_S,
                    E.QTGIRODIA AS GIRODIA_S,
                    P.TEMREPOS AS TEMREPOS_S,
                    CASE 
                    WHEN (E.QTGIRODIA * P.TEMREPOS) > E.ESTMIN THEN (E.QTGIRODIA * P.TEMREPOS)
                    ELSE E.ESTMIN
                    END AS ESTIDEAL_S

                    FROM PCEST E, PCPRODUT P
                    WHERE E.CODPROD = P.CODPROD
                    AND E.CODFILIAL = :filial_saida

                ),

                ENTRADA AS (

                    SELECT E.CODFILIAL AS FILIAL_E, E.CODPROD,  (E.QTESTGER - E.QTRESERV - E.QTBLOQUEADA) AS ESTOQUE_E, E.ESTMIN AS ESTMIN_E, E.ESTMAX AS ESTMAX_E,
                    E.QTGIRODIA AS GIRODIA_E,
                    P.TEMREPOS AS TEMREPOS_E,
                    CASE 
                    WHEN (E.QTGIRODIA * P.TEMREPOS) > E.ESTMIN THEN (E.QTGIRODIA * P.TEMREPOS)
                    ELSE E.ESTMIN
                    END AS ESTIDEAL_E

                    FROM PCEST E, PCPRODUT P
                    WHERE E.CODPROD = P.CODPROD
                    AND E.CODFILIAL = :filial_entrada

                ),
                PRODUTO AS (

                SELECT 
                B.CODAUXILIAR,
                B.CODPROD,
                CASE 
                    WHEN B.DESCRICAOECF IS NULL THEN P.DESCRICAO
                    ELSE B.DESCRICAOECF
                END AS PRODUTO

                FROM PCEMBALAGEM B, PCPRODUT P, PCEST E
                WHERE 1=1
                AND B.CODPROD = P.CODPROD
                AND B.CODPROD = E.CODPROD
                AND B.CODFILIAL = E.CODFILIAL
                AND E.CODFILIAL = 1
                AND  B.CODAUXILIAR  = :codbarra

                )

                SELECT P.CODPROD, P.CODAUXILIAR, P.PRODUTO,
                S.FILIAL_S, S.ESTOQUE_S, S.ESTMIN_S, S.ESTMAX_S, S.GIRODIA_S, S.TEMREPOS_S, S.ESTIDEAL_S,
                E.FILIAL_E, E.ESTOQUE_E, E.ESTMIN_E, E.ESTMAX_E, E.GIRODIA_E, E.TEMREPOS_E, E.ESTIDEAL_E
                FROM SAIDA S, ENTRADA E, PRODUTO P
                WHERE P.CODPROD = S.CODPROD 
                AND P.CODPROD = E.CODPROD


        ";


        $stmt = oci_parse($conexao, $sql);

        // Associar a data formatada ao placeholder SQL
        oci_bind_by_name($stmt, ":filial_saida", $filial_saida);
        oci_bind_by_name($stmt, ":filial_entrada", $filial_entrada);
        oci_bind_by_name($stmt, ":codbarra", $codbarra);

        // Executa a consulta
        oci_execute($stmt);

        // Coletar os resultados em um array
        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) != false) {
            
            $row['CODPROD'] = isset($row['CODPROD']) ? (int)$row['CODPROD'] : null;
            $row['CODAUXILIAR'] = isset($row['CODAUXILIAR']) ? (int)$row['CODAUXILIAR'] : null;
            $row['FILIAL_S'] = isset($row['FILIAL_S']) ? (int)$row['FILIAL_S'] : null;
            $row['ESTOQUE_S'] = isset($row['ESTOQUE_S']) ? (int)$row['ESTOQUE_S'] : null;
            $row['ESTMIN_S'] = isset($row['ESTMIN_S']) ? (int)$row['ESTMIN_S'] : null;
            $row['ESTMAX_S'] = isset($row['ESTMAX_S']) ? (int)$row['ESTMAX_S'] : null;
            $row['GIRODIA_S'] = isset($row['GIRODIA_S']) ? (int)$row['GIRODIA_S'] : null;
            $row['TEMREPOS_S'] = isset($row['TEMREPOS_S']) ? (int)$row['TEMREPOS_S'] : null;
            $row['ESTIDEAL_S'] = isset($row['ESTIDEAL_S']) ? (int)$row['ESTIDEAL_S'] : null;
            $row['FILIAL_E'] = isset($row['FILIAL_E']) ? (int)$row['FILIAL_E'] : null;
            $row['ESTOQUE_E'] = isset($row['ESTOQUE_E']) ? (int)$row['ESTOQUE_E'] : null;
            $row['ESTMIN_E'] = isset($row['ESTMIN_E']) ? (int)$row['ESTMIN_E'] : null;
            $row['ESTMAX_E'] = isset($row['ESTMAX_E']) ? (int)$row['ESTMAX_E'] : null;
            $row['GIRODIA_E'] = isset($row['GIRODIA_E']) ? (int)$row['GIRODIA_E'] : null;
            $row['TEMREPOS_E'] = isset($row['TEMREPOS_E']) ? (int)$row['TEMREPOS_E'] : null;
            $row['ESTIDEAL_E'] = isset($row['ESTIDEAL_E']) ? (int)$row['ESTIDEAL_E'] : null;
         

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
});
