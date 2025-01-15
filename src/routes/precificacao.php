<?php

use Slim\Http\Request;
use Slim\Http\Response;

use Symfony\Component\Console\Descriptor\Descriptor;

// Rota para listar os dados de PCFILIAL
$app->group('/api/v1', function () {

    $this->get('/precificacao/lista', function (Request $request, Response $response) {

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

        // Obtendo a data da requisição (GET parameter)
        $params = $request->getQueryParams();
        $data_entrada = isset($params['data_entrada']) ? date('d-M-Y', strtotime($params['data_entrada'])) : '02-SEP-2024';


        $sql = "WITH CTE AS (
            SELECT
                E.CODFILIAL AS CODIGO_FILIAL,
                P.CODPROD AS COD_PRODUTO,
                P.DESCRICAO AS PRODUTO,
           
                D.DESCRICAO AS DEPARTAMENTO,
                P.CODAUXILIAR,
                NC.CODNCM, 
                PDO.NUMPED,
                PDO.CODCOMPRADOR,
                B.MARGEM AS MARGEM_IDEAL,
                P.CLASSEVENDA,
                E.CUSTOULTENT AS CUSTO,
                ROUND(B.PVENDA, 2) AS VL_VENDA,
                ROW_NUMBER() OVER (
                    PARTITION BY E.CODFILIAL,
                    P.CODPROD
                    ORDER BY
                        PDO.DTFATUR DESC
                ) AS RN
            
            FROM
                PCEST E 
                JOIN PCPRODUT P ON E.CODPROD = P.CODPROD
                JOIN PCFORNEC F ON P.CODFORNEC = F.CODFORNEC
                JOIN PCITEM I ON P.CODPROD = I.CODPROD
                JOIN PCPEDIDO PDO ON I.NUMPED = PDO.NUMPED
                AND PDO.CODFILIAL = E.CODFILIAL
                JOIN PCEMBALAGEM B ON P.CODPROD = B.CODPROD
                AND E.CODFILIAL = B.CODFILIAL
                JOIN PCDEPTO D ON P.CODEPTO = D.CODEPTO 
                JOIN PCNCM NC ON P.CODNCMEX = NC.CODNCMEX
                JOIN PCMARCA MAR ON P.CODMARCA = MAR.CODMARCA
                JOIN PCSECAO CS ON P.CODSEC = CS.CODSEC  
            WHERE
                PDO.DTFATUR > TO_DATE('01-JAN-2024', 'DD-MM-YYYY')
                AND B.EMBALAGEM IN ('UN', 'KG')
            ),
                      ULTIMA_ENTRADA AS (
               
                SELECT
                    M.CODOPER,
                    M.NUMNOTA,
                    M.NUMPED,
                    M.NUMTRANSENT,
                    M.CODFORNEC,
                    M.PERCREDICMS AS CREDITO_CUSTO_ICMS,
                    M.PERPIS AS PIS,
                    M.PERCOFINS AS COFINS,
                    (M.PERCREDICMS + M.PERPIS + M.PERCOFINS) AS CREDITO_ENTRADA,
                    M.VALORULTENT AS VALOR_ULTIMA_ENTRADA,
                    M.CODFILIAL AS CODIGO_FILIAL,
                    M.CODPROD AS COD_PRODUTO,
                    M.DTMOV AS DATA_ULTIMA_ENTRADA,
                    M.QT AS QT_TRANSFERIDA,
                    ROW_NUMBER() OVER (
                        PARTITION BY M.CODFILIAL,
                        M.CODPROD
                        ORDER BY
                            M.DTMOV DESC
                    ) AS RN
                FROM
                    PCMOV M, PCPEDIDO P
                WHERE M.NUMPED = P.NUMPED
                    AND M.DTMOV > TO_DATE('01-JAN-2023', 'DD-MM-YYYY')
                    AND M.CODOPER IN ('E','EB')
                    AND P.TIPOBONIFIC IN ('N', 'B')
           
     
            ),

             PVT AS (
              SELECT 
						    P.CODPROD, 
						    M.CODFILIAL,
						    ROUND(
						        M.VALORULTENT + 
						        (M.VALORULTENT * NVL(SM.MARKUP, 40) / 100), 
						        2
						    ) AS PRECOMINSUG,
						    CONCAT(NVL(SM.MARKUP, 40), '%') AS MARKUP,
						    ROW_NUMBER() OVER (PARTITION BY M.CODFILIAL, P.CODPROD ORDER BY M.DTMOV DESC) AS RN
						FROM 
						    PCPRODUT P
						    JOIN PCSECAO S ON P.CODSEC = S.CODSEC -- Relaciona com a seção
						    JOIN PCMOV M ON P.CODPROD = M.CODPROD -- Relaciona com o movimento
						    LEFT JOIN SITEMARKUP SM 
						        ON P.CODSEC = SM.CODSEC 
						        AND P.CLASSEVENDA = SM.CURVA -- Relaciona markup com classe de venda e seção
						WHERE 
						    M.CODOPER = 'E' -- Somente entradas
						    AND M.DTMOV > TO_DATE('01-JAN-2023', 'DD-MM-YYYY')
                         
            ),
             IMPOSTO AS (
             
				 SELECT
				        CODPROD,
				        CODFILIAL,
				        PERCIMP,
				        ROW_NUMBER() OVER (
				            PARTITION BY CODFILIAL,
				            CODPROD
				            ORDER BY
				                CODPROD ASC
				        ) AS RN
				    FROM
				        (
				            SELECT
				                NVL(
				                    (
				                        CASE
				                            WHEN PCEMBALAGEM.DTOFERTAINI <= TRUNC(SYSDATE)
				                            AND PCEMBALAGEM.DTOFERTAFIM >= TRUNC(SYSDATE) THEN PCEMBALAGEM.POFERTA
				                            ELSE PCEMBALAGEM.PVENDA
				                        END
				                    ),
				                    0
				                ) PVENDA,
				                NVL(
				                    (
				                        CASE
				                            WHEN PCCONSUM.SUGVENDA = 1 THEN PCEST.CUSTOREAL
				                            WHEN PCCONSUM.SUGVENDA = 2 THEN PCEST.CUSTOFIN
				                            ELSE PCEST.CUSTOULTENT
				                        END
				                    ),
				                    0
				                ) PCUSTO,
				                PCEMBALAGEM.MARGEM,
				                PCTRIBUT.CODICMTAB,
				                PCTRIBUT.PERDESCCUSTO,
				                PCEMBALAGEM.CODAUXILIAR,
				                PCPRODUT.DESCRICAO,
				                PCEMBALAGEM.EMBALAGEM,
				                PCEMBALAGEM.UNIDADE,
				                PCPRODUT.PRECOFIXO,
				                PCPRODUT.PCOMREP1,
				                PCPRODUT.PESOBRUTO,
				                PCEST.CUSTOFIN,
				                PCEST.CUSTOREAL,
				                PCEST.CODFILIAL,
				                PCEST.CODPROD,
				                PCEST.DTULTENT,
				                PCEST.QTULTENT,
				                PCEST.CUSTOULTENT, 
				                (
				                    NVL(PCEST.QTESTGER, 0) - NVL(PCEST.QTBLOQUEADA, 0) - NVL(PCEST.QTRESERV, 0)
				                ) QTESTDISP,
				                PCREGIAO.VLFRETEKG,
				                (
				                    NVL(PCCONSUM.TXVENDA, 0) + NVL(PCPRODUT.PCOMREP1, 0) + NVL(PCTRIBUT.CODICMTAB, 0)
				                ) / 100 PERCIMP,
				                NVL(PCTRIBUT.PERDESCCUSTO, 0) / 100 PERDESCUSTO
				            FROM
				                PCTABTRIB,
				                PCPRODUT,
				                PCEMBALAGEM,
				                PCEST,
				                PCTRIBUT,
				                PCCONSUM,
				                PCREGIAO
				            WHERE
				                PCTABTRIB.CODPROD = PCPRODUT.CODPROD
				                AND PCPRODUT.CODPROD = PCEST.CODPROD
				                AND PCPRODUT.CODPROD = PCEMBALAGEM.CODPROD
				                AND PCTABTRIB.CODST = PCTRIBUT.CODST
				                AND PCREGIAO.NUMREGIAO = 1.000000
				                AND PCTABTRIB.UFDESTINO = 'PA'
				                AND PCTABTRIB.CODFILIALNF = PCEMBALAGEM.CODFILIAL
				                AND PCEMBALAGEM.CODFILIAL = PCEST.CODFILIAL
				                
				        )
				
				             )
             
             
            SELECT 
                    A.CODIGO_FILIAL,
                     C.NUMNOTA,
                     A.DEPARTAMENTO,
                     F.FORNECEDOR,
                    A.NUMPED,
                    A.CODCOMPRADOR,
                    COM.NOME,
                    A.COD_PRODUTO,
                    A.PRODUTO,
                    A.CODAUXILIAR,
                    A.CLASSEVENDA,
                    C.DATA_ULTIMA_ENTRADA,
                    A.CODNCM, 
                    A.MARGEM_IDEAL,
                    ROUND(C.VALOR_ULTIMA_ENTRADA, 2 ) VALOR_ULTIMA_ENTRADA,
                    ROUND(A.CUSTO, 2) CUSTO,
                    ROUND((((A.VL_VENDA - C.VALOR_ULTIMA_ENTRADA) / NULLIF(A.VL_VENDA, 0)) * 100), 2) AS MARGEM_ATUAL,
                                        CASE 
					    WHEN A.VL_VENDA <> 0 THEN ROUND(((A.VL_VENDA - ((I.PERCIMP * A.VL_VENDA) + A.CUSTO)) / A.VL_VENDA) * 100, 2) 
					    ELSE NULL 
					END AS MARGEM_ATUAL_WTH,
                    A.VL_VENDA,
                    B.PRECOMINSUG,
                    B.MARKUP,
                    I.PERCIMP AS IMPOSTO,
                    ROUND(((B.PRECOMINSUG-((I.PERCIMP*B.PRECOMINSUG)+A.CUSTO))/B.PRECOMINSUG)*100, 2) AS MARGEM_FUTURA_WTH
                    
                    
            FROM CTE A, PVT B, ULTIMA_ENTRADA C, PCEMPR COM, PCFORNEC F, IMPOSTO I
            WHERE A.COD_PRODUTO = B.CODPROD
            AND A.CODIGO_FILIAL = B.CODFILIAL
            AND A.COD_PRODUTO = I.CODPROD
            AND A.CODIGO_FILIAL = I.CODFILIAL
            AND A.CODCOMPRADOR = COM.MATRICULA
            AND A.COD_PRODUTO = C.COD_PRODUTO
            AND C.CODFORNEC = F.CODFORNEC
            AND A.CODIGO_FILIAL = C.CODIGO_FILIAL
            AND A.RN = 1
            AND B.RN = 1
            AND C.RN = 1
            AND I.RN = 1
            AND B.PRECOMINSUG > A.VL_VENDA
            AND (A.VL_VENDA * 1.01) <= B.PRECOMINSUG
            AND C.DATA_ULTIMA_ENTRADA >= TO_DATE(:data_entrada, 'DD-MON-YYYY')
            ORDER BY A.DEPARTAMENTO,  A.COD_PRODUTO      
            ";


        $stmt = oci_parse($conexao, $sql);

        // Vincular o parâmetro :data_entrada ao valor obtido
        oci_bind_by_name($stmt, ":data_entrada", $data_entrada);

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

    $this->get('/precificacao/lista/negativo', function (Request $request, Response $response) {
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
            $consulta = "WITH CTE AS (
                        SELECT
                            E.CODFILIAL AS CODIGO_FILIAL,
                            P.CODPROD AS COD_PRODUTO,
                            P.DESCRICAO AS PRODUTO,
                    
                            D.DESCRICAO AS DEPARTAMENTO,
                            P.CODAUXILIAR,
                            NC.CODNCM, 
                            PDO.NUMPED,
                            PDO.CODCOMPRADOR,
                            B.MARGEM AS MARGEM_IDEAL,
                            P.CLASSEVENDA,
                            E.CUSTOULTENT AS CUSTO,
                            ROUND(B.PVENDA, 2) AS VL_VENDA,
                            ROW_NUMBER() OVER (
                                PARTITION BY E.CODFILIAL,
                                P.CODPROD
                                ORDER BY
                                    PDO.DTFATUR DESC
                            ) AS RN
                        
                        FROM
                            PCEST E 
                            JOIN PCPRODUT P ON E.CODPROD = P.CODPROD
                            JOIN PCFORNEC F ON P.CODFORNEC = F.CODFORNEC
                            JOIN PCITEM I ON P.CODPROD = I.CODPROD
                            JOIN PCPEDIDO PDO ON I.NUMPED = PDO.NUMPED
                            AND PDO.CODFILIAL = E.CODFILIAL
                            JOIN PCEMBALAGEM B ON P.CODPROD = B.CODPROD
                            AND E.CODFILIAL = B.CODFILIAL
                            JOIN PCDEPTO D ON P.CODEPTO = D.CODEPTO 
                            JOIN PCNCM NC ON P.CODNCMEX = NC.CODNCMEX
                            JOIN PCMARCA MAR ON P.CODMARCA = MAR.CODMARCA
                            JOIN PCSECAO CS ON P.CODSEC = CS.CODSEC  
                        WHERE
                            PDO.DTFATUR > TO_DATE('01-JAN-2024', 'DD-MM-YYYY')
                            AND B.EMBALAGEM IN ('UN', 'KG')
                        ),
                                ULTIMA_ENTRADA AS (
                        
                            SELECT
                                M.CODOPER,
                                M.NUMNOTA,
                                M.NUMPED,
                                M.NUMTRANSENT,
                                M.CODFORNEC,
                                M.NBM AS NCM_ENTRADA,
                                M.PERCREDICMS AS CREDITO_CUSTO_ICMS,
                                M.PERPIS AS PIS,
                                M.PERCOFINS AS COFINS,
                                (M.PERCREDICMS + M.PERPIS + M.PERCOFINS) AS CREDITO_ENTRADA,
                                M.VALORULTENT AS VALOR_ULTIMA_ENTRADA,
                                M.CODFILIAL AS CODIGO_FILIAL,
                                M.CODPROD AS COD_PRODUTO,
                                M.DTMOV AS DATA_ULTIMA_ENTRADA,
                                M.QT AS QT_TRANSFERIDA,
                                ROW_NUMBER() OVER (
                                    PARTITION BY M.CODFILIAL,
                                    M.CODPROD
                                    ORDER BY
                                        M.DTMOV DESC
                                ) AS RN
                            FROM
                                PCMOV M, PCPEDIDO P
                            WHERE M.NUMPED = P.NUMPED
                                AND M.DTMOV > TO_DATE('01-JAN-2024', 'DD-MM-YYYY')
                                AND M.CODOPER IN ('E','EB')
                                AND P.TIPOBONIFIC IN ('N', 'B')
                    
                
                        ),



                        PVT AS (
                        SELECT 
						    P.CODPROD, 
						    M.CODFILIAL,
						    sm.curva,
						    sm.codsec,
						    ROUND(
						        M.VALORULTENT + 
						        (M.VALORULTENT * NVL(SM.MARKUP, 40) / 100), 
						        2
						    ) AS PRECOMINSUG, -- Preço sugerido com markup
						    CONCAT(NVL(SM.MARKUP, 40), '%') AS MARKUP, -- Markup como string
						    ROW_NUMBER() OVER (PARTITION BY M.CODFILIAL, P.CODPROD ORDER BY M.DTMOV DESC) AS RN -- Rastrear o último registro
						FROM 
						    PCPRODUT P
						    JOIN PCSECAO S ON P.CODSEC = S.CODSEC -- Relaciona com a seção
						    JOIN PCMOV M ON P.CODPROD = M.CODPROD -- Relaciona com o movimento
						    LEFT JOIN SITEMARKUP SM 
						        ON P.CODSEC = SM.CODSEC 
						        AND P.CLASSEVENDA = SM.CURVA -- Relaciona markup com classe de venda e seção
						WHERE 
						    M.CODOPER = 'E' -- Somente entradas
						    AND M.DTMOV > TO_DATE('01-JAN-2024', 'DD-MM-YYYY')
                        ),
                        IMPOSTO AS (
                        
                            SELECT
                                    CODPROD,
                                    CODFILIAL,
                                    PERCIMP,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY CODFILIAL,
                                        CODPROD
                                        ORDER BY
                                            CODPROD ASC
                                    ) AS RN
                                FROM
                                    (
                                        SELECT
                                            NVL(
                                                (
                                                    CASE
                                                        WHEN PCEMBALAGEM.DTOFERTAINI <= TRUNC(SYSDATE)
                                                        AND PCEMBALAGEM.DTOFERTAFIM >= TRUNC(SYSDATE) THEN PCEMBALAGEM.POFERTA
                                                        ELSE PCEMBALAGEM.PVENDA
                                                    END
                                                ),
                                                0
                                            ) PVENDA,
                                            NVL(
                                                (
                                                    CASE
                                                        WHEN PCCONSUM.SUGVENDA = 1 THEN PCEST.CUSTOREAL
                                                        WHEN PCCONSUM.SUGVENDA = 2 THEN PCEST.CUSTOFIN
                                                        ELSE PCEST.CUSTOULTENT
                                                    END
                                                ),
                                                0
                                            ) PCUSTO,
                                            PCEMBALAGEM.MARGEM,
                                            PCTRIBUT.CODICMTAB,
                                            PCTRIBUT.PERDESCCUSTO,
                                            PCEMBALAGEM.CODAUXILIAR,
                                            PCPRODUT.DESCRICAO,
                                            PCEMBALAGEM.EMBALAGEM,
                                            PCEMBALAGEM.UNIDADE,
                                            PCPRODUT.PRECOFIXO,
                                            PCPRODUT.PCOMREP1,
                                            PCPRODUT.PESOBRUTO,
                                            PCEST.CUSTOFIN,
                                            PCEST.CUSTOREAL,
                                            PCEST.CODFILIAL,
                                            PCEST.CODPROD,
                                            PCEST.DTULTENT,
                                            PCEST.QTULTENT,
                                            PCEST.CUSTOULTENT, 
                                            (
                                                NVL(PCEST.QTESTGER, 0) - NVL(PCEST.QTBLOQUEADA, 0) - NVL(PCEST.QTRESERV, 0)
                                            ) QTESTDISP,
                                            PCREGIAO.VLFRETEKG,
                                            (
                                                NVL(PCCONSUM.TXVENDA, 0) + NVL(PCPRODUT.PCOMREP1, 0) + NVL(PCTRIBUT.CODICMTAB, 0)
                                            ) / 100 PERCIMP,
                                            NVL(PCTRIBUT.PERDESCCUSTO, 0) / 100 PERDESCUSTO
                                        FROM
                                            PCTABTRIB,
                                            PCPRODUT,
                                            PCEMBALAGEM,
                                            PCEST,
                                            PCTRIBUT,
                                            PCCONSUM,
                                            PCREGIAO
                                        WHERE
                                            PCTABTRIB.CODPROD = PCPRODUT.CODPROD
                                            AND PCPRODUT.CODPROD = PCEST.CODPROD
                                            AND PCPRODUT.CODPROD = PCEMBALAGEM.CODPROD
                                            AND PCTABTRIB.CODST = PCTRIBUT.CODST
                                            AND PCREGIAO.NUMREGIAO = 1.000000
                                            AND PCTABTRIB.UFDESTINO = 'PA'
                                            AND PCTABTRIB.CODFILIALNF = PCEMBALAGEM.CODFILIAL
                                            AND PCEMBALAGEM.CODFILIAL = PCEST.CODFILIAL
                                            
                                    )
                            
                                        ),
                                        SAIDA AS (
                                        
                                            SELECT M.DTMOV AS DTSAIDA, M.CODPROD, M.CODFILIAL, 
                                            ROW_NUMBER() OVER (
                                                PARTITION BY M.CODFILIAL,
                                                M.CODPROD
                                                ORDER BY
                                                    M.DTMOV DESC
                                            ) AS RN
                                            FROM PCMOV M
                                            WHERE M.CODOPER = 'S'
                                            AND TRUNC(M.DTMOV) = TRUNC(SYSDATE)


                                        
                                        ),
                                        FINAL AS (
                                        
                                        
                        SELECT 
                                A.CODIGO_FILIAL,
                                C.NUMNOTA,
                                A.DEPARTAMENTO,
                                F.FORNECEDOR,
                                A.NUMPED,
                                C.NCM_ENTRADA,
                                A.CODCOMPRADOR,
                                C.CREDITO_ENTRADA,
                                COM.NOME,
                                A.COD_PRODUTO,
                                A.PRODUTO,
                                A.CODAUXILIAR,
                                A.CLASSEVENDA,
                                C.DATA_ULTIMA_ENTRADA,
                                A.CODNCM, 
                                A.MARGEM_IDEAL,
                                ROUND(C.VALOR_ULTIMA_ENTRADA, 2 ) VALOR_ULTIMA_ENTRADA,
                                ROUND(A.CUSTO, 2) CUSTO,
                                ROUND((((A.VL_VENDA - C.VALOR_ULTIMA_ENTRADA) / NULLIF(A.VL_VENDA, 0)) * 100), 2) AS MARGEM_ATUAL,
                                                    CASE 
                                    WHEN A.VL_VENDA <> 0 THEN ROUND(((A.VL_VENDA - ((I.PERCIMP * A.VL_VENDA) + A.CUSTO)) / A.VL_VENDA) * 100, 2) 
                                    ELSE NULL 
                                END AS MARGEM_ATUAL_WTH,
                                A.VL_VENDA,
                                B.PRECOMINSUG,
                                B.MARKUP,
                                I.PERCIMP AS IMPOSTO,
                                ROUND(((B.PRECOMINSUG-((I.PERCIMP*B.PRECOMINSUG)+A.CUSTO))/B.PRECOMINSUG)*100, 2) AS MARGEM_FUTURA_WTH
                                
                                
                        FROM CTE A, PVT B, ULTIMA_ENTRADA C, PCEMPR COM, PCFORNEC F, IMPOSTO I, SAIDA S
                        WHERE A.COD_PRODUTO = B.CODPROD
                        AND A.CODIGO_FILIAL = B.CODFILIAL
                        AND A.COD_PRODUTO = S.CODPROD
                        AND A.CODIGO_FILIAL = S.CODFILIAL
                        AND A.COD_PRODUTO = I.CODPROD
                        AND A.CODIGO_FILIAL = I.CODFILIAL
                        AND A.CODCOMPRADOR = COM.MATRICULA
                        AND A.COD_PRODUTO = C.COD_PRODUTO
                        AND C.CODFORNEC = F.CODFORNEC
                        AND A.CODIGO_FILIAL = C.CODIGO_FILIAL
                        AND A.RN = 1
                        AND B.RN = 1
                        AND C.RN = 1
                        AND I.RN = 1
                        AND S.RN = 1
                        AND B.PRECOMINSUG > A.VL_VENDA
                        AND (A.VL_VENDA * 1.01) <= B.PRECOMINSUG
                        AND TRUNC(S.DTSAIDA) = TRUNC(SYSDATE)
                        ORDER BY A.DEPARTAMENTO,  A.COD_PRODUTO  
                                        
                                        )
                                        
                                        SELECT * FROM FINAL, SITEPRAZOFINANCEIRO SITE
                                        WHERE SITE.TITLE = 'MARGEM'
                                         AND MARGEM_ATUAL < (SITE.DAYS)
                                        ORDER BY MARGEM_ATUAL ASC
                                    
                        
             
            ";

            // Executando a consulta
            $statement = oci_parse($conexao, $consulta);
            if (!oci_execute($statement)) {
                $e = oci_error($statement);
                return $response->withJson(['error' => $e['message']], 500);
            }

            // Manipulando os resultados e formatando valores
            // Manipulando os resultados e formatando valores
            $dados = [];
            while ($row = oci_fetch_assoc($statement)) {
                $row = array_map(function ($value) {
                    if (is_string($value)) {
                        return utf8_encode($value);
                    } elseif (is_numeric($value)) {
                        // Formata o número com 2 casas decimais e adiciona "0" se começar com "."
                        $formattedValue = number_format((float)$value, 2, '.', '');
                        return (strpos($formattedValue, '.') === 0) ? "0" . $formattedValue : $formattedValue;
                    }
                    return $value;
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

    $this->post('/precificacao/update', function (Request $request, Response $response) {

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
        $PRECOMINSUG = $params['PRECOMINSUG'] ?? null;
        $COD_PRODUTO = $params['COD_PRODUTO'] ?? null;

        // Valida se os parâmetros necessários foram enviados
        if (!$PRECOMINSUG || !$COD_PRODUTO) {
            return $response->withJson(['error' => 'Parâmetros inválidos'], 400);
        }

        // Primeira SQL de UPDATE para 'UN'
        $sqlUpdateUN = "
            UPDATE PCEMBALAGEM B 
            SET B.PTABELA = :PRECOMINSUG, B.JUSTIFICATIVAPRECO = NULL, B.DTULTALTPTABELA = SYSDATE
            WHERE B.CODPROD = :COD_PRODUTO
             AND B.UNIDADE in ('UN', 'KG')
        ";

        // Primeira SQL de UPDATE para 'UN' ATACADO
        $sqlUpdateUNAtacado = "
            UPDATE PCEMBALAGEM B 
            SET B.PTABELAATAC = ROUND(:PRECOMINSUG * 0.95, 2), B.JUSTIFICATIVAPRECO = NULL, B.DTULTALTPTABELA = SYSDATE
            WHERE B.CODPROD = :COD_PRODUTO
            AND B.UNIDADE in ('UN', 'KG')
        ";

        // Segunda SQL de UPDATE para 'CX'
        $sqlUpdateCX = "
            UPDATE PCEMBALAGEM B 
            SET B.PTABELA = ROUND(:PRECOMINSUG * B.QTUNIT, 2), B.JUSTIFICATIVAPRECO = NULL, B.DTULTALTPTABELA = SYSDATE
            WHERE B.CODPROD = :COD_PRODUTO
            AND B.UNIDADE IN ('CX','SC','CT','DP','FD','PC','PT', 'UN')
        ";

        // Segunda SQL de UPDATE para 'CX' ATACADO
        $sqlUpdateCXAtacado = "
            UPDATE PCEMBALAGEM B 
            SET B.PTABELAATAC = ROUND(:PRECOMINSUG * 0.95 * B.QTUNIT, 2), B.JUSTIFICATIVAPRECO = NULL, B.DTULTALTPTABELA = SYSDATE
            WHERE B.CODPROD = :COD_PRODUTO
            AND B.UNIDADE IN ('CX','SC','CT','DP','FD','PC','PT', 'UN')
        ";

        // Preparando e executando o primeiro UPDATE (para 'UN')
        $stmtUpdateUN = oci_parse($conexao, $sqlUpdateUN);
        oci_bind_by_name($stmtUpdateUN, ":PRECOMINSUG", $PRECOMINSUG);
        oci_bind_by_name($stmtUpdateUN, ":COD_PRODUTO", $COD_PRODUTO);
        $resultUN = oci_execute($stmtUpdateUN);

        // Preparando e executando o primeiro UPDATE (para 'UN') ATACADO
        $stmtUpdateUNAtacado = oci_parse($conexao, $sqlUpdateUNAtacado);
        oci_bind_by_name($stmtUpdateUNAtacado, ":PRECOMINSUG", $PRECOMINSUG);
        oci_bind_by_name($stmtUpdateUNAtacado, ":COD_PRODUTO", $COD_PRODUTO);
        $resultUNAtacado = oci_execute($stmtUpdateUNAtacado);

        // Preparando e executando o segundo UPDATE (para 'CX')
        $stmtUpdateCX = oci_parse($conexao, $sqlUpdateCX);
        oci_bind_by_name($stmtUpdateCX, ":PRECOMINSUG", $PRECOMINSUG);
        oci_bind_by_name($stmtUpdateCX, ":COD_PRODUTO", $COD_PRODUTO);
        $resultCX = oci_execute($stmtUpdateCX);

        // Preparando e executando o segundo UPDATE (para 'CX') ATACADO
        $stmtUpdateCXAtacado = oci_parse($conexao, $sqlUpdateCXAtacado);
        oci_bind_by_name($stmtUpdateCXAtacado, ":PRECOMINSUG", $PRECOMINSUG);
        oci_bind_by_name($stmtUpdateCXAtacado, ":COD_PRODUTO", $COD_PRODUTO);
        $resultCXAtacado = oci_execute($stmtUpdateCXAtacado);

        // Verifica se todos os updates foram bem-sucedidos
        if ($resultUN && $resultUNAtacado && $resultCX && $resultCXAtacado) {
            // Se os updates forem bem-sucedidos, retorna uma mensagem de sucesso
            return $response->withJson(['message' => 'Atualização bem-sucedida'], 200);
        } else {
            // Se houver erro em qualquer um dos updates, retorna uma mensagem de erro
            $e = oci_error();
            return $response->withJson(['error' => $e['message']], 500);
        }

        // Fechar os statements e a conexão
        oci_free_statement($stmtUpdateUN);
        oci_free_statement($stmtUpdateCX);
        oci_free_statement($stmtUpdateUNAtacado);
        oci_free_statement($stmtUpdateCXAtacado);
        oci_close($conexao);
    });

    //entrada nf

    $this->get('/precificacao/notanf/lista', function (Request $request, Response $response) {

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

        // Obtendo a data da requisição (GET parameter)
        $params = $request->getQueryParams();
        $data_entrada = isset($params['data_entrada']) ? date('d-M-Y', strtotime($params['data_entrada'])) : '02-SEP-2024';


        $sql = "WITH CTE AS (
            SELECT
                E.CODFILIAL AS CODIGO_FILIAL,
                P.CODPROD AS COD_PRODUTO,
                P.DESCRICAO AS PRODUTO,
                P.CODAUXILIAR,
                NC.CODNCM, 
                B.MARGEM AS MARGEM_IDEAL,
                P.CLASSEVENDA,
                E.CUSTOULTENT AS CUSTO,
                ROUND(B.PVENDA, 2) AS VL_VENDA,
                ROW_NUMBER() OVER (
                    PARTITION BY E.CODFILIAL,
                    P.CODPROD
                    ORDER BY
                        PDO.DTFATUR DESC
                ) AS RN
            
            FROM
                PCEST E 
                JOIN PCPRODUT P ON E.CODPROD = P.CODPROD
                JOIN PCFORNEC F ON P.CODFORNEC = F.CODFORNEC
                JOIN PCITEM I ON P.CODPROD = I.CODPROD
                JOIN PCPEDIDO PDO ON I.NUMPED = PDO.NUMPED
                AND PDO.CODFILIAL = E.CODFILIAL
                JOIN PCEMBALAGEM B ON P.CODPROD = B.CODPROD
                AND E.CODFILIAL = B.CODFILIAL
                JOIN PCDEPTO D ON P.CODEPTO = D.CODEPTO
                JOIN PCNCM NC ON P.CODNCMEX = NC.CODNCMEX
                JOIN PCMARCA MAR ON P.CODMARCA = MAR.CODMARCA
                JOIN PCSECAO CS ON P.CODSEC = CS.CODSEC  
            WHERE
                PDO.DTFATUR > TO_DATE('01-JAN-2024', 'DD-MM-YYYY')
                AND B.EMBALAGEM IN ('UN', 'KG')
            ),
                       ULTIMA_ENTRADA AS (
               
                SELECT
                    M.CODOPER,
                    M.NUMNOTA,
                    M.NUMPED,
                    M.NUMTRANSENT,
                    M.CODFORNEC,
                    M.PERCREDICMS AS CREDITO_CUSTO_ICMS,
                    M.PERPIS AS PIS,
                    M.PERCOFINS AS COFINS,
                    (M.PERCREDICMS + M.PERPIS + M.PERCOFINS) AS CREDITO_ENTRADA,
                    M.VALORULTENT AS VALOR_ULTIMA_ENTRADA,
                    M.CODFILIAL AS CODIGO_FILIAL,
                    M.CODPROD AS COD_PRODUTO,
                    M.DTMOV AS DATA_ULTIMA_ENTRADA,
                    M.QT AS QT_TRANSFERIDA,
                    ROW_NUMBER() OVER (
                        PARTITION BY M.CODFILIAL,
                        M.CODPROD
                        ORDER BY
                            M.DTMOV DESC
                    ) AS RN
                FROM
                    PCMOV M, PCPEDIDO P
                WHERE M.NUMPED = P.NUMPED
                    AND M.DTMOV > TO_DATE('01-JAN-2023', 'DD-MM-YYYY')
                    AND M.CODOPER IN ('E','EB')
                    AND P.TIPOBONIFIC IN ('N', 'B')
           
     
            ),


          PVT AS (
              SELECT 
						    P.CODPROD, 
						    M.CODFILIAL,
						    ROUND(
						        M.VALORULTENT + 
						        (M.VALORULTENT * NVL(SM.MARKUP, 40) / 100), 
						        2
						    ) AS PRECOMINSUG,
						    CONCAT(NVL(SM.MARKUP, 40), '%') AS MARKUP,
						    ROW_NUMBER() OVER (PARTITION BY M.CODFILIAL, P.CODPROD ORDER BY M.DTMOV DESC) AS RN
						FROM 
						    PCPRODUT P
						    JOIN PCSECAO S ON P.CODSEC = S.CODSEC -- Relaciona com a seção
						    JOIN PCMOV M ON P.CODPROD = M.CODPROD -- Relaciona com o movimento
						    LEFT JOIN SITEMARKUP SM 
						        ON P.CODSEC = SM.CODSEC 
						        AND P.CLASSEVENDA = SM.CURVA -- Relaciona markup com classe de venda e seção
						WHERE 
						    M.CODOPER = 'E' -- Somente entradas
						    AND M.DTMOV > TO_DATE('01-JAN-2023', 'DD-MM-YYYY')
                         
            ),
             IMPOSTO AS (
             
				 SELECT
				        CODPROD,
				        CODFILIAL,
				        PERCIMP,
				        ROW_NUMBER() OVER (
				            PARTITION BY CODFILIAL,
				            CODPROD
				            ORDER BY
				                CODPROD ASC
				        ) AS RN
				    FROM
				        (
				            SELECT
				                NVL(
				                    (
				                        CASE
				                            WHEN PCEMBALAGEM.DTOFERTAINI <= TRUNC(SYSDATE)
				                            AND PCEMBALAGEM.DTOFERTAFIM >= TRUNC(SYSDATE) THEN PCEMBALAGEM.POFERTA
				                            ELSE PCEMBALAGEM.PVENDA
				                        END
				                    ),
				                    0
				                ) PVENDA,
				                NVL(
				                    (
				                        CASE
				                            WHEN PCCONSUM.SUGVENDA = 1 THEN PCEST.CUSTOREAL
				                            WHEN PCCONSUM.SUGVENDA = 2 THEN PCEST.CUSTOFIN
				                            ELSE PCEST.CUSTOULTENT
				                        END
				                    ),
				                    0
				                ) PCUSTO,
				                PCEMBALAGEM.MARGEM,
				                PCTRIBUT.CODICMTAB,
				                PCTRIBUT.PERDESCCUSTO,
				                PCEMBALAGEM.CODAUXILIAR,
				                PCPRODUT.DESCRICAO,
				                PCEMBALAGEM.EMBALAGEM,
				                PCEMBALAGEM.UNIDADE,
				                PCPRODUT.PRECOFIXO,
				                PCPRODUT.PCOMREP1,
				                PCPRODUT.PESOBRUTO,
				                PCEST.CUSTOFIN,
				                PCEST.CUSTOREAL,
				                PCEST.CODFILIAL,
				                PCEST.CODPROD,
				                PCEST.DTULTENT,
				                PCEST.QTULTENT,
				                PCEST.CUSTOULTENT, 
				                (
				                    NVL(PCEST.QTESTGER, 0) - NVL(PCEST.QTBLOQUEADA, 0) - NVL(PCEST.QTRESERV, 0)
				                ) QTESTDISP,
				                PCREGIAO.VLFRETEKG,
				                (
				                    NVL(PCCONSUM.TXVENDA, 0) + NVL(PCPRODUT.PCOMREP1, 0) + NVL(PCTRIBUT.CODICMTAB, 0)
				                ) / 100 PERCIMP,
				                NVL(PCTRIBUT.PERDESCCUSTO, 0) / 100 PERDESCUSTO
				            FROM
				                PCTABTRIB,
				                PCPRODUT,
				                PCEMBALAGEM,
				                PCEST,
				                PCTRIBUT,
				                PCCONSUM,
				                PCREGIAO
				            WHERE
				                PCTABTRIB.CODPROD = PCPRODUT.CODPROD
				                AND PCPRODUT.CODPROD = PCEST.CODPROD
				                AND PCPRODUT.CODPROD = PCEMBALAGEM.CODPROD
				                AND PCTABTRIB.CODST = PCTRIBUT.CODST
				                AND PCREGIAO.NUMREGIAO = 1.000000
				                AND PCTABTRIB.UFDESTINO = 'PA'
				                AND PCTABTRIB.CODFILIALNF = PCEMBALAGEM.CODFILIAL
				                AND PCEMBALAGEM.CODFILIAL = PCEST.CODFILIAL
				                
				        )
				
				             ),
				             
				             SEMI_FINAL AS (
				               SELECT 
                    A.CODIGO_FILIAL,
                    C.NUMNOTA,
                    C.NUMPED,
                    PE.CODCOMPRADOR,
                    COM.NOME,
                    A.COD_PRODUTO,
                    F.FORNECEDOR,
                    A.PRODUTO,
                    A.CODAUXILIAR,
                    A.CLASSEVENDA,
                    C.DATA_ULTIMA_ENTRADA,
                    A.CODNCM, 
                    A.MARGEM_IDEAL,
                    ROUND(C.VALOR_ULTIMA_ENTRADA, 2 ) VALOR_ULTIMA_ENTRADA,
                    ROUND(A.CUSTO, 2) CUSTO,
                     ROUND((((A.VL_VENDA - C.VALOR_ULTIMA_ENTRADA) / NULLIF(A.VL_VENDA, 0)) * 100), 2) AS MARGEM_ATUAL,
                                         CASE 
					    WHEN A.VL_VENDA <> 0 THEN ROUND(((A.VL_VENDA - ((I.PERCIMP * A.VL_VENDA) + A.CUSTO)) / A.VL_VENDA) * 100, 2) 
					    ELSE NULL 
					END AS MARGEM_ATUAL_WTH,
                    A.VL_VENDA,
                     CASE
                     	WHEN A.VL_VENDA > B.PRECOMINSUG THEN A.VL_VENDA
                     	WHEN A.VL_VENDA <= B.PRECOMINSUG THEN B.PRECOMINSUG
                    	ELSE NULL
                	END AS PRECOMINSUG,
                  
                    B.MARKUP,
                                        I.PERCIMP AS IMPOSTO
                    
                    
                    
                    
                    
           FROM CTE A, PVT B, ULTIMA_ENTRADA C, PCPEDIDO PE, PCEMPR COM, PCFORNEC F, PCNFENT E, IMPOSTO I
            WHERE A.COD_PRODUTO = B.CODPROD
            AND A.CODIGO_FILIAL = B.CODFILIAL
           	AND C.NUMNOTA = E.NUMNOTA
          	AND PE.NUMPED = C.NUMPED
          	            AND A.COD_PRODUTO = I.CODPROD
            AND A.CODIGO_FILIAL = I.CODFILIAL
            AND PE.CODCOMPRADOR = COM.MATRICULA
            AND A.COD_PRODUTO = C.COD_PRODUTO
            AND C.CODFORNEC = F.CODFORNEC
            AND A.CODIGO_FILIAL = C.CODIGO_FILIAL
            AND C.DATA_ULTIMA_ENTRADA = E.DTENT
            AND A.RN = 1
            AND B.RN = 1
            AND C.RN = 1
             AND I.RN = 1
            AND C.DATA_ULTIMA_ENTRADA = TO_DATE(:data_entrada, 'DD-MON-YYYY')
            ORDER BY  C.NUMNOTA      
            
            )
            
            SELECT 
            F.*,
            ROUND(((F.PRECOMINSUG-((F.IMPOSTO * F.PRECOMINSUG)+F.CUSTO))/F.PRECOMINSUG)*100, 2) AS MARGEM_FUTURA_WTH 
            FROM SEMI_FINAL F
             
             
                       
            ";


        $stmt = oci_parse($conexao, $sql);

        // Vincular o parâmetro :data_entrada ao valor obtido
        oci_bind_by_name($stmt, ":data_entrada", $data_entrada);

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

    // atacado

    $this->get('/precificacao/atacado/lista', function (Request $request, Response $response) {

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

        // Obtendo a data da requisição (GET parameter)
        $params = $request->getQueryParams();
        $data_entrada = isset($params['data_entrada']) ? date('d-M-Y', strtotime($params['data_entrada'])) : '02-SEP-2024';


        $sql = "WITH CTE AS (
            SELECT
                E.CODFILIAL AS CODIGO_FILIAL,
                P.CODPROD AS COD_PRODUTO,
                P.DESCRICAO AS PRODUTO,
           		B.PVENDAATAC ,
                D.DESCRICAO AS DEPARTAMENTO,
                P.CODAUXILIAR,
                NC.CODNCM, 
                PDO.NUMPED,
                PDO.CODCOMPRADOR,
                B.MARGEM AS MARGEM_IDEAL,
                P.CLASSEVENDA,
                E.CUSTOULTENT AS CUSTO,
                ROUND(B.PVENDA, 2) AS VL_VENDA,
                ROW_NUMBER() OVER (
                    PARTITION BY E.CODFILIAL,
                    P.CODPROD
                    ORDER BY
                        PDO.DTFATUR DESC
                ) AS RN
            
            FROM
                PCEST E 
                JOIN PCPRODUT P ON E.CODPROD = P.CODPROD
                JOIN PCFORNEC F ON P.CODFORNEC = F.CODFORNEC
                JOIN PCITEM I ON P.CODPROD = I.CODPROD
                JOIN PCPEDIDO PDO ON I.NUMPED = PDO.NUMPED
                AND PDO.CODFILIAL = E.CODFILIAL
                JOIN PCEMBALAGEM B ON P.CODPROD = B.CODPROD
                AND E.CODFILIAL = B.CODFILIAL
                JOIN PCDEPTO D ON P.CODEPTO = D.CODEPTO 
                JOIN PCNCM NC ON P.CODNCMEX = NC.CODNCMEX
                JOIN PCMARCA MAR ON P.CODMARCA = MAR.CODMARCA
                JOIN PCSECAO CS ON P.CODSEC = CS.CODSEC  
            WHERE
                PDO.DTFATUR > TO_DATE('01-JAN-2024', 'DD-MM-YYYY')
                AND B.EMBALAGEM IN ('UN', 'KG')
            ),
                      ULTIMA_ENTRADA AS (
               
                SELECT
                    M.CODOPER,
                    M.NUMNOTA,
                    M.NUMPED,
                    M.NUMTRANSENT,
                    M.CODFORNEC,
                    M.PERCREDICMS AS CREDITO_CUSTO_ICMS,
                    M.PERPIS AS PIS,
                    M.PERCOFINS AS COFINS,
                    (M.PERCREDICMS + M.PERPIS + M.PERCOFINS) AS CREDITO_ENTRADA,
                    M.VALORULTENT AS VALOR_ULTIMA_ENTRADA,
                    M.CODFILIAL AS CODIGO_FILIAL,
                    M.CODPROD AS COD_PRODUTO,
                    M.DTMOV AS DATA_ULTIMA_ENTRADA,
                    M.QT AS QT_TRANSFERIDA,
                    ROW_NUMBER() OVER (
                        PARTITION BY M.CODFILIAL,
                        M.CODPROD
                        ORDER BY
                            M.DTMOV DESC
                    ) AS RN
                FROM
                    PCMOV M, PCPEDIDO P
                WHERE M.NUMPED = P.NUMPED
                    AND M.DTMOV > TO_DATE('01-JAN-2023', 'DD-MM-YYYY')
                    AND M.CODOPER IN ('E','EB')
                    AND P.TIPOBONIFIC IN ('N', 'B')
           
     
            ),



             PVT AS (
              SELECT 
						    P.CODPROD, 
						    M.CODFILIAL,
						    ROUND(
						        M.VALORULTENT + 
						        (M.VALORULTENT * NVL(SM.MARKUP, 40) / 100), 
						        2
						    ) AS PRECOMINSUG,
						    CONCAT(NVL(SM.MARKUP, 40), '%') AS MARKUP,
						    ROW_NUMBER() OVER (PARTITION BY M.CODFILIAL, P.CODPROD ORDER BY M.DTMOV DESC) AS RN
						FROM 
						    PCPRODUT P
						    JOIN PCSECAO S ON P.CODSEC = S.CODSEC -- Relaciona com a seção
						    JOIN PCMOV M ON P.CODPROD = M.CODPROD -- Relaciona com o movimento
						    LEFT JOIN SITEMARKUP SM 
						        ON P.CODSEC = SM.CODSEC 
						        AND P.CLASSEVENDA = SM.CURVA -- Relaciona markup com classe de venda e seção
						WHERE 
						    M.CODOPER = 'E' -- Somente entradas
						    AND M.DTMOV > TO_DATE('01-JAN-2023', 'DD-MM-YYYY')
                         
            ),
             IMPOSTO AS (
             
				 SELECT
				        CODPROD,
				        CODFILIAL,
				        PERCIMP,
				        ROW_NUMBER() OVER (
				            PARTITION BY CODFILIAL,
				            CODPROD
				            ORDER BY
				                CODPROD ASC
				        ) AS RN
				    FROM
				        (
				            SELECT
				                NVL(
				                    (
				                        CASE
				                            WHEN PCEMBALAGEM.DTOFERTAINI <= TRUNC(SYSDATE)
				                            AND PCEMBALAGEM.DTOFERTAFIM >= TRUNC(SYSDATE) THEN PCEMBALAGEM.POFERTA
				                            ELSE PCEMBALAGEM.PVENDA
				                        END
				                    ),
				                    0
				                ) PVENDA,
				                NVL(
				                    (
				                        CASE
				                            WHEN PCCONSUM.SUGVENDA = 1 THEN PCEST.CUSTOREAL
				                            WHEN PCCONSUM.SUGVENDA = 2 THEN PCEST.CUSTOFIN
				                            ELSE PCEST.CUSTOULTENT
				                        END
				                    ),
				                    0
				                ) PCUSTO,
				                PCEMBALAGEM.MARGEM,
				                PCTRIBUT.CODICMTAB,
				                PCTRIBUT.PERDESCCUSTO,
				                PCEMBALAGEM.CODAUXILIAR,
				                PCPRODUT.DESCRICAO,
				                PCEMBALAGEM.EMBALAGEM,
				                PCEMBALAGEM.UNIDADE,
				                PCPRODUT.PRECOFIXO,
				                PCPRODUT.PCOMREP1,
				                PCPRODUT.PESOBRUTO,
				                PCEST.CUSTOFIN,
				                PCEST.CUSTOREAL,
				                PCEST.CODFILIAL,
				                PCEST.CODPROD,
				                PCEST.DTULTENT,
				                PCEST.QTULTENT,
				                PCEST.CUSTOULTENT, 
				                (
				                    NVL(PCEST.QTESTGER, 0) - NVL(PCEST.QTBLOQUEADA, 0) - NVL(PCEST.QTRESERV, 0)
				                ) QTESTDISP,
				                PCREGIAO.VLFRETEKG,
				                (
				                    NVL(PCCONSUM.TXVENDA, 0) + NVL(PCPRODUT.PCOMREP1, 0) + NVL(PCTRIBUT.CODICMTAB, 0)
				                ) / 100 PERCIMP,
				                NVL(PCTRIBUT.PERDESCCUSTO, 0) / 100 PERDESCUSTO
				            FROM
				                PCTABTRIB,
				                PCPRODUT,
				                PCEMBALAGEM,
				                PCEST,
				                PCTRIBUT,
				                PCCONSUM,
				                PCREGIAO
				            WHERE
				                PCTABTRIB.CODPROD = PCPRODUT.CODPROD
				                AND PCPRODUT.CODPROD = PCEST.CODPROD
				                AND PCPRODUT.CODPROD = PCEMBALAGEM.CODPROD
				                AND PCTABTRIB.CODST = PCTRIBUT.CODST
				                AND PCREGIAO.NUMREGIAO = 1.000000
				                AND PCTABTRIB.UFDESTINO = 'PA'
				                AND PCTABTRIB.CODFILIALNF = PCEMBALAGEM.CODFILIAL
				                AND PCEMBALAGEM.CODFILIAL = PCEST.CODFILIAL
				                
				        )
				
				             )
             
             
            SELECT 
                    A.CODIGO_FILIAL,
                     C.NUMNOTA,
                     A.DEPARTAMENTO,
                     F.FORNECEDOR,
                    A.NUMPED,
                    A.CODCOMPRADOR,
                    COM.NOME,
                    A.COD_PRODUTO,
                    A.PRODUTO,
                    A.CODAUXILIAR,
                    A.CLASSEVENDA,
                    C.DATA_ULTIMA_ENTRADA,
                    A.CODNCM, 
                    A.MARGEM_IDEAL,
                    ROUND(C.VALOR_ULTIMA_ENTRADA, 2 ) VALOR_ULTIMA_ENTRADA,
                    ROUND(A.CUSTO, 2) CUSTO,
                    ROUND((((A.PVENDAATAC - C.VALOR_ULTIMA_ENTRADA) / NULLIF(A.PVENDAATAC, 0)) * 100), 2) AS MARGEM_ATUAL,
                    CASE 
					    WHEN A.PVENDAATAC <> 0 THEN ROUND(((A.PVENDAATAC - ((I.PERCIMP * A.PVENDAATAC) + A.CUSTO)) / A.PVENDAATAC) * 100, 2) 
					    ELSE NULL 
					END AS MARGEM_ATUAL_ATAC_WTH,
                    A.VL_VENDA,
                    A.PVENDAATAC AS VALOR_ATACADO,
                    ROUND((B.PRECOMINSUG * 0.95), 2) AS PRECOMINSUG,
                    B.MARKUP,
                    I.PERCIMP AS IMPOSTO,
                    ROUND((((B.PRECOMINSUG * 0.95)-((I.PERCIMP*(B.PRECOMINSUG * 0.95))+A.CUSTO))/(B.PRECOMINSUG * 0.95))*100, 2) AS MARGEM_FUTURA_WTH
                    
                    
            FROM CTE A, PVT B, ULTIMA_ENTRADA C, PCEMPR COM, PCFORNEC F, IMPOSTO I
            WHERE A.COD_PRODUTO = B.CODPROD
            AND A.CODIGO_FILIAL = B.CODFILIAL
            AND A.COD_PRODUTO = I.CODPROD
            AND A.CODIGO_FILIAL = I.CODFILIAL
            AND A.CODCOMPRADOR = COM.MATRICULA
            AND A.COD_PRODUTO = C.COD_PRODUTO
            AND C.CODFORNEC = F.CODFORNEC
            AND A.CODIGO_FILIAL = C.CODIGO_FILIAL
            AND A.RN = 1
            AND B.RN = 1
            AND C.RN = 1
            AND I.RN = 1
            AND A.DEPARTAMENTO != 'ACOUGUE'
            AND (A.PVENDAATAC < (C.VALOR_ULTIMA_ENTRADA * 1.10) OR A.PVENDAATAC > (C.VALOR_ULTIMA_ENTRADA * 1.25))
            AND C.DATA_ULTIMA_ENTRADA = TO_DATE(:data_entrada, 'DD-MON-YYYY')
            ORDER BY A.DEPARTAMENTO,  A.COD_PRODUTO      
            ";


        $stmt = oci_parse($conexao, $sql);

        // Vincular o parâmetro :data_entrada ao valor obtido
        oci_bind_by_name($stmt, ":data_entrada", $data_entrada);

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

    $this->post('/precificacao/atacado/update', function (Request $request, Response $response) {

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
        $PRECOMINSUG = $params['PRECOMINSUG'] ?? null;
        $COD_PRODUTO = $params['COD_PRODUTO'] ?? null;

        // Valida se os parâmetros necessários foram enviados
        if (!$PRECOMINSUG || !$COD_PRODUTO) {
            return $response->withJson(['error' => 'Parâmetros inválidos'], 400);
        }

        // Primeira SQL de UPDATE para 'UN' ATACADO
        $sqlUpdateCX = "
                UPDATE PCEMBALAGEM B 
                SET B.PTABELAATAC = :PRECOMINSUG , B.JUSTIFICATIVAPRECO = NULL, B.DTULTALTPTABELA = SYSDATE
                WHERE B.CODPROD = :COD_PRODUTO
                AND B.UNIDADE in ('UN', 'KG')
        ";

        // Segunda SQL de UPDATE para 'CX' ATACADO
        $sqlUpdateCXAtacado = "
            UPDATE PCEMBALAGEM B 
            SET B.PTABELAATAC = (:PRECOMINSUG  * B.QTUNIT), B.JUSTIFICATIVAPRECO = NULL, B.DTULTALTPTABELA = SYSDATE
            WHERE B.CODPROD = :COD_PRODUTO
            AND B.UNIDADE IN ('CX','SC','CT','DP','FD','PC','PT', 'UN')
        ";

        // Preparando e executando o segundo UPDATE (para 'CX')
        $stmtUpdateCX = oci_parse($conexao, $sqlUpdateCX);
        oci_bind_by_name($stmtUpdateCX, ":PRECOMINSUG", $PRECOMINSUG);
        oci_bind_by_name($stmtUpdateCX, ":COD_PRODUTO", $COD_PRODUTO);
        $resultCX = oci_execute($stmtUpdateCX);

        // Preparando e executando o segundo UPDATE (para 'CX') ATACADO
        $stmtUpdateCXAtacado = oci_parse($conexao, $sqlUpdateCXAtacado);
        oci_bind_by_name($stmtUpdateCXAtacado, ":PRECOMINSUG", $PRECOMINSUG);
        oci_bind_by_name($stmtUpdateCXAtacado, ":COD_PRODUTO", $COD_PRODUTO);
        $resultCXAtacado = oci_execute($stmtUpdateCXAtacado);

        // Verifica se todos os updates foram bem-sucedidos
        if ($resultCX && $resultCXAtacado) {
            // Se os updates forem bem-sucedidos, retorna uma mensagem de sucesso
            return $response->withJson(['message' => 'Atualização bem-sucedida'], 200);
        } else {
            // Se houver erro em qualquer um dos updates, retorna uma mensagem de erro
            $e = oci_error();
            return $response->withJson(['error' => $e['message']], 500);
        }

        // Fechar os statements e a conexão
        oci_free_statement($stmtUpdateUN);
        oci_free_statement($stmtUpdateCX);
        oci_free_statement($stmtUpdateUNAtacado);
        oci_free_statement($stmtUpdateCXAtacado);
        oci_close($conexao);
    });

    // FILTRO PRODUTO
    $this->get('/precificacao/filtro/produto/{codproduto}/{filial}', function (Request $request, Response $response) {

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

        $codproduto = $request->getAttribute('codproduto');
        $filial = $request->getAttribute('filial');

        $sql = "WITH CTE AS (
                SELECT
                    E.CODFILIAL AS CODIGO_FILIAL,
                    P.CODPROD AS COD_PRODUTO,
                    P.DESCRICAO AS PRODUTO,
                    MAR.CODMARCA,
                    MAR.MARCA,
                    NC.CODNCM,
                    NC.DESCRICAO,
                    P.QTUNITCX,
                    B.MARGEM AS MARGEM_IDEAL,
                    F.FORNECEDOR AS FORNECEDOR_203,
                    E.CUSTOULTENT AS CUSTO,
                    B.PVENDA AS VL_VENDA,
                    F.PRAZOENTREGA AS PRAZO_DE_ENTREGA,
                    E.QTPEDIDA AS PED_ABERTO,
                    CASE
                        WHEN E.QTPEDIDA > 0 THEN PDO.NUMPED
                        ELSE NULL
                    END AS NUMERO_PEDIDO,
                    CASE
                        WHEN E.QTPEDIDA > 0 THEN PDO.DTFATUR
                        ELSE NULL
                    END AS DATA_FATURAMENTO_ROT_220,
                    PDO.CODFORNEC AS COD_FORNECEDOR_DO_PEDIDO,
                    D.CODEPTO AS CODIGO_DEPARTAMENTO,
                    D.DESCRICAO AS DEPARTAMENTO,
                    CS.CODSEC AS CODIGO_SECAO,  
                    CS.DESCRICAO AS SECAO,
                    E.ESTMIN AS ESTOQUE_MINIMO,
                    E.ESTMAX AS ESTOQUE_MAXIMO,
                    E.QTGIRODIA AS GIRO_DIA,
                    P.TEMREPOS AS TEMPO_REPOSICAO,
                    B.DTULTALTPVENDA,
                    E.QTESTGER AS QT_ESTOQUE,
                    I.QTPEDIDA AS QT_ULT_PEDIDO,
                    ROW_NUMBER() OVER (
                        PARTITION BY E.CODFILIAL,
                        P.CODPROD
                        ORDER BY
                            PDO.DTFATUR DESC
                    ) AS rn,
                    E.QTGIRODIA,
                    F.PRAZOENTREGA,
                    P.TEMREPOS,
                    E.QTESTGER,
                    E.QTRESERV,
                    E.QTINDENIZ,
                    E.ESTMIN,
                    F.SIMPLESNACIONAL,
                    E.ESTMAX,
                    GREATEST(E.QTGIRODIA * (F.PRAZOENTREGA + P.TEMREPOS)) AS ESTOQUE_IDEAL,
                    E.QTPEDIDA AS QTD_PENDENDET,
                    GREATEST(
                        E.QTGIRODIA * (F.PRAZOENTREGA + P.TEMREPOS),
                        E.ESTMIN
                    ) - (
                        (E.QTESTGER - E.QTRESERV - E.QTINDENIZ) + COALESCE(E.QTPEDIDA, 0)
                    ) AS SUGESTAO_COMPRA,
                    (
                        SELECT
                            SUM(QTESTGER) AS ESTOQUE_TOTAL
                        FROM
                            PCEST
                        WHERE
                            CODFILIAL IN (1, 2, 3, 4, 5)
                            AND PCEST.CODPROD = P.CODPROD
                    ) AS ESTOQUE_MULTI_FILIAL
                FROM
                    PCEST E 
                    JOIN PCPRODUT P ON E.CODPROD = P.CODPROD
                    JOIN PCFORNEC F ON P.CODFORNEC = F.CODFORNEC
                    JOIN PCITEM I ON P.CODPROD = I.CODPROD
                    JOIN PCPEDIDO PDO ON I.NUMPED = PDO.NUMPED
                    AND PDO.CODFILIAL = E.CODFILIAL
                    JOIN PCEMBALAGEM B ON P.CODPROD = B.CODPROD
                    AND E.CODFILIAL = B.CODFILIAL
                    JOIN PCDEPTO D ON P.CODEPTO = D.CODEPTO
                    JOIN PCNCM NC ON P.CODNCMEX = NC.CODNCMEX
                    JOIN PCMARCA MAR ON P.CODMARCA = MAR.CODMARCA
                    JOIN PCSECAO CS ON P.CODSEC = CS.CODSEC
                WHERE
                    PDO.DTFATUR > TO_DATE('01-JUL-2024', 'DD-MM-YYYY')
                    AND P.CODPROD IN (:codproduto)
            ),

            ULTIMA_ENTRADA AS (
                SELECT
                    M.CODOPER,
                    M.CUSTOFIN,
                    M.PTABELA,
                    M.NUMNOTA,
                    M.NUMPED,
                    M.NUMTRANSENT,
                    M.PERCREDICMS AS CREDITO_CUSTO_ICMS,
                    M.PERPIS AS PIS,
                    M.PERCOFINS AS COFINS,
                    (M.PERCREDICMS + M.PERPIS + M.PERCOFINS) AS CREDITO_ENTRADA,
                    M.VALORULTENT AS VALOR_ULTIMA_ENTRADA,
                    M.CODFILIAL AS CODIGO_FILIAL,
                    M.CODPROD AS COD_PRODUTO,
                    M.DTMOV AS DATA_ULTIMA_ENTRADA,
                    M.QT AS QT_TRANSFERIDA,
                    ROW_NUMBER() OVER (
                        PARTITION BY M.CODFILIAL,
                        M.CODPROD
                        ORDER BY
                            M.DTMOV DESC
                    ) AS rn
                FROM
                    PCMOV M
                    
                    
                WHERE
                    M.DTMOV > TO_DATE('01-JUL-2024', 'DD-MM-YYYY')
                    AND M.CODOPER IN ('E')
                
            ),
            MARGEM_DIARIA AS(
                SELECT
                    CODPROD,
                    CODFILIAL,
                    PERCIMP,
                    PERDESCUSTO,
                    PCUSTO *(1 - PERDESCUSTO) + PVENDA * PERCIMP AS CMV,
                    ROUND(
                        DECODE(
                            PVENDA,
                            0,
                            -100,
                            100 *(
                                PVENDA -(PCUSTO *(1 - PERDESCUSTO) + PVENDA * PERCIMP)
                            ) / PVENDA
                        ),
                        2
                    ) AS MARGEM_DIARIA,
                    ROW_NUMBER() OVER (
                        PARTITION BY CODFILIAL,
                        CODPROD
                        ORDER BY
                            CODPROD ASC
                    ) AS rn
                FROM
                    (
                        SELECT
                            NVL(
                                (
                                    CASE
                                        WHEN PCEMBALAGEM.DTOFERTAINI <= TRUNC(SYSDATE)
                                        AND PCEMBALAGEM.DTOFERTAFIM >= TRUNC(SYSDATE) THEN PCEMBALAGEM.POFERTA
                                        ELSE PCEMBALAGEM.PVENDA
                                    END
                                ),
                                0
                            ) PVENDA,
                            NVL(
                                (
                                    CASE
                                        WHEN PCCONSUM.SUGVENDA = 1 THEN PCEST.CUSTOREAL
                                        WHEN PCCONSUM.SUGVENDA = 2 THEN PCEST.CUSTOFIN
                                        ELSE PCEST.CUSTOULTENT
                                    END
                                ),
                                0
                            ) PCUSTO,
                            PCEMBALAGEM.MARGEM,
                            PCTRIBUT.CODICMTAB,
                            PCTRIBUT.PERDESCCUSTO,
                            PCEMBALAGEM.CODAUXILIAR,
                            PCPRODUT.DESCRICAO,
                            PCEMBALAGEM.EMBALAGEM,
                            PCEMBALAGEM.UNIDADE,
                            PCPRODUT.PRECOFIXO,
                            PCPRODUT.PCOMREP1,
                            PCPRODUT.PESOBRUTO,
                            PCEST.CUSTOFIN,
                            PCEST.CUSTOREAL,
                            PCEST.CODFILIAL,
                            PCEST.CODPROD,
                            PCEST.DTULTENT,
                            PCEST.QTULTENT,
                            PCEST.CUSTOULTENT, 
                            (
                                NVL(PCEST.QTESTGER, 0) - NVL(PCEST.QTBLOQUEADA, 0) - NVL(PCEST.QTRESERV, 0)
                            ) QTESTDISP,
                            PCREGIAO.VLFRETEKG,
                            (
                                NVL(PCCONSUM.TXVENDA, 0) + NVL(PCPRODUT.PCOMREP1, 0) + NVL(PCTRIBUT.CODICMTAB, 0)
                            ) / 100 PERCIMP,
                            NVL(PCTRIBUT.PERDESCCUSTO, 0) / 100 PERDESCUSTO
                        FROM
                            PCTABTRIB,
                            PCPRODUT,
                            PCEMBALAGEM,
                            PCEST,
                            PCTRIBUT,
                            PCCONSUM,
                            PCREGIAO
                        WHERE
                            PCTABTRIB.CODPROD = PCPRODUT.CODPROD
                            AND PCPRODUT.CODPROD = PCEST.CODPROD
                            AND PCPRODUT.CODPROD = PCEMBALAGEM.CODPROD
                            AND PCTABTRIB.CODST = PCTRIBUT.CODST
                            AND PCREGIAO.NUMREGIAO = 1.000000
                            AND PCTABTRIB.UFDESTINO = 'PA'
                            AND PCTABTRIB.CODFILIALNF = PCEMBALAGEM.CODFILIAL
                            AND PCEMBALAGEM.CODFILIAL = PCEST.CODFILIAL
                            
                    )
            ),


            TRIBUTACAO_SAIDA AS (
                SELECT
                    B.UFDESTINO,
                    B.CODFILIALNF,
                    B.CODPROD,
                    T.CODST,
                    T.MENSAGEM,
                    T.CODICMTAB
                FROM
                    PCTABTRIB B,
                    PCTRIBUT T
                WHERE
                    B.CODST = T.CODST
                    AND B.UFDESTINO = 'PA'
            ),

            NCM_NF AS (

            SELECT DTMOV, NBM, CODPROD, NUMNOTA,
            ROW_NUMBER() OVER (
                        PARTITION BY CODFILIAL,
                        CODPROD
                        ORDER BY
                            DTMOV DESC
                    ) AS rn 
            FROM PCMOVPREENT 

            )

            SELECT
            A.CODIGO_FILIAL,
                A.COD_PRODUTO,
                A.PRODUTO,
                ROUND(C.CMV, 2) CMV,
                B.CREDITO_ENTRADA,
                TS.CODICMTAB AS TRIB_SAIDA,
                ROUND(
                    (
                        (B.VALOR_ULTIMA_ENTRADA - A.CUSTO) / B.VALOR_ULTIMA_ENTRADA
                    ) * 100,
                    2
                ) AS CALC_PROPR_CRED,	
                B.NUMNOTA AS N_NF_ULT_ENT,
                A.CODNCM,
                NCNF.NBM

            FROM
                CTE A
                LEFT JOIN ULTIMA_ENTRADA B ON A.CODIGO_FILIAL = B.CODIGO_FILIAL
                AND A.COD_PRODUTO = B.COD_PRODUTO
                
                LEFT JOIN MARGEM_DIARIA C ON A.CODIGO_FILIAL = C.CODFILIAL
                AND A.COD_PRODUTO = C.CODPROD
            
                LEFT JOIN TRIBUTACAO_SAIDA TS ON A.COD_PRODUTO = TS.CODPROD
                AND A.CODIGO_FILIAL = TS.CODFILIALNF
                LEFT JOIN NCM_NF NCNF ON NCNF.CODPROD = A.COD_PRODUTO AND NCNF.NUMNOTA =  B.NUMNOTA
                
            WHERE
                A.rn = 1
                AND B.rn = 1
                AND C.RN = 1
                AND A.CODIGO_FILIAL = :filial
                
                ORDER BY A.CODIGO_FILIAL, B.NUMTRANSENT
            ";


        $stmt = oci_parse($conexao, $sql);

        // Associar a data formatada ao placeholder SQL
        oci_bind_by_name($stmt, ":codproduto", $codproduto);
        oci_bind_by_name($stmt, ":filial", $filial);

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


    $this->get('/precificacao/dia/{ano}/{mes}', function (Request $request, Response $response) {
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
        $sql = "WITH VALOR AS (
                SELECT 
                                TO_CHAR(FUNC_RETORNADIAFINANCEIRO(
                                    PCLANC.CODFILIAL, 
                                    0, 
                                    NVL(PCLANC.DTCHEQ, PCLANC.DTVENC), 
                                    1, 
                                    1, 
                                    'PCLANC'
                                ), 'DD') AS DIA,
                                TO_CHAR(FUNC_RETORNADIAFINANCEIRO(
                                    PCLANC.CODFILIAL, 
                                    0, 
                                    NVL(PCLANC.DTCHEQ, PCLANC.DTVENC), 
                                    1, 
                                    1, 
                                    'PCLANC'
                                ), 'MON') AS MES,
                                TO_CHAR(FUNC_RETORNADIAFINANCEIRO(
                                    PCLANC.CODFILIAL, 
                                    0, 
                                    NVL(PCLANC.DTCHEQ, PCLANC.DTVENC), 
                                    1, 
                                    1, 
                                    'PCLANC'
                                ), 'YYYY') AS ANO,
                                SUM(
                                    (NVL(PCLANC.VALOR, 0) 
                                    - NVL(PCLANC.VALORDEV, 0) 
                                    - NVL(PCLANC.DESCONTOFIN, 0) 
                                    + NVL(PCLANC.TXPERM, 0)) * (-1)
                                ) AS TOTAL_VALOR
                            FROM 
                                PCLANC
                            JOIN 
                                PCCONTA ON PCCONTA.CODCONTA = PCLANC.CODCONTA
                            WHERE 
                                PCLANC.DTPAGTO IS NULL
                                AND TO_CHAR(NVL(PCLANC.DTCHEQ, PCLANC.DTVENC), 'MON') = :mes
                                AND TO_CHAR(NVL(PCLANC.DTCHEQ, PCLANC.DTVENC), 'YYYY') = :ano 
                                AND TO_CHAR(FUNC_RETORNADIAFINANCEIRO(
                                    PCLANC.CODFILIAL, 
                                    0, 
                                    NVL(PCLANC.DTCHEQ, PCLANC.DTVENC),
                                    1,
                                    0,
                                    'PCLANC'
                                ), 'MON')= :mes
                                AND TO_CHAR(FUNC_RETORNADIAFINANCEIRO(
                                    PCLANC.CODFILIAL, 
                                    0, 
                                    NVL(PCLANC.DTCHEQ, PCLANC.DTVENC),
                                    1,
                                    0,
                                    'PCLANC'
                                ), 'YYYY') = :ano 

                            GROUP BY 
                                TO_CHAR(FUNC_RETORNADIAFINANCEIRO(
                                    PCLANC.CODFILIAL, 
                                    0, 
                                    NVL(PCLANC.DTCHEQ, PCLANC.DTVENC), 
                                    1, 
                                    1, 
                                    'PCLANC'
                                ), 'DD'),
                                TO_CHAR(FUNC_RETORNADIAFINANCEIRO(
                                    PCLANC.CODFILIAL, 
                                    0, 
                                    NVL(PCLANC.DTCHEQ, PCLANC.DTVENC), 
                                    1, 
                                    1, 
                                    'PCLANC'
                                ), 'MON'),
                                TO_CHAR(FUNC_RETORNADIAFINANCEIRO(
                                    PCLANC.CODFILIAL, 
                                    0, 
                                    NVL(PCLANC.DTCHEQ, PCLANC.DTVENC), 
                                    1, 
                                    1, 
                                    'PCLANC'
                                ), 'YYYY')



                ),

                PEDIDO AS (


                SELECT TO_CHAR(FUNC_RETORNADIAFINANCEIRO(PCLANC3.CODFILIAL, 
                                                0, 
                                                PCLANC3.DTVENC, 
                                                1, 
                                                1, 
                                                'PCLANC3'), 'DD') AS DIA,
                    TO_CHAR(FUNC_RETORNADIAFINANCEIRO(PCLANC3.CODFILIAL, 
                                                0, 
                                                PCLANC3.DTVENC, 
                                                1, 
                                                1, 
                                                'PCLANC3'), 'MON', 'NLS_DATE_LANGUAGE=ENGLISH') AS MES,
                    TO_CHAR(FUNC_RETORNADIAFINANCEIRO(PCLANC3.CODFILIAL, 
                                                0, 
                                                PCLANC3.DTVENC, 
                                                1, 
                                                1, 
                                                'PCLANC3'), 'YYYY') AS ANO,
                    (NVL(VALOR, 0) * (-1)) AS VALOR
                FROM PCLANC3 
                WHERE TO_CHAR(FUNC_RETORNADIAFINANCEIRO(PCLANC3.CODFILIAL, 0, PCLANC3.DTVENC, 1, 1, 'PCLANC3'), 'MON') = :mes
                AND TO_CHAR(FUNC_RETORNADIAFINANCEIRO(PCLANC3.CODFILIAL, 0, PCLANC3.DTVENC, 1, 1, 'PCLANC3'), 'YYYY') = :ano 

                AND TO_CHAR(PCLANC3.DTVENC, 'MON') = :mes
                AND TO_CHAR(PCLANC3.DTVENC, 'YYYY') = :ano 

                ORDER BY PCLANC3.DTVENC 

                ),

                PEDIDO_AGRUPADO AS (
                    SELECT 
                        DIA, MES, ANO, SUM(VALOR) AS TOTAL_PEDIDO
                    FROM PEDIDO
                    GROUP BY DIA, MES, ANO
                )



                SELECT 
                    V.DIA, V.MES, V.ANO, COALESCE(V.TOTAL_VALOR, 0) + COALESCE(P.TOTAL_PEDIDO, 0) AS TOTAL_VALOR
                FROM 
                    VALOR V
                LEFT JOIN 
                    PEDIDO_AGRUPADO P
                ON 
                    V.DIA = P.DIA
                    AND V.MES = P.MES
                    AND V.ANO = P.ANO

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

        // Coletar os resultados em um array
        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) !== false) {
            // Convertendo valores para número
            $filiais[] = [
                'DIA' => (int)$row['DIA'],
                'MES' => (int)$row['MES'],
                'ANO' => (int)$row['ANO'],
                'TOTAL_VALOR' => (float)$row['TOTAL_VALOR']
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
});
