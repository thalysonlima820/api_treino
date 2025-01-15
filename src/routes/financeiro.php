<?php

use Slim\Http\Request;
use Slim\Http\Response;

use Symfony\Component\Console\Descriptor\Descriptor;

// Rota para listar os dados de PCFILIAL
$app->group('/api/v1', function () {


    $this->get('/financeiro/contas/get/{grupo}', function (Request $request, Response $response, array $args) {
        try {
            // Recupera o grupo da rota
            $grupo = $args['grupo'];
    
            // Validação do parâmetro grupo
            if (empty($grupo)) {
                return $response->withJson(['error' => 'O parâmetro "grupo" é obrigatório.'], 400);
            }
    
            // Configuração do banco de dados
            $settings = $this->get('settings')['db'];
            $dsn = $settings['dsn'];
            $username = $settings['username'];
            $password = $settings['password'];
    
            // Conectando ao Oracle
            $conexao = oci_connect($username, $password, $dsn);
    
            if (!$conexao) {
                $e = oci_error();
                return $response->withJson(['error' => 'Falha na conexão com o banco de dados: ' . $e['message']], 500);
            }
    
            // Comando SQL para consultar os registros
            $consulta = "
                SELECT C.CODCONTA, C.CONTA
                FROM PCCONTA C
                JOIN PCGRUPO G ON G.CODGRUPO = C.GRUPOCONTA
                WHERE G.CODGRUPO = :grupo
            ";
    
            // Preparando a consulta
            $statement = oci_parse($conexao, $consulta);
            if (!$statement) {
                $e = oci_error($conexao);
                oci_close($conexao);
                return $response->withJson(['error' => 'Erro ao preparar a consulta: ' . $e['message']], 500);
            }
    
            // Bind do parâmetro
            oci_bind_by_name($statement, ':grupo', $grupo);
    
            // Executando a consulta
            if (!oci_execute($statement)) {
                $e = oci_error($statement);
                oci_free_statement($statement);
                oci_close($conexao);
                return $response->withJson(['error' => 'Erro ao executar a consulta: ' . $e['message']], 500);
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
            return $response->withJson(['error' => 'Erro inesperado: ' . $e->getMessage()], 500);
        }
    });
    $this->get('/financeiro/grupo/get', function (Request $request, Response $response) {
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
            $consulta = "
                SELECT CODGRUPO, GRUPO FROM PCGRUPO
                WHERE CODGRUPO IN (
                120,
                130,
                140,
                220,
                230,
                240,
                320,
                330,
                340,
                420,
                430,
                440,
                520

                )
            ";
    
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
    
    $this->get('/financeiro/banco/get', function (Request $request, Response $response) {
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
            $consulta = "
                SELECT CODBANCO, NOME FROM PCBANCO
                WHERE TIPOCXBCO IN ('B', 'C')
                ORDER BY CODBANCO
            ";
    
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
    
    $this->get('/financeiro/vale/{dataInicio}/{dataFim}/{codcliente}', function (Request $request, Response $response) {
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
        $dataFim = $request->getAttribute('dataFim');
        $codcliente = (int) $request->getAttribute('codcliente');

        // Consulta SQL com GROUP BY e formato de data correto
        $sql = "SELECT 
                    P.CODFILIALNF AS CODFILIALNF  
                    , P.CODCLI
                    , NT.MATRICULA AS CODCLIENTE_WINTHOR
                    , NT.CHAPA_RM
                    , C.CLIENTE
                    , C.CGCENT
                    , P.DUPLIC
                    , P.VALOR
                    , P.CODCOB
                    , B.COBRANCA
                    , P.DTEMISSAO
                    , TRUNC(P.DTVENC) AS DTVENC
                    , P.DTPAG
                    , C.BLOQUEIO
                    , P.NUMTRANS
                    , P.PREST
                    , P.NUMTRANSVENDA
                    , P.CODSUPERVISOR
                    
                FROM PCPREST P
                    , PCOPERADORACARTAO
                    , PCCOB B
                    , PCCLIENT C
                    , PCUSUARI U
                    , PCSUPERV S
                    , PCCOB COBORIG 
                    , PCREDECLIENTE R 
                    , PCFILIAL F 
                    , PCEMPR NT
                WHERE B.CODCOB = P.CODCOB 
                AND NT.CPF = REPLACE(REPLACE(REPLACE(C.CGCENT, '.', ''), '-', ''), '/', '')
                AND COBORIG.CODCOB(+) = P.CODCOBORIG 
                AND C.CODCLI = P.CODCLI 
                AND P.CODUSUR = U.CODUSUR  
                AND S.CODSUPERVISOR = U.CODSUPERVISOR   
                AND B.CODOPERADORACARTAO = PCOPERADORACARTAO.CODIGO(+)
                AND C.CODREDE = R.CODREDE(+) 
                AND P.CODFILIAL = F.CODIGO 
                AND --Script para retornar apenas registros com permissão rotina 131        
                EXISTS( SELECT 1                                                       
                        FROM PCLIB                                                   
                        WHERE CODTABELA = TO_CHAR(8)                                 
                            AND (CODIGOA = NVL(P.CODCOB, CODIGOA) OR CODIGOA = '9999')                  
                            AND CODFUNC   = 125                                          
                            AND PCLIB.CODIGOA IS NOT NULL)                              
                AND --Script para retornar apenas registros com permissão rotina 131        
                EXISTS( SELECT 1                                                       
                        FROM PCLIB                                                   
                        WHERE CODTABELA = TO_CHAR(1)                                 
                            AND (CODIGOA = NVL(P.CODFILIAL, CODIGOA) OR CODIGOA = '99')                  
                            AND CODFUNC   = 125                                          
                            AND PCLIB.CODIGOA IS NOT NULL)                              
                AND (P.CODFILIAL IN ( '1','2','3','4','5' )) 
                AND P.DTEMISSAO BETWEEN TO_DATE(:dataInicio, 'YYYY-MM-DD') AND TO_DATE(:dataFim, 'YYYY-MM-DD')
                AND (P.CODCLI IN ( :codcliente )) 
                AND P.DTPAG IS NULL  
                AND P.CODCOB NOT IN ('DESD','CRED','DEVT','ESTR', 'CANC') 
                AND P.DTCANCEL IS NULL 
                ORDER BY P.DTVENC, P.CODCLI ";




        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }

        // Associar parâmetros ao placeholder SQL
        oci_bind_by_name($stmt, ":dataInicio", $dataInicio);
        oci_bind_by_name($stmt, ":dataFim", $dataFim);
        oci_bind_by_name($stmt, ":codcliente", $codcliente);

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

    $this->post('/financeiro/vale/update', function (Request $request, Response $response) {

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
        $VALOR = $params['VALOR'] ?? null;
        $CODUSUARIO = $params['CODUSUARIO'] ?? null;
        $PREST = $params['PREST'] ?? null;
        $NUMTRANSVENDA = $params['NUMTRANSVENDA'] ?? null;
        $CODFILIAL = $params['CODFILIAL'] ?? null;
        $DUPLICATA = $params['DUPLICATA'] ?? null;
        $CODCLIENTE = $params['CODCLIENTE'] ?? null;
        $NOMECLIENTE = $params['NOMECLIENTE'] ?? null;
        $NUMDOC = $params['NUMDOC'] ?? null;
        $HORA = $params['HORA'] ?? null;
        $MINUTO = $params['MINUTO'] ?? null;
        $BANCO = $params['BANCO'] ?? null;
        $FILIALUSUARIO = $params['FILIALUSUARIO'] ?? null;

        //tranformar valor em number 
        if ($VALOR !== null) {
            // Substitui vírgula por ponto e converte para número
            $VALOR = (float) str_replace(',', '.', $VALOR);
        }

        // Valida se os parâmetros necessários foram enviados
        if (
            !$VALOR || !$CODUSUARIO || !$PREST || !$NUMTRANSVENDA || !$CODFILIAL
            || !$DUPLICATA || !$CODCLIENTE || !$NOMECLIENTE || !$NUMDOC || !$HORA || !$MINUTO || !$BANCO || !$FILIALUSUARIO
        ) {
            return $response->withJson(['error' => 'Parâmetros inválidos'], 400);
        }

        // Executa o SELECT para obter o saldo do banco
        $consultaSaldo = "SELECT VALOR FROM PCESTCR WHERE CODBANCO = :BANCO AND CODCOB = 'VALE'";
        $stmtSaldo = oci_parse($conexao, $consultaSaldo);
        // Vincula o parâmetro :BANCO

        oci_bind_by_name($stmtSaldo, ":BANCO", $BANCO);

        oci_execute($stmtSaldo);
        $rowSaldo = oci_fetch_assoc($stmtSaldo);

        if (!$rowSaldo) {
            // Se não encontrar o saldo, retorna um erro
            return $response->withJson(['error' => 'Saldo não encontrado'], 500);
        }

        // Atribui o valor do saldo obtido para a variável
        $SALDOBANCO = $rowSaldo['VALOR'] + $VALOR;

        //tranformar valor em number 
        if ($SALDOBANCO !== null) {
            // Substitui vírgula por ponto e converte para número
            $SALDOBANCO = (float) str_replace(',', '.', $SALDOBANCO);
        }


        /////////////////////////////////////////////////////////////////////////

        //pegar num trans
        // Executa o SELECT para obter o saldo do banco
        $consultaNumTrans = "SELECT NVL(PROXNUMTRANS,1) PROXNUMTRANS FROM PCCONSUM FOR UPDATE";
        $stmtNUMTRANS = oci_parse($conexao, $consultaNumTrans);
        oci_execute($stmtNUMTRANS);
        $rowNUMTRANS = oci_fetch_assoc($stmtNUMTRANS);

        if (!$rowNUMTRANS) {
            // Se não encontrar o saldo, retorna um erro
            return $response->withJson(['error' => 'Saldo não encontrado'], 500);
        }

        // Atribui o valor do saldo obtido para a variável
        $NUMTRANS = $rowNUMTRANS['PROXNUMTRANS'];

        /////////////////////////////////////////////////////////////////////////

        //pegar num vale
        // Executa o SELECT para obter o saldo do banco
        $consultaNumVale = "SELECT NVL(PROXNUMVALE,1) PROXNUMVALE FROM PCCONSUM FOR UPDATE ";
        $stmtNUMVALE = oci_parse($conexao, $consultaNumVale);
        oci_execute($stmtNUMVALE);
        $rowNUMVALE = oci_fetch_assoc($stmtNUMVALE);

        if (!$rowNUMVALE) {
            // Se não encontrar o saldo, retorna um erro
            return $response->withJson(['error' => 'Saldo não encontrado'], 500);
        }

        // Atribui o valor do saldo obtido para a variável
        $NUMVALE = $rowNUMVALE['PROXNUMVALE'];

        /////////////////////////////////////////////////////////////////////////

        //pegar num RECNUM
        // Executa o SELECT para obter o saldo do banco
        $consultaRECNUM = "SELECT DFSEQ_PCCORREN.NEXTVAL FROM DUAL";
        $stmtRECNUM = oci_parse($conexao, $consultaRECNUM);
        oci_execute($stmtRECNUM);
        $rowRECNUM = oci_fetch_assoc($stmtRECNUM);

        if (!$rowRECNUM) {
            // Se não encontrar o saldo, retorna um erro
            return $response->withJson(['error' => 'Saldo não encontrado'], 500);
        }

        // Atribui o valor do saldo obtido para a variável
        $RECNUM = $rowRECNUM['NEXTVAL'];

        /////////////////////////////////////////////////////////////////////////

        // desdobrar vale e baixar notinha
        $desdobrarVale = "update PCPREST
                        set
                        CODCOB = 'DBFU',
                        VPAGO = DECODE(:VALOR,0,0,:VALOR),
                        TXPERM = DECODE(0.000000,0,NULL,0.000000),
                        DTPAG = TRUNC(SYSDATE),
                        VALORDESC = 0.000000,
                        PERDESC = 0.000000,
                        VALORMULTA = 0.000000, 
                        VLRDESPBANCARIAS = 0.000000,
                        VLRDESPCARTORAIS = 0.000000,
                        VLROUTROSACRESC = 0.000000,
                        VLRTOTDESPESASEJUROS = 0.000000,
                        DTVENCVALE = TRUNC(SYSDATE),
                        CODHISTVALE = 1.000000,
                        CODFUNCVALE = :CODUSUARIO,
                        DTULTALTER = TRUNC(SYSDATE),
                        CODFUNCULTALTER = 125.000000,
                        DTBAIXA = TRUNC(SYSDATE),
                        CODBAIXA = 125.000000,
                        DTFECHA = DECODE(DTFECHA,NULL,TRUNC(SYSDATE),DTFECHA),
                        CARTORIO = 'N',  
                        PROTESTO = 'N', 
                        OBS2 = Null,
                        OBSTITULO = Null,
                        CODBANCO = :BANCO,
                        CODBARRA = Null,
                        LINHADIG = Null,
                        CODCOBBANCO = 'D',
                        NUMDIASCARENCIA = 0.000000,
                        DATAHORAMINUTOBAIXA = TRUNC(SYSDATE), 
                        CODFUNCFECHA = (CASE WHEN NVL(CODFUNCFECHA,0) = 0 THEN 125.000000 ELSE CODFUNCFECHA END) 
                        where
                        PREST = :PREST and
                        NUMTRANSVENDA = :NUMTRANSVENDA
        ";
        //log desdobramento e baixa
        $logbaixa = "
            INSERT INTO pclogcr (codfilial, duplic, prest, data,
            rotina, codcli, numtransvenda, codfunc)
            VALUES (:CODFILIAL, :DUPLICATA, :PREST, TRUNC(SYSDATE), '1207-2', 
            :CODCLIENTE, :NUMTRANSVENDA, 125.000000) 
        ";
        //atualizar saldo do banco 237
        $atualizarBanco = "UPDATE PCESTCR 
            SET VALOR = :SALDOBANCO,  
                VALORSALDOTOTALCONCIL = 0.000000, 
                VALORCONCILIADO = 0.000000, 
                DTULTCONCILIA = Null, 
                VALORSALDOTOTALCOMP = 0.000000, 
                VALORCOMPENSADO = 0.000000, 
                DTULTCOMPENSACAO = Null 
            WHERE CODCOB = 'VALE' 
            AND CODBANCO = :BANCO
        
        ";
        //BAIXA
        $baixaValePCMOVCR = "INSERT INTO PCMOVCR ( 
                NUMTRANS,
                DATA,              
                CODBANCO,          
                CODCOB,            
                HISTORICO,         
                HISTORICO2,        
                VALOR,             
                TIPO,    
                NUMCARR,
                NUMDOC,    
                VLSALDO, 
                DTCOMPENSACAO,   
                CODFUNCCOMP,   
                HORA,              
                MINUTO,            
                CODFUNC,           
                CODCONTADEB,     
                CODCONTACRED,    
                INDICE,            
                CODROTINALANC      
            )                         
            VALUES(           
                :NUMTRANS,
                SYSDATE,             
                :BANCO,         
                'VALE',           
                'BAIXA REF. TITULO Nro. 348406-1(DBFU)',        
                :NOMECLIENTE,       
                :VALOR,            
                'D',    
                10989.000000,  --ACHAR
                Null,       
                :SALDOBANCO,     --SALDO DO BANCO COM A SOMA DA BAIXA DO VALOR
                Null,  
                Null,  
                :HORA,             
                :MINUTO,           
                125.000000,          
                1007.000000,    
                101.000000,   
                'A',           
                1207.000000     
            ) 
        ";
        //
        $UpdatePCPREST = "UPDATE PCPREST SET
            NUMTRANS = :NUMTRANS
            WHERE
            PREST = :PREST AND
            NUMTRANSVENDA = :NUMTRANSVENDA
        ";
        //lançar no rh rotina 777
        $RmFinal = "INSERT INTO PCCORREN (
                RECNUM,
                CODFILIAL, DTLANC, CODFUNC, HISTORICO, 
                TIPOLANC, VALOR, NUMDOC, CODHIST, HISTORICO2, 
                DTVENC, DTVENCORIG, 
                NUMVALE, 
                TIPOFUNC, CODEMITE, 
                CODFUNCORIG, CODROTINA, CODEMITEORIG, COBJUROS, CODBANCO, 
                NUMTRANS, 
                HORA, MINUTO, DTBAIXAVALE, CODFUNBAIXA, 
                INDICE, NUMTRANSBAIXA, CONSIDERABASECALCULOIMPOSTO, DTDOC, VALEEXPORTADO                                                                                                         
                )
            VALUES (
                :RECNUM,
                :FILIALUSUARIO, TRUNC(SYSDATE), :CODUSUARIO,'P',
                'D', :VALOR, :NUMDOC, 1.000000, 'P',
                TRUNC(SYSDATE), TRUNC(SYSDATE), 
                :NUMVALE,
                'F', 125.000000,
                :CODUSUARIO, 1207.000000, 125.000000, Null, :BANCO,
                :NUMTRANS,
                TO_NUMBER(TO_CHAR(SYSDATE, 'HH24')), TO_NUMBER(TO_CHAR(SYSDATE, 'MI')), Null, Null, 
                'A', Null, 'N', TRUNC(SYSDATE), 'N'       
            ) 
        ";


        // Preparando e executando baixa de nota
        $baixa = oci_parse($conexao, $desdobrarVale);
        oci_bind_by_name($baixa, ":VALOR", $VALOR);
        oci_bind_by_name($baixa, ":CODUSUARIO", $CODUSUARIO);
        oci_bind_by_name($baixa, ":PREST", $PREST);
        oci_bind_by_name($baixa, ":NUMTRANSVENDA", $NUMTRANSVENDA);
        oci_bind_by_name($baixa, ":BANCO", $BANCO);
        $resultbaixa = oci_execute($baixa);

        // Preparando e executando log baixa
        $logbx = oci_parse($conexao, $logbaixa);
        oci_bind_by_name($logbx, ":CODFILIAL", $CODFILIAL);
        oci_bind_by_name($logbx, ":DUPLICATA", $DUPLICATA);
        oci_bind_by_name($logbx, ":PREST", $PREST);
        oci_bind_by_name($logbx, ":CODCLIENTE", $CODCLIENTE);
        oci_bind_by_name($logbx, ":NUMTRANSVENDA", $NUMTRANSVENDA);
        $resultlogbaixa = oci_execute($logbx);

        // Preparando e executando atualizaçao bancaria
        $bancoSaldo = oci_parse($conexao, $atualizarBanco);
        oci_bind_by_name($bancoSaldo, ":SALDOBANCO", $SALDOBANCO);
        oci_bind_by_name($bancoSaldo, ":BANCO", $BANCO);
        $resultbanco = oci_execute($bancoSaldo);
        //baixa
        $baixaTabelaPCMOVCR = oci_parse($conexao, $baixaValePCMOVCR);
        oci_bind_by_name($baixaTabelaPCMOVCR, ":NUMTRANS", $NUMTRANS);
        oci_bind_by_name($baixaTabelaPCMOVCR, ":NOMECLIENTE", $NOMECLIENTE);
        oci_bind_by_name($baixaTabelaPCMOVCR, ":VALOR", $VALOR);
        oci_bind_by_name($baixaTabelaPCMOVCR, ":SALDOBANCO", $SALDOBANCO);
        oci_bind_by_name($baixaTabelaPCMOVCR, ":HORA", $HORA);
        oci_bind_by_name($baixaTabelaPCMOVCR, ":MINUTO", $MINUTO);
        oci_bind_by_name($baixaTabelaPCMOVCR, ":BANCO", $BANCO);
        $resulbaixaPCMOVCR = oci_execute($baixaTabelaPCMOVCR);
        //UpdatePCPREST
        $UmpPCPREST = oci_parse($conexao, $UpdatePCPREST);
        oci_bind_by_name($UmpPCPREST, ":NUMTRANS", $NUMTRANS);
        oci_bind_by_name($UmpPCPREST, ":PREST", $PREST);
        oci_bind_by_name($UmpPCPREST, ":NUMTRANSVENDA", $NUMTRANSVENDA);
        $resulPCPREST = oci_execute($UmpPCPREST);
        // lançar na 777
        $RmFinalRH = oci_parse($conexao, $RmFinal);
        oci_bind_by_name($RmFinalRH, ":RECNUM", $RECNUM);
        oci_bind_by_name($RmFinalRH, ":CODFILIAL", $CODFILIAL);
        oci_bind_by_name($RmFinalRH, ":CODUSUARIO", $CODUSUARIO);
        oci_bind_by_name($RmFinalRH, ":VALOR", $VALOR);
        oci_bind_by_name($RmFinalRH, ":NUMDOC", $NUMDOC);
        oci_bind_by_name($RmFinalRH, ":NUMVALE", $NUMVALE);
        oci_bind_by_name($RmFinalRH, ":NUMTRANS", $NUMTRANS);
        oci_bind_by_name($RmFinalRH, ":BANCO", $BANCO);
        oci_bind_by_name($RmFinalRH, ":FILIALUSUARIO", $FILIALUSUARIO);
        $resulRH = oci_execute($RmFinalRH);


        // Verifica se todos os updates foram bem-sucedidos
        if ($resultbaixa && $resultlogbaixa && $resultbanco && $resulbaixaPCMOVCR && $resulPCPREST && $resulRH) {
            // Se os updates forem bem-sucedidos, executa as atualizações dos numeradores
            $updateNUMTRANS = oci_parse($conexao, "UPDATE PCCONSUM SET PROXNUMTRANS = NVL(PROXNUMTRANS,1) + 1 ");
            oci_execute($updateNUMTRANS);
            oci_free_statement($updateNUMTRANS);

            $updateNUMVALE = oci_parse($conexao, "UPDATE PCCONSUM SET PROXNUMVALE = NVL(PROXNUMVALE,1) + 1 ");
            oci_execute($updateNUMVALE);
            oci_free_statement($updateNUMVALE);

            return $response->withJson(['message' => 'Atualização bem-sucedida'], 200);
        } else {
            // Se houver erro em qualquer um dos updates, retorna uma mensagem de erro
            $e = oci_error();
            return $response->withJson(['error' => $e['message']], 500);
        }

        // Fechar os statements e a conexão
        oci_free_statement($stmtSaldo);
        oci_free_statement($stmtNUMTRANS);
        oci_free_statement($stmtNUMVALE);
        oci_free_statement($stmtRECNUM);
        oci_free_statement($baixa);
        oci_free_statement($logbx);
        oci_free_statement($bancoSaldo);
        oci_free_statement($baixaTabelaPCMOVCR);
        oci_free_statement($UmpPCPREST);
        oci_free_statement($RmFinalRH);


        oci_close($conexao);
    });


    $this->get('/financeiro/get/orcamento', function (Request $request, Response $response) {
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

        // Comando SQL para consultar os registros com STATUS = 'P'
        $consulta = "SELECT * FROM SITERECDESP WHERE STATUS = 'P' ";

        // Preparando e executando o comando de consulta
        $statement = oci_parse($conexao, $consulta);
        $resultado = oci_execute($statement);

        if ($resultado) {
            $dados = [];
            while ($row = oci_fetch_assoc($statement)) {
                $dados[] = $row;
            }

            // Retorna os resultados em formato JSON
            return $response->withJson($dados, 200);
        } else {
            $e = oci_error($statement);
            return $response->withJson(['error' => $e['message']], 500);
        }

        // Liberando os recursos
        oci_free_statement($statement);
        oci_close($conexao);
    });
    $this->post('/financeiro/631/orcamento', function (Request $request, Response $response) {
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
        $VALORNORMAL = $params['VALORNORMAL'] ?? null;
        $CONTA = $params['CONTA'] ?? null;
        $HISTORICO = $params['HISTORICO'] ?? null;
        $BANCO = $params['BANCO'] ?? null;
        $USUARIO = $params['USUARIO'] ?? null;
        $CODFUNC = $params['CODFUNC'] ?? null;
        $MOEDA = $params['MOEDA'] ?? null;
        $STATUS = $params['STATUS'] ?? null;

        // Valida se os parâmetros necessários foram enviados
        if (!$VALORNORMAL || !$CONTA || !$HISTORICO || !$BANCO || !$USUARIO || !$CODFUNC || !$MOEDA || !$STATUS) {
            return $response->withJson(['error' => 'Parâmetros inválidos. Todos os campos são obrigatórios.'], 400);
        }

        // Comando SQL para inserção
        $atualizarBanco = "
            INSERT INTO SITERECDESP (
                VALORNORMAL, CONTA, HISTORICO, BANCO, USUARIO, CODFUNC, MOEDA, STATUS
            ) VALUES (
                :VALORNORMAL, :CONTA, :HISTORICO, :BANCO, :USUARIO, :CODFUNC, :MOEDA, :STATUS
            )
        ";

        // Preparando e executando o comando de inserção
        $bancoSaldo = oci_parse($conexao, $atualizarBanco);
        oci_bind_by_name($bancoSaldo, ":VALORNORMAL", $VALORNORMAL);
        oci_bind_by_name($bancoSaldo, ":CONTA", $CONTA);
        oci_bind_by_name($bancoSaldo, ":HISTORICO", $HISTORICO);
        oci_bind_by_name($bancoSaldo, ":BANCO", $BANCO);
        oci_bind_by_name($bancoSaldo, ":USUARIO", $USUARIO);
        oci_bind_by_name($bancoSaldo, ":CODFUNC", $CODFUNC);
        oci_bind_by_name($bancoSaldo, ":MOEDA", $MOEDA);
        oci_bind_by_name($bancoSaldo, ":STATUS", $STATUS);
        $resultbanco = oci_execute($bancoSaldo);

        // Verifica se a inserção foi bem-sucedida
        if ($resultbanco) {
            return $response->withJson(['message' => 'Inserção bem-sucedida'], 200);
        } else {
            $e = oci_error($bancoSaldo);
            return $response->withJson(['error' => $e['message']], 500);
        }

        oci_free_statement($bancoSaldo);
        oci_close($conexao);
    });
    $this->post('/financeiro/631/aprovar', function (Request $request, Response $response) {
        // Pega a conexão do Oracle configurada
        $settings = $this->get('settings')['db'];
        $dsn = $settings['dsn'];
        $username = $settings['username'];
        $password = $settings['password'];

        // Conectando ao Oracle
        $conexao = oci_connect($username, $password, $dsn);
        if (!$conexao) {
            $e = oci_error();
            return $response->withJson(['error' => 'Erro ao conectar ao banco de dados: ' . $e['message']], 500);
        }

        // Obtém os dados enviados no corpo da requisição POST
        $params = $request->getParsedBody();
        $ID = $params['ID'] ?? null;
        $STATUS = $params['STATUS'] ?? null;

        // Valida se os parâmetros necessários foram enviados
        if (!$ID || !$STATUS) {
            return $response->withJson(['error' => 'Parâmetros inválidos. O ID e o STATUS são obrigatórios.'], 400);
        }

        // Comando SQL para atualização
        $atualizarBanco = "
            UPDATE SITERECDESP 
            SET STATUS = :STATUS
            WHERE ID = :ID
        ";

        try {
            // Preparando e executando o comando de atualização
            $bancoSaldo = oci_parse($conexao, $atualizarBanco);
            oci_bind_by_name($bancoSaldo, ":ID", $ID);
            oci_bind_by_name($bancoSaldo, ":STATUS", $STATUS);

            $resultbanco = oci_execute($bancoSaldo);

            // Verifica se a atualização foi bem-sucedida
            if ($resultbanco) {
                oci_free_statement($bancoSaldo);
                oci_close($conexao);
                return $response->withJson(['message' => 'Atualização bem-sucedida'], 200);
            } else {
                $e = oci_error($bancoSaldo);
                oci_free_statement($bancoSaldo);
                oci_close($conexao);
                return $response->withJson(['error' => 'Erro ao atualizar registro: ' . $e['message']], 500);
            }
        } catch (Exception $e) {
            // Trata exceções
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro inesperado: ' . $e->getMessage()], 500);
        }
    });


    $this->post('/financeiro/631/update', function (Request $request, Response $response) {

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
        // VALOR + OU -
        // VALOR SEM NADA SO VALOR
        $VALORNORMAL = $params['VALORNORMAL'] ?? null;
        $CONTA = $params['CONTA'] ?? null;
        $HISTORICO = $params['HISTORICO'] ?? null;
        $BANCO = $params['BANCO'] ?? null;
        $USUARIO = $params['USUARIO'] ?? null;
        $CODFUNC = $params['CODFUNC'] ?? null;
        $MOEDA = $params['MOEDA'] ?? null;


        //tranformar valor em number 
        if ($VALORNORMAL !== null) {
            // Substitui vírgula por ponto e converte para número
            $VALORNORMAL = (float) str_replace(',', '.', $VALORNORMAL);
        }

        // Valida se os parâmetros necessários foram enviados
        if (
            !$CONTA || !$HISTORICO || !$BANCO || !$USUARIO
            || !$CODFUNC || !$MOEDA || !$VALORNORMAL
        ) {
            return $response->withJson(['error' => 'Parâmetros inválidos'], 400);
        }

        // Executa o SELECT para obter o saldo do banco
        $consultaSaldo = "SELECT VALOR FROM PCESTCR WHERE CODBANCO = :BANCO AND CODCOB = :MOEDA";
        $stmtSaldo = oci_parse($conexao, $consultaSaldo);
        // Vincula o parâmetro :BANCO

        oci_bind_by_name($stmtSaldo, ":BANCO", $BANCO);
        oci_bind_by_name($stmtSaldo, ":MOEDA", $MOEDA);

        oci_execute($stmtSaldo);
        $rowSaldo = oci_fetch_assoc($stmtSaldo);

        if (!$rowSaldo) {
            // Se não encontrar o saldo, retorna um erro
            return $response->withJson(['error' => 'Saldo não encontrado'], 500);
        }

        // Atribui o valor do saldo obtido para a variável
        $SALDOBANCO = $rowSaldo['VALOR'] + ($VALORNORMAL * -1);

        //tranformar valor em number 
        if ($SALDOBANCO !== null) {
            // Substitui vírgula por ponto e converte para número
            $SALDOBANCO = (float) str_replace(',', '.', $SALDOBANCO);
        }

        /////////////////////////////////////////////////////////////////////////

        //pegar num trans
        // Executa o SELECT para obter o saldo do banco
        $consultaNumTrans = "SELECT NVL(PROXNUMTRANS,1) PROXNUMTRANS FROM PCCONSUM FOR UPDATE";
        $stmtNUMTRANS = oci_parse($conexao, $consultaNumTrans);
        oci_execute($stmtNUMTRANS);
        $rowNUMTRANS = oci_fetch_assoc($stmtNUMTRANS);

        if (!$rowNUMTRANS) {
            // Se não encontrar o saldo, retorna um erro
            return $response->withJson(['error' => 'Saldo não encontrado'], 500);
        }

        // Atribui o valor do saldo obtido para a variável
        $NUMTRANS = $rowNUMTRANS['PROXNUMTRANS'];

        /////////////////////////////////////////////////////////////////////////

        //pegar num RECNUM
        // Executa o SELECT para obter o saldo do banco
        $consultaRECNUM = "SELECT DFSEQ_PCCORREN.NEXTVAL FROM DUAL";
        $stmtRECNUM = oci_parse($conexao, $consultaRECNUM);
        oci_execute($stmtRECNUM);
        $rowRECNUM = oci_fetch_assoc($stmtRECNUM);

        if (!$rowRECNUM) {
            // Se não encontrar o saldo, retorna um erro
            return $response->withJson(['error' => 'Saldo não encontrado'], 500);
        }

        // Atribui o valor do saldo obtido para a variável
        $RECNUM = $rowRECNUM['NEXTVAL'];

        /////////////////////////////////////////////////////////////////////////

        // desdobrar vale e baixar notinha
        $desdobrarVale = "
                Insert Into PCLANC(
                NUMBORDERO, 
                DTPAGTO, 
                VPAGO, 
                TIPOLANC, 
                RECNUM, 
                DTLANC, 
                CODCONTA, 
                CODFORNEC, 
                HISTORICO, 
                HISTORICO2, 
                NUMNOTA, 
                VALOR, 
                DTVENC, 
                LOCALIZACAO, 
                CODFILIAL, 
                INDICE, 
                DESCONTOFIN, 
                TXPERM, 
                VALORDEV, 
                NUMBANCO, 
                TIPOPARCEIRO, 
                NOMEFUNC, 
                MOEDA, 
                CODROTINABAIXA, 
                NUMTRANS, 
                DTEMISSAO, 
                RECNUMPRINC, 
                CODFUNCBAIXA, 
                DTASSINATURA, 
                ASSINATURA, 
                DTCHEQ, 
                DTBORDER, 
                FROTA_CODPRACA, 
                FROTA_QTLITROS, 
                FROTA_NUMCAR, 
                FROTA_CODVEICULO, 
                FROTA_COMISSAO, 
                FROTA_DTABASTECE, 
                FROTA_KMABASTECE, 
                NUMCHEQUE, 
                CODPROJETO, 
                DTCOMPETENCIA, 
                FORNECEDOR, 
                NUMNEGOCIACAO, 
                DTAUTOR, 
                VLVARIACAOCAMBIAL, 
                CODMOEDABAIXA
                ) Values (
                Null, 
                TRUNC(SYSDATE), 
                (:VALORNORMAL * -1),  --    RECEITA -  DESPESA +
                'C', 
                :RECNUM,
                TRUNC(SYSDATE), 
                :CONTA,     
                0.000000, 
                :HISTORICO, 
                Null, 
                0.000000, 
                (:VALORNORMAL * -1),   --    RECEITA -  DESPESA +
                TRUNC(SYSDATE), 
                'ROTINA_LANC_631', 
                '1', 
                'A', 
                Null, 
                Null, 
                Null, 
                :BANCO,
                'O', 
                :USUARIO,  
                'R', 
                631.000000, 
                :NUMTRANS,
                TRUNC(SYSDATE), 
                Null, 
                :CODFUNC,  
                Null, 
                Null, 
                Null, 
                Null, 
                0.000000, 
                0.000000, 
                0.000000, 
                Null, 
                Null, 
                Null, 
                0.000000, 
                Null, 
                Null, 
                TRUNC(SYSDATE), 
                Null, 
                Null, 
                Null, 
                Null, 
                :MOEDA  
                ) 
    
        ";
        //log desdobramento e baixa
        $logbaixa = "
            INSERT INTO PCMOVCR( 
            NUMTRANS,          
            DATA,              
            CODBANCO,          
            CODCOB,            
            VALOR,             
            TIPO,              
            HISTORICO,         
            NUMCARR,           
            VLSALDO,           
            HORA,              
            MINUTO,            
            CODFUNC,           
            CODCONTADEB,       
            CODCONTACRED,      
            INDICE,            
            HISTORICO2,        
            OPERACAO,          
            NUMLANC,           
            NUMCARREG,         
            CODROTINALANC      
            ) VALUES (           
            :NUMTRANS,   
            TRUNC(SYSDATE),             
            :BANCO,
            :MOEDA,          
            :VALORNORMAL,
            'D',             
            'A',        
            0.000000,          
            :SALDOBANCO,
            TO_CHAR(SYSDATE, 'HH24'),
            TO_CHAR(SYSDATE, 'MI'),           
            :CODFUNC,          
            :CONTA,   
            0.000000,     
            'A',           
            Null,       
            1.000000,         
            :RECNUM,
            0.000000,        
            631.000000 ) 
        ";
        //atualizar saldo do banco 237
        $atualizarBanco = "
            UPDATE PCESTCR
            SET VALOR = VALOR + :VALORNORMAL 
            WHERE CODCOB = :MOEDA AND CODBANCO = :BANCO
        ";





        // Preparando e executando baixa de nota
        $baixa = oci_parse($conexao, $desdobrarVale);
        oci_bind_by_name($baixa, ":VALORNORMAL", $VALORNORMAL);
        oci_bind_by_name($baixa, ":RECNUM", $RECNUM);
        oci_bind_by_name($baixa, ":CONTA", $CONTA);
        oci_bind_by_name($baixa, ":HISTORICO", $HISTORICO);
        oci_bind_by_name($baixa, ":BANCO", $BANCO);
        oci_bind_by_name($baixa, ":USUARIO", $USUARIO);
        oci_bind_by_name($baixa, ":NUMTRANS", $NUMTRANS);
        oci_bind_by_name($baixa, ":CODFUNC", $CODFUNC);
        oci_bind_by_name($baixa, ":MOEDA", $MOEDA);
        $resultbaixa = oci_execute($baixa);

        // Preparando e executando log baixa
        $logbx = oci_parse($conexao, $logbaixa);
        oci_bind_by_name($logbx, ":NUMTRANS", $NUMTRANS);
        oci_bind_by_name($logbx, ":BANCO", $BANCO);
        oci_bind_by_name($logbx, ":MOEDA", $MOEDA);
        oci_bind_by_name($logbx, ":VALORNORMAL", $VALORNORMAL);
        oci_bind_by_name($logbx, ":SALDOBANCO", $SALDOBANCO);
        oci_bind_by_name($logbx, ":CODFUNC", $CODFUNC);
        oci_bind_by_name($logbx, ":CONTA", $CONTA);
        oci_bind_by_name($logbx, ":RECNUM", $RECNUM);
        $resultlogbaixa = oci_execute($logbx);

        // Preparando e executando atualizaçao bancaria
        $bancoSaldo = oci_parse($conexao, $atualizarBanco);
        oci_bind_by_name($bancoSaldo, ":VALORNORMAL", $VALORNORMAL);
        oci_bind_by_name($bancoSaldo, ":MOEDA", $MOEDA);
        oci_bind_by_name($bancoSaldo, ":BANCO", $BANCO);
        $resultbanco = oci_execute($bancoSaldo);




        // Verifica se todos os updates foram bem-sucedidos
        if ($resultbaixa && $resultlogbaixa && $resultbanco) {
            // Se os updates forem bem-sucedidos, executa as atualizações dos numeradores
            $updateNUMTRANS = oci_parse($conexao, "UPDATE PCCONSUM SET PROXNUMTRANS = NVL(PROXNUMTRANS,1) + 1 ");
            oci_execute($updateNUMTRANS);
            oci_free_statement($updateNUMTRANS);

            return $response->withJson(['message' => 'Atualização bem-sucedida'], 200);
        } else {
            // Se houver erro em qualquer um dos updates, retorna uma mensagem de erro
            $e = oci_error();
            return $response->withJson(['error' => $e['message']], 500);
        }

        // Fechar os statements e a conexão
        oci_free_statement($stmtSaldo);
        oci_free_statement($stmtNUMTRANS);
        oci_free_statement($stmtRECNUM);
        oci_free_statement($baixa);
        oci_free_statement($logbx);
        oci_free_statement($bancoSaldo);
        oci_close($conexao);
    });



    $this->get('/banco/get', function (Request $request, Response $response) {

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



        $sql = " WITH MOVIMENTACAO AS (
            SELECT 
                CODBANCO,
                CODCOB,
                DATA
            FROM (
                SELECT 
                    CODBANCO,
                    CODCOB,
                    DATA,
                    ROW_NUMBER() OVER (PARTITION BY CODBANCO, CODCOB ORDER BY DATA ASC) AS RN
                FROM PCMOVCR
                WHERE DATA > TO_DATE('01-JAN-2024', 'DD-MON-YYYY')
                AND DTCONCIL IS NULL
            ) sub
            WHERE RN = 1

            ),

            BANCOS AS (
            SELECT 
                            H.CODBANCO,
                            B.NOME AS BANCO,
                            H.CODCOB,
                            M.MOEDA,
                            H.VALOR
                            
                        FROM PCESTCR H, PCBANCO B, PCMOEDA M
                        WHERE H.CODBANCO = B.CODBANCO
                        AND H.CODCOB = M.CODMOEDA
                        AND H.VALOR != 0
                        --AND B.CODBANCO NOT IN (14,2,3,4,45)
                        ORDER BY H.CODBANCO

            )

            SELECT 
                B.CODBANCO,
                B.BANCO,
                B.CODCOB,
                B.MOEDA,
                B.VALOR,
                M.DATA
            FROM BANCOS B,  MOVIMENTACAO M
            WHERE B.CODBANCO = M.CODBANCO
            AND B.CODCOB = M.CODCOB
         ";

        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }


        if (!oci_execute($stmt)) {
            $e = oci_error($stmt);
            $this->logger->error("Erro ao executar a consulta SQL: " . $e['message']);
            oci_free_statement($stmt);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro ao executar a consulta SQL'], 500);
        }

        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) !== false) {
            $row['CODBANCO'] = isset($row['CODBANCO']) ? (int)$row['CODBANCO'] : null;
            $row['VALOR'] = isset($row['VALOR']) ? (float)$row['VALOR'] : null;

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
