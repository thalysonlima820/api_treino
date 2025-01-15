<?php

use Slim\Http\Request;
use Slim\Http\Response;

use Symfony\Component\Console\Descriptor\Descriptor;

// Rota para listar os dados de PCFILIAL
$app->group('/api/v1', function () {

    $this->get('/concorrente/get', function (Request $request, Response $response) {
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
            $consulta = "SELECT CODCONC, CONCORRENTE 
                        FROM PCCONCOR
                        WHERE ATIVO = 'S'
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

    $this->get('/concorrente/pesquisa/produto', function (Request $request, Response $response) {
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
            $consulta = "SELECT P.CODAUXILIAR AS CODBARRA, P.CODPROD, P.DESCRICAO AS PRODUTO, D.DESCRICAO AS DEPARTAMENTO FROM PCPRODUT P, PCDEPTO D
                WHERE P.CODEPTO = D.CODEPTO
                AND P.REVENDA = 'S'
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

    $this->post('/concorrente/inserir/produto', function (Request $request, Response $response) {
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
        $CODCONC = $params['CODCONC'] ?? null;
        $PUNIT = $params['PUNIT'] ?? null;
        $PUNITATAC = $params['PUNITATAC'] ?? null;
    
        // Valida se os parâmetros necessários foram enviados
        if (!$CODPROD || !$CODCONC || !$PUNIT || !$PUNITATAC) {
            return $response->withJson(['error' => 'Parâmetros inválidos. Todos os campos são obrigatórios.'], 400);
        }
    
        // Obtém o próximo valor da sequência DFSEQ_PCCOTA
        $querySequence = "SELECT DFSEQ_PCCOTA.NEXTVAL AS CODCOTACAO FROM DUAL";
        $sequenceStmt = oci_parse($conexao, $querySequence);
        oci_execute($sequenceStmt);
        $sequenceResult = oci_fetch_assoc($sequenceStmt);
        $CODCOTACAO = $sequenceResult['CODCOTACAO'];
        oci_free_statement($sequenceStmt);
    
        // Comando SQL para inserção
        $atualizarBanco = "INSERT INTO PCCOTA
                    (CODCOTACAO, 
                    CODPROD, 
                    CODCONC,  
                    CODCLI, 
                    PUNIT, 
                    DATA, 
                    CODUSUR, 
                    NUMREGIAO, 
                    CODFILIAL, 
                    PTABELA, 
                    CODPLPAG, 
                    FONTE, 
                    CUSTOFIN, 
                    CUSTOREAL, 
                    PRAZO, 
                    DATADOC, 
                    OBS, 
                    ESTOQUE, 
                    LISTA, 
                    OBS2, 
                    PERCMAXDESCMERCADO,
                    PUNITATAC, 
                    TIPOEMBALAGEMPEDIDO
                    ) VALUES
                    (
                    :CODCOTACAO, 
                    :CODPROD, 
                    :CODCONC, 
                    Null, 
                    :PUNIT, 
                    TRUNC(SYSDATE), 
                    1.000000,
                    1.000000, 
                    Null, 
                    0.000000, 
                    1.000000, 
                    'N', 
                    Null, 
                    Null, 
                    Null, 
                    TRUNC(SYSDATE), 
                    Null, 
                    'S', 
                    9.000000, 
                    Null, 
                    Null, 
                    :PUNITATAC, 
                    Null)";
    
        // Preparando e executando o comando de inserção
        $bancoSaldo = oci_parse($conexao, $atualizarBanco);
        oci_bind_by_name($bancoSaldo, ":CODCOTACAO", $CODCOTACAO);
        oci_bind_by_name($bancoSaldo, ":CODPROD", $CODPROD);
        oci_bind_by_name($bancoSaldo, ":CODCONC", $CODCONC);
        oci_bind_by_name($bancoSaldo, ":PUNIT", $PUNIT);
        oci_bind_by_name($bancoSaldo, ":PUNITATAC", $PUNITATAC);
    
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

    $this->get('/concorrente/preco/produto', function (Request $request, Response $response) {
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
            $consulta = "SELECT C.CODCOTACAO, B.CODPROD, P.DESCRICAO AS PRODUTO, B.PVENDA AS PRECO,  B.PVENDAATAC AS PRECO_ATACADO, C.CODCONC, CC.CONCORRENTE,
                        C.PUNIT AS PRECO_CONCORRENTE, C.PUNITATAC AS PRECO_CONCORRENTE_ATACADO
                        FROM PCPRODUT P, PCEMBALAGEM B, PCCOTA C, PCCONCOR CC
                        WHERE P.CODPROD = B.CODPROD
                        AND P.CODPROD = C.CODPROD
                        AND C.CODCONC = CC.CODCONC
                        AND B.EMBALAGEM = 'UN'
                        AND B.CODFILIAL = 1
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
    

});
