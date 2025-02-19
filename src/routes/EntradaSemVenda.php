<?php

use Slim\Http\Request;
use Slim\Http\Response;
use Symfony\Component\Console\Descriptor\Descriptor;

use Dompdf\Dompdf;
use Dompdf\Options;


$app->group('/api/v1', function () {

    //entrada do dia anterior
    $this->get('/financeiro/entrada/sem/saida/{codfilial}', function (Request $request, Response $response) {
        $settings = $this->get('settings')['db'];
        $dsn = $settings['dsn'];
        $username = $settings['username'];
        $password = $settings['password'];
    
        // Configurações do Telegram
        $telegramApiUrl = 'https://api.telegram.org/bot';
        $telegramToken = '7843895711:AAFyLUOKt797cLX6pxUEeanGP_h40EdtyLQ';
        $chatIds = ['2004371623', '1273984870']; // IDs dos destinatários
    
        // Conexão com o Oracle
        $conexao = oci_connect($username, $password, $dsn);
        if (!$conexao) {
            $e = oci_error();
            $this->logger->error("Erro de conexão ao Oracle: " . $e['message']);
            return $response->withJson(['error' => 'Erro de conexão ao banco de dados'], 500);
        }
    
        $codfilial = $request->getAttribute('codfilial');
        $sql = "SELECT M.DTMOV, M.CODFILIAL, P.CODAUXILIAR, P.CODPROD, P.DESCRICAO, D.DESCRICAO AS DEPARTAMENTO, SUM(M.QT) AS QT
                FROM PCMOV M, PCPRODUT P, PCDEPTO D
                WHERE M.CODPROD = P.CODPROD
                AND M.CODEPTO = D.CODEPTO
                AND TRUNC(M.DTMOV) = TRUNC(SYSDATE -1)
                AND M.CODOPER IN ('E', 'ET')
                AND M.CODFILIAL = :codfilial
                GROUP BY 
                    M.DTMOV, M.CODFILIAL, P.CODAUXILIAR, P.CODPROD, P.DESCRICAO, D.DESCRICAO
                ORDER BY D.DESCRICAO, P.DESCRICAO";
    
        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }
    
        oci_bind_by_name($stmt, ":codfilial", $codfilial);
    
        if (!oci_execute($stmt)) {
            $e = oci_error($stmt);
            $this->logger->error("Erro ao executar a consulta SQL: " . json_encode($e));
            oci_free_statement($stmt);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro ao executar a consulta SQL', 'details' => $e], 500);
        }
    
        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) !== false) {
            $row['CODFILIAL'] = isset($row['CODFILIAL']) ? (int)$row['CODFILIAL'] : null;
            $row['CODAUXILIAR'] = isset($row['CODAUXILIAR']) ? (int)$row['CODAUXILIAR'] : null;
            $row['CODPROD'] = isset($row['CODPROD']) ? (int)$row['CODPROD'] : null;
            $row['QT'] = isset($row['QT']) ? (int)$row['QT'] : null;
            $filiais[] = $row;
        }
    
        oci_free_statement($stmt);
        oci_close($conexao);
    
        if (empty($filiais)) {
            return $response->withJson(['message' => 'Nenhum dado encontrado'], 404);
        }
    
        // Geração do PDF
        $dataEntrada = date('d/m/Y', strtotime($filiais[0]['DTMOV']));
        $html = "<h1>ENTRADAS DO DIA ANTERIOR</h1>";
        $html .= "<p><strong>Data de Entrada:</strong> {$dataEntrada}</p>";
        $html .= "<p><strong>Filial:</strong> {$filiais[0]['CODFILIAL']}</p>";
    
        // Agrupar por departamento
        $departamentos = [];
        foreach ($filiais as $filial) {
            $departamentos[$filial['DEPARTAMENTO']][] = $filial;
        }
    
        foreach ($departamentos as $departamento => $produtos) {
            $html .= "<h2>Departamento: {$departamento}</h2>";
            $html .= '<table border="1" cellspacing="0" cellpadding="5">
                        <tr>
                            <th>Código Auxiliar</th>
                            <th>Código Produto</th>
                            <th>Descrição</th>
                            <th>Quantidade</th>
                        </tr>';
            foreach ($produtos as $produto) {
                $html .= '<tr>
                            <td>' . htmlspecialchars($produto['CODAUXILIAR']) . '</td>
                            <td>' . htmlspecialchars($produto['CODPROD']) . '</td>
                            <td>' . htmlspecialchars($produto['DESCRICAO']) . '</td>
                            <td>' . htmlspecialchars($produto['QT']) . '</td>
                          </tr>';
            }
            $html .= '</table>';
        }
    
        $options = new Options();
        $options->set('defaultFont', 'Courier');
        $dompdf = new Dompdf($options);
        $dompdf->loadHtml($html);
        $dompdf->setPaper('A4', 'landscape');
        $dompdf->render();
    
        $pdfOutput = $dompdf->output();
        $tempFile = tempnam(sys_get_temp_dir(), 'pdf');
        file_put_contents($tempFile, $pdfOutput);
    
        foreach ($chatIds as $chatId) {
            $telegramUrl = "{$telegramApiUrl}{$telegramToken}/sendDocument";
            $data = [
                'chat_id' => $chatId,
                'caption' => "Relatório de Entradas do Dia Anterior - Filial {$filiais[0]['CODFILIAL']} \n Favor repassar a lista para o responsável de cada setor para reposição",
            ];
            $postFields = array_merge($data, ['document' => new CURLFile($tempFile, 'application/pdf', 'relatorio.pdf')]);
    
            $ch = curl_init();
            curl_setopt($ch, CURLOPT_URL, $telegramUrl);
            curl_setopt($ch, CURLOPT_POST, true);
            curl_setopt($ch, CURLOPT_POSTFIELDS, $postFields);
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    
            $result = curl_exec($ch);
            if (curl_errno($ch)) {
                $this->logger->error("Erro ao enviar o PDF para o Telegram: " . curl_error($ch));
            } else {
                $this->logger->info("PDF enviado com sucesso para o chat ID: {$chatId}");
            }
            curl_close($ch);
        }
    
        unlink($tempFile);
    
        return $response->withJson(['message' => 'PDF gerado e enviado para o Telegram com sucesso.']);
    });
    //entrada da semana sem venda
    $this->get('/financeiro/entrada/sem/saida/leo/{codfilial}', function (Request $request, Response $response) {
        $settings = $this->get('settings')['db'];
        $dsn = $settings['dsn'];
        $username = $settings['username'];
        $password = $settings['password'];
    
        // Configurações do Telegram
        $telegramApiUrl = 'https://api.telegram.org/bot';
        $telegramToken = '7843895711:AAFyLUOKt797cLX6pxUEeanGP_h40EdtyLQ';
        $chatIds = ['2004371623', '1273984870']; // IDs dos destinatários
    
        // Conexão com o Oracle
        $conexao = oci_connect($username, $password, $dsn);
        if (!$conexao) {
            $e = oci_error();
            $this->logger->error("Erro de conexão ao Oracle: " . $e['message']);
            return $response->withJson(['error' => 'Erro de conexão ao banco de dados'], 500);
        }
    
        $codfilial = $request->getAttribute('codfilial');
        $sql = "WITH ENTRADA AS (

            SELECT M.DTMOV, M.CODFILIAL, P.CODAUXILIAR, P.CODPROD, P.DESCRICAO, D.DESCRICAO AS DEPARTAMENTO, SUM(M.QT) AS QT
            FROM PCMOV M, PCPRODUT P, PCDEPTO D
            WHERE M.CODPROD = P.CODPROD
            AND M.CODEPTO = D.CODEPTO
            AND M.DTMOV >= SYSDATE -7
            AND M.DTMOV <= SYSDATE -2
            AND M.CODOPER IN ('E', 'ET')
            AND M.CODFILIAL = :codfilial
            AND M.CODEPTO NOT IN (17,115)

            GROUP BY 
                M.DTMOV, M.CODFILIAL, P.CODAUXILIAR, P.CODPROD, P.DESCRICAO, D.DESCRICAO
            ORDER BY D.DESCRICAO, P.DESCRICAO

            ),

            VENDA AS (

            SELECT M.DTMOV, M.CODFILIAL, M.CODPROD FROM PCMOV M
            WHERE M.CODOPER = 'S'
            AND M.CODFILIAL = :codfilial
            AND M.DTMOV >= SYSDATE -7

            )


            SELECT E.DTMOV, E.CODFILIAL, E.CODAUXILIAR, E.CODPROD, E.DESCRICAO, E.DEPARTAMENTO, E.QT
            FROM ENTRADA E
            LEFT JOIN VENDA S ON E.CODPROD = S.CODPROD
            WHERE S.CODPROD IS NULL
            order by E.DTMOV

        ";
    
        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }
    
        oci_bind_by_name($stmt, ":codfilial", $codfilial);
    
        if (!oci_execute($stmt)) {
            $e = oci_error($stmt);
            $this->logger->error("Erro ao executar a consulta SQL: " . json_encode($e));
            oci_free_statement($stmt);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro ao executar a consulta SQL', 'details' => $e], 500);
        }
    
        $filiais = [];
        while (($row = oci_fetch_assoc($stmt)) !== false) {
            $row['CODFILIAL'] = isset($row['CODFILIAL']) ? (int)$row['CODFILIAL'] : null;
            $row['CODAUXILIAR'] = isset($row['CODAUXILIAR']) ? (int)$row['CODAUXILIAR'] : null;
            $row['CODPROD'] = isset($row['CODPROD']) ? (int)$row['CODPROD'] : null;
            $row['QT'] = isset($row['QT']) ? (int)$row['QT'] : null;
            $filiais[] = $row;
        }
    
        oci_free_statement($stmt);
        oci_close($conexao);
    
        if (empty($filiais)) {
            return $response->withJson(['message' => 'Nenhum dado encontrado'], 404);
        }
    
        // Geração do PDF
        $html = "<h1>ENTRADAS DA SEMANA SEM VENDA</h1>";
        $html .= "<h2><strong>Filial:</strong> {$filiais[0]['CODFILIAL']}</h2>";
        
        // Agrupar por departamento
        $departamentos = [];
        foreach ($filiais as $filial) {
            $departamentos[$filial['DEPARTAMENTO']][] = $filial;
        }
        
        // Iterar pelos departamentos
        foreach ($departamentos as $departamento => $produtos) {
            $html .= "<h2>Departamento: {$departamento}</h2>";
        
            // Coletar datas únicas
            $datasUnicas = [];
            foreach ($produtos as $produto) {
                if (!in_array($produto['DTMOV'], $datasUnicas)) {
                    $datasUnicas[] = $produto['DTMOV'];
                }
            }
        
            // Exibir datas únicas
            $html .= "<h3>Datas de Entrada:</h3>";
            $html .= '<ul>';
            foreach ($datasUnicas as $data) {
                $html .= '<li>' . htmlspecialchars($data) . '</li>';
            }
            $html .= '</ul>';
        
            // Tabela de produtos do departamento
            $html .= '<table border="1" cellspacing="0" cellpadding="5">
                        <tr>
                            <th>Código Auxiliar</th>
                            <th>Código Produto</th>
                            <th>Descrição</th>
                            <th>Quantidade</th>
                        </tr>';
            foreach ($produtos as $produto) {
                $html .= '<tr>
                            <td>' . htmlspecialchars($produto['CODAUXILIAR']) . '</td>
                            <td>' . htmlspecialchars($produto['CODPROD']) . '</td>
                            <td>' . htmlspecialchars($produto['DESCRICAO']) . '</td>
                            <td>' . htmlspecialchars($produto['QT']) . '</td>
                          </tr>';
            }
            $html .= '</table>';
        }
        
        
    
        $options = new Options();
        $options->set('defaultFont', 'Courier');
        $dompdf = new Dompdf($options);
        $dompdf->loadHtml($html);
        $dompdf->setPaper('A4', 'landscape');
        $dompdf->render();
    
        $pdfOutput = $dompdf->output();
        $tempFile = tempnam(sys_get_temp_dir(), 'pdf');
        file_put_contents($tempFile, $pdfOutput);
    
        foreach ($chatIds as $chatId) {
            $telegramUrl = "{$telegramApiUrl}{$telegramToken}/sendDocument";
            $data = [
                'chat_id' => $chatId,
                'caption' => "Relatório de Entradas da Semana Sem Venda - Filial {$filiais[0]['CODFILIAL']} \n RELATÓRIO EMITIDO ÀS SEGUNDAS \n Verifique se os Produtos da lista estão na área de Venda ou se realmente são produtos de baixo giro.",
            ];
            $postFields = array_merge($data, ['document' => new CURLFile($tempFile, 'application/pdf', 'relatorio.pdf')]);
    
            $ch = curl_init();
            curl_setopt($ch, CURLOPT_URL, $telegramUrl);
            curl_setopt($ch, CURLOPT_POST, true);
            curl_setopt($ch, CURLOPT_POSTFIELDS, $postFields);
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    
            $result = curl_exec($ch);
            if (curl_errno($ch)) {
                $this->logger->error("Erro ao enviar o PDF para o Telegram: " . curl_error($ch));
            } else {
                $this->logger->info("PDF enviado com sucesso para o chat ID: {$chatId}");
            }
            curl_close($ch);
        }
    
        unlink($tempFile);
    
        return $response->withJson(['message' => 'PDF gerado e enviado para o Telegram com sucesso.']);
    });
    $this->get('/financeiro/sem/entrada', function (Request $request, Response $response) {
        $settings = $this->get('settings')['db'];
        $dsn = $settings['dsn'];
        $username = $settings['username'];
        $password = $settings['password'];
    
        // Configurações do Telegram
        $telegramApiUrl = 'https://api.telegram.org/bot';
        $telegramToken = '7843895711:AAFyLUOKt797cLX6pxUEeanGP_h40EdtyLQ';
    
        // Conexão com o Oracle
        $conexao = oci_connect($username, $password, $dsn);
        if (!$conexao) {
            $e = oci_error();
            $this->logger->error("Erro de conexão ao Oracle: " . $e['message']);
            return $response->withJson(['error' => 'Erro de conexão ao banco de dados'], 500);
        }
    
        // Buscar chatIds do banco de dados
        $sqlChatIds = "
            SELECT LISTAGG(ID, ', ') WITHIN GROUP (ORDER BY ID) AS IDS
            FROM SITEIDUSUNOTI
            WHERE PERMISSAO = 'ENTRADA'
            GROUP BY PERMISSAO
        ";
        $stmtChatIds = oci_parse($conexao, $sqlChatIds);
        if (!$stmtChatIds || !oci_execute($stmtChatIds)) {
            $e = oci_error($stmtChatIds);
            $this->logger->error("Erro ao buscar chat IDs: " . $e['message']);
            return $response->withJson(['error' => 'Erro ao buscar chat IDs'], 500);
        }
    
        $chatIds = [];
        while ($row = oci_fetch_assoc($stmtChatIds)) {
            $chatIds = explode(', ', $row['IDS']);
        }
        oci_free_statement($stmtChatIds);
    
        if (empty($chatIds)) {
            $this->logger->warning("Nenhum chat ID encontrado para notificações.");
            return $response->withJson(['error' => 'Nenhum chat ID encontrado'], 404);
        }
    
        // Novo SQL para buscar dados por data e filial
        $sql = " WITH NOTAS AS (
                SELECT 'N' AS SELECIONADO,
                    PCMANIFDESTINATARIO.CODIGO,
                    TO_NUMBER(SUBSTR(PCMANIFDESTINATARIO.CHAVENFE, 26, 9)) NUMNOTA,
                    SUBSTR(PCMANIFDESTINATARIO.CHAVENFE, 23, 3) SERIE,
                    PCMANIFDESTINATARIO.DATAEMISSAO,
                    NVL(PCMANIFDESTINATARIO.DATAENTRADA, '') AS DATAENTRADA,
                    DECODE(NVL(PCNFENT.CODFILIALNF, PCNFENT.CODFILIAL),
                            PCMANIFDESTINATARIO.CODFILIAL,
                            NVL(PCNFENT.DTENT, ''),
                            '') AS DTENT,
                    PCMANIFDESTINATARIO.NOME AS FORNECEDOR,
                    PCFORNEC.REVENDA,
                    PCFORNEC.CODFORNEC,
                    PCMANIFDESTINATARIO.CODFILIAL,
                    PCMANIFDESTINATARIO.CNPJCPF,       
                    NVL(PCMANIFDESTINATARIO.SITCONFIRMACAODEST, 0) AS SITUACAOMANIF,
                    NVL(PCMANIFDESTINATARIO.SITCONFIRMACAODESTANT, 0) AS SITUACAOMANIFANT,
                    PCMANIFDESTINATARIO.VLTOTALNFE,
                    PCMANIFDESTINATARIO.CHAVENFE,
                    PCMANIFDESTINATARIO.DATARECEBDOCUMENTO,
                    DECODE(PCMANIFDESTINATARIO.SITUACAONFE,
                            1,
                            'USO AUTORIZADO',
                            2,
                            'DENEGADA',
                            3,
                            'CANCELADA') AS SITUACAONFE,
                    DECODE(PCMANIFDESTINATARIO.TIPODOCUMENTO,
                            0,
                            'NF-e',
                            1,
                            'CANCELAMENTO',
                            2,
                            'CC-e') AS TIPODOC,
                    DECODE(PCMANIFDESTINATARIO.AMBIENTE,
                            'H',
                            'HOMOLOGACAO',
                            'P',
                            'PRODUCAO') AS AMBIENTE,
                    PCMANIFDESTINATARIO.JUSTIFICATIVA,
                    NVL(PCNFENT.ESPECIE, 'X') AS ESPECIE,
                    NVL(PCNFENT.NUMTRANSENT, 0) AS NUMTRANSENT

                FROM PCFILIAL, PCFORNEC PCFORNEC2, PCMANIFDESTINATARIO, PCFORNEC, PCNFENT

                WHERE EXISTS(SELECT 1 
                                FROM PCRETCONSMANIFDEST RET
                            WHERE RET.CHAVENFE = PCMANIFDESTINATARIO.CHAVENFE
                                AND RET.CODFILIAL = PCMANIFDESTINATARIO.CODFILIAL) 
                AND PCMANIFDESTINATARIO.CNPJCPF = REGEXP_REPLACE(PCFORNEC.CGC(+), '[^0-9]')
                AND SUBSTR(PCMANIFDESTINATARIO.CHAVENFE, 7, 14) = REGEXP_REPLACE(PCFORNEC.CGC(+), '[^0-9]')
                AND PCMANIFDESTINATARIO.CHAVENFE = PCNFENT.CHAVENFE(+)
                AND PCMANIFDESTINATARIO.DATAEMISSAO = PCNFENT.DTEMISSAO(+)
                AND (PCFORNEC.REVENDA IS NULL OR PCFORNEC.REVENDA = 'S')
                ------------ATENÇÃO COM O CODIGO ABAIXC-----------------                                                                                  
                --subselect abaixo serve para não apresentar notas onde a filial é apenas o transportador da nota, e não o destinatario                   
                AND NVL((SELECT DISTINCT REGEXP_REPLACE(PCNFSAID.CGCFRETE, '[^0-9]')
                            FROM PCNFSAID
                            WHERE PCNFSAID.CHAVENFE = PCMANIFDESTINATARIO.CHAVENFE
                            AND PCNFSAID.ESPECIE = 'NF'
                            AND PCNFSAID.NUMNOTA = TO_NUMBER(SUBSTR(PCMANIFDESTINATARIO.CHAVENFE, 26, 9))),
                        NVL((SELECT DISTINCT REGEXP_REPLACE(FORNEC.CGC, '[^0-9]')
                                FROM PCNFSAID, PCFORNEC FORNEC, PCCLIENT
                                WHERE PCNFSAID.CODFORNECFRETE = FORNEC.CODFORNEC
                                AND PCNFSAID.CODCLI = PCCLIENT.CODCLI
                                AND PCNFSAID.CHAVENFE = PCMANIFDESTINATARIO.CHAVENFE
                                AND PCNFSAID.ESPECIE = 'NF'
                                AND PCNFSAID.NUMNOTA = TO_NUMBER(SUBSTR(PCMANIFDESTINATARIO.CHAVENFE, 26, 9))
                                AND REGEXP_REPLACE(FORNEC.CGC, '[^0-9]') <> REGEXP_REPLACE(PCCLIENT.CGCENT, '[^0-9]')),'X')) <> REGEXP_REPLACE(PCFILIAL.CGC, '[^0-9]')
                            
                ---- Valida notas de entrada onde a filial é transportadora            
                AND NVL((SELECT DISTINCT REGEXP_REPLACE(FORNEC.CGC, '[^0-9]')
                            FROM PCNFENT, PCFORNEC FORNEC
                            WHERE PCNFENT.CODFORNECFRETE = FORNEC.CODFORNEC
                            AND PCNFENT.CHAVENFE = PCMANIFDESTINATARIO.CHAVENFE
                            AND PCNFENT.ESPECIE = 'NF'
                            AND PCNFENT.CODFILIAL <> PCFILIAL.CODIGO
                            AND PCNFENT.NUMNOTA = TO_NUMBER(SUBSTR(PCMANIFDESTINATARIO.CHAVENFE, 26, 9))), 'X') <> REGEXP_REPLACE(PCFILIAL.CGC, '[^0-9]')
                                        
                AND NOME IS NOT NULL
                AND VLTOTALNFE IS NOT NULL
                ----------Se for o Op. Logistico não mostrar notas do destinatário ----------                                                             
                AND NOT EXISTS(SELECT I.CHAVENFE
                        FROM PCCONHECIMENTOFRETEI I, PCNFSAID S
                        WHERE I.CHAVENFE = PCMANIFDESTINATARIO.CHAVENFE
                        AND NVL(s.CODFILIALNF, s.CODFILIAL) = PCMANIFDESTINATARIO.CODFILIAL
                        AND S.NUMTRANSVENDA = I.NUMTRANSCONHEC
                        AND NVL(S.ESPECIE, 'NF') IN ('CO', 'CT', 'CE'))
                ---------------------------------------------------------                                                                                 
                AND (('VALIDO'  = NVL(PCFORNEC.CGC,'VALIDO'))OR (PCFORNEC.CODFORNEC =  (SELECT MAX(PCFORNEC.CODFORNEC) FROM PCFORNEC  WHERE PCMANIFDESTINATARIO.CNPJCPF = REGEXP_REPLACE(PCFORNEC.CGC, '[^0-9]') ))) 
                AND NVL(PCMANIFDESTINATARIO.DATAEMISSAO, PCNFENT.DTEMISSAO) > '01-JAN-2025' 
                AND NVL(PCMANIFDESTINATARIO.DATAEMISSAO, PCNFENT.DTEMISSAO) <= SYSDATE - 3
                AND NVL(PCMANIFDESTINATARIO.SITCONFIRMACAODEST, 0) IN (0,4)
                AND (SUBSTR(PCMANIFDESTINATARIO.CHAVENFE, 7, 14)) <> REGEXP_REPLACE(NVL(PCFORNEC2.CGC, PCFILIAL.CGC), '[^0-9]') 
                AND PCFORNEC2.CODFORNEC = PCFILIAL.CODFORNEC
                AND PCFILIAL.CODIGO = PCMANIFDESTINATARIO.CODFILIAL
                AND NVL(especie, 'X') <> 'OE' 
                ORDER BY NUMNOTA 

                )
                SELECT NT.CODFORNEC, NT.CODFILIAL, NT.NUMNOTA, NT.DATAEMISSAO, NT.FORNECEDOR,  NT.VLTOTALNFE
                FROM NOTAS NT
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM PCLANCPREENT N
                    WHERE N.NUMNOTA = NT.NUMNOTA
                )
                AND NT.DTENT IS NULL
                order by NT.CODFILIAL
        ";
    
        $stmt = oci_parse($conexao, $sql);
        if (!$stmt || !oci_execute($stmt)) {
            $e = oci_error($stmt);
            $this->logger->error("Erro ao executar a consulta SQL: " . json_encode($e));
            oci_free_statement($stmt);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro ao executar a consulta SQL'], 500);
        }
    
        $dados = [];
        while (($row = oci_fetch_assoc($stmt)) !== false) {
            $dataEmissao = $row['DATAEMISSAO'];
            $codFilial = $row['CODFILIAL'];
            $dados[$dataEmissao][$codFilial][] = $row;
        }
    
        oci_free_statement($stmt);
        oci_close($conexao);
    
        if (empty($dados)) {
            return $response->withJson(['message' => 'Nenhum dado encontrado'], 404);
        }
    
        // Geração do PDF
        $html = "<h1>NF SEM PRÉ-ENTRADA</h1>";
    
        foreach ($dados as $dataEmissao => $filiais) {
            $html .= "<h2>Data de Emissão: {$dataEmissao}</h2>";
            foreach ($filiais as $codFilial => $produtos) {
                $html .= "<h3>Filial: {$codFilial}</h3>";
                $html .= '<table border="1" cellspacing="0" cellpadding="5">
                            <tr>
                                <th>Código Fornecedor</th>
                                <th>Número Nota</th>
                                <th>Fornecedor</th>
                                <th>Valor Total NF-e</th>
                            </tr>';
                foreach ($produtos as $produto) {
                    $html .= '<tr>
                                <td>' . htmlspecialchars($produto['CODFORNEC']) . '</td>
                                <td>' . htmlspecialchars($produto['NUMNOTA']) . '</td>
                                <td>' . htmlspecialchars($produto['FORNECEDOR']) . '</td>
                                <td>' . htmlspecialchars(number_format($produto['VLTOTALNFE'], 2, ',', '.')) . '</td>
                              </tr>';
                }
                $html .= '</table>';
            }
        }
    
        $options = new Options();
        $options->set('defaultFont', 'Courier');
        $dompdf = new Dompdf($options);
        $dompdf->loadHtml($html);
        $dompdf->setPaper('A4', 'landscape');
        $dompdf->render();
    
        $pdfOutput = $dompdf->output();
        $tempFile = tempnam(sys_get_temp_dir(), 'pdf');
        file_put_contents($tempFile, $pdfOutput);
    
        foreach ($chatIds as $chatId) {
            $telegramUrl = "{$telegramApiUrl}{$telegramToken}/sendDocument";
            $data = [
                'chat_id' => $chatId,
                'caption' => "Relatório NF sem Pré-Entrada - data: " . date('d/m/Y'),
            ];
            $postFields = array_merge($data, ['document' => new CURLFile($tempFile, 'application/pdf', 'relatorio.pdf')]);
    
            $ch = curl_init();
            curl_setopt($ch, CURLOPT_URL, $telegramUrl);
            curl_setopt($ch, CURLOPT_POST, true);
            curl_setopt($ch, CURLOPT_POSTFIELDS, $postFields);
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    
            $result = curl_exec($ch);
            if (curl_errno($ch)) {
                $this->logger->error("Erro ao enviar o PDF para o Telegram: " . curl_error($ch));
            } else {
                $this->logger->info("PDF enviado com sucesso para o chat ID: {$chatId}");
            }
            curl_close($ch);
        }
    
        unlink($tempFile);
    
        return $response->withJson(['message' => 'PDF gerado e enviado para o Telegram com sucesso.']);
    });



    
    $this->get('/financeiro/permissao/notificacao', function (Request $request, Response $response) {

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
    
        $sql = "SELECT 
                    LISTAGG(ID, ', ') WITHIN GROUP (ORDER BY ID) AS IDS,
                    LISTAGG(CODUSUARIO, ', ') WITHIN GROUP (ORDER BY ID) AS CODUSUARIOS,
                    PERMISSAO
                FROM SITEIDUSUNOTI
                GROUP BY PERMISSAO";
    
        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }
    
        // Executa a consulta
        if (!oci_execute($stmt)) {
            $e = oci_error($stmt);
            $this->logger->error("Erro ao executar a consulta SQL: " . json_encode($e));
            oci_free_statement($stmt);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro ao executar a consulta SQL', 'details' => $e], 500);
        }
    
        // Coletar os resultados
        $rows = [];
        while (($row = oci_fetch_assoc($stmt)) !== false) {
            $rows[] = $row;
        }
    
        // Fechar a conexão
        oci_free_statement($stmt);
        oci_close($conexao);
    
        if (empty($rows)) {
            return $response->withJson(['error' => 'Nenhum dado encontrado'], 404);
        }
    
        // Convertendo resultados para UTF-8
        foreach ($rows as &$row) {
            array_walk_recursive($row, function (&$item) {
                if (!mb_detect_encoding($item, 'utf-8', true)) {
                    $item = utf8_encode($item);
                }
            });
        }
    
        $this->logger->info("Consulta executada com sucesso.");
    
        // Retornar todos os resultados em JSON
        return $response->withJson($rows);
    });

    $this->get('/financeiro/permissao/usuario/{CODUSUARIO}', function (Request $request, Response $response) {

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
    
        // Obter o parâmetro CODUSUARIO
        $codUsuario = $request->getAttribute('CODUSUARIO');
        if (!$codUsuario) {
            return $response->withJson(['error' => 'Parâmetro CODUSUARIO ausente'], 400);
        }
    
        $sql = "SELECT CODUSUARIO, PERMISSAO FROM SITEIDUSUNOTI WHERE CODUSUARIO = :CODUSUARIO";
    
        $stmt = oci_parse($conexao, $sql);
        if (!$stmt) {
            $e = oci_error($conexao);
            $this->logger->error("Erro ao preparar a consulta SQL: " . $e['message']);
            oci_close($conexao);
            return $response->withJson(['error' => 'Erro na preparação da consulta SQL'], 500);
        }
    
        // Bind do parâmetro
        oci_bind_by_name($stmt, ':CODUSUARIO', $codUsuario);
    
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
            array_walk_recursive($row, function (&$item) {
                if (!mb_detect_encoding($item, 'utf-8', true)) {
                    $item = utf8_encode($item);
                }
            });
            $filiais[] = $row;
        }
    
        // Fechar a conexão
        oci_free_statement($stmt);
        oci_close($conexao);
    
        // Verificar se há resultados
        if (empty($filiais)) {
            return $response->withJson(['error' => 'Nenhum dado encontrado para o usuário informado'], 404);
        }
    
        $this->logger->info("Consulta executada com sucesso.");
    
        // Retornar os resultados em JSON
        return $response->withJson($filiais);
    });
    
    $this->post('/financeiro/inserir/permissao', function (Request $request, Response $response) {
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
        $IDTELEGRAM = $params['IDTELEGRAM'] ?? null;
        $CODUSUARIO = $params['CODUSUARIO'] ?? null;
        $PERMISSAO = $params['PERMISSAO'] ?? null;

        if (!$IDTELEGRAM || !$CODUSUARIO || !$PERMISSAO) {
            error_log("Parâmetros inválidos: " . print_r($params, true));
            return $response->withJson(['error' => 'Parâmetros inválidos. Todos os campos são obrigatórios.'], 400);
        }

        $atualizarBanco = "
             INSERT INTO SITEIDUSUNOTI (ID, CODUSUARIO, PERMISSAO)
                VALUES (:IDTELEGRAM, :CODUSUARIO, :PERMISSAO)
        ";

        $bancoSaldo = oci_parse($conexao, $atualizarBanco);
        oci_bind_by_name($bancoSaldo, ":IDTELEGRAM", $IDTELEGRAM);
        oci_bind_by_name($bancoSaldo, ":CODUSUARIO", $CODUSUARIO);
        oci_bind_by_name($bancoSaldo, ":PERMISSAO", $PERMISSAO);

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

    $this->delete('/financeiro/delete/permissao/{CODUSUARIO}/{PERMISSAO}', function (Request $request, Response $response) {
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
    
            // Obtendo os parâmetros da rota
            $CODUSUARIO = $request->getAttribute('CODUSUARIO');
            $PERMISSAO = $request->getAttribute('PERMISSAO');
    
            // Valida se os parâmetros foram enviados
            if (!$CODUSUARIO || !$PERMISSAO) {
                return $response->withJson(['error' => 'Os parâmetros CODUSUARIO e PERMISSAO são obrigatórios.'], 400);
            }
    
            // Comando SQL para deletar o registro
            $deleteQuery = "DELETE FROM SITEIDUSUNOTI WHERE CODUSUARIO = :CODUSUARIO AND PERMISSAO = :PERMISSAO";
    
            // Preparando e executando a consulta
            $statement = oci_parse($conexao, $deleteQuery);
            oci_bind_by_name($statement, ':CODUSUARIO', $CODUSUARIO);
            oci_bind_by_name($statement, ':PERMISSAO', $PERMISSAO);
    
            if (!oci_execute($statement, OCI_COMMIT_ON_SUCCESS)) {
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
                return $response->withJson(['error' => 'Nenhum registro encontrado para os parâmetros informados.'], 404);
            }
        } catch (Exception $e) {
            // Captura de exceções
            return $response->withJson(['error' => 'Erro no servidor.', 'details' => $e->getMessage()], 500);
        }
    });
    
    
});
