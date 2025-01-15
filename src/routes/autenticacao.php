<?php
use Slim\Http\Request;
use Slim\Http\Response;
use Firebase\JWT\JWT;

// Geração de token
$app->post('/api/token', function ($request, $response) {

    // Pega a conexão do Oracle configurada
    $settings = $this->get('settings')['db'];
    $dsn = $settings['dsn'];
    $username = $settings['username'];
    $password = $settings['password'];

    // Conectando ao Oracle
    $conexao = oci_connect($username, $password, $dsn);

    if (!$conexao) {
        $e = oci_error();
        return $response->withJson(['error' => 'Erro de conexão ao banco de dados'], 500);
    }

    // Obtém os dados enviados no corpo da requisição POST
    $dados = $request->getParsedBody();
    $nome = $dados['nome'] ?? null;
    $senha = $dados['senha'] ?? null;

    // Valida se os parâmetros necessários foram enviados
    if (!$nome || !$senha) {
        return $response->withJson([
            'status' => 'erro',
            'mensagem' => 'Nome ou senha não fornecidos'
        ], 400);
    }

    // Criptografar a senha com MD5 (para verificar se bate com o que está no banco)
    $senhaHash = md5($senha);

    // Verifica se o usuário já existe na tabela SITEUSUARIO com a senha correta
    $sqlSelect = "SELECT * FROM SITEUSUARIO WHERE NOME = :nome AND SENHA = :senha";
    $stmtSelect = oci_parse($conexao, $sqlSelect);
    oci_bind_by_name($stmtSelect, ":nome", $nome);
    oci_bind_by_name($stmtSelect, ":senha", $senhaHash);
    oci_execute($stmtSelect);
    $usuario = oci_fetch_assoc($stmtSelect);
    oci_free_statement($stmtSelect);

    // Se o usuário não for encontrado, retorna erro
    if (!$usuario) {
        return $response->withJson([
            'status' => 'erro',
            'mensagem' => 'Nome ou senha inválidos'
        ], 401);
    }

    // Se o usuário foi encontrado, gerar o token JWT
    $secretKey = $this->get('settings')['secretKey'];
    $issuedAt = time();
    $expirationTime = $issuedAt + 43200 ; // O token expira em 12 horas (43200  segundos)
    $payload = [
        'iat' => $issuedAt, // Hora em que o token foi emitido
        'exp' => $expirationTime, // Expiração do token
        'data' => [
            'nome' => $usuario['NOME'],
            'PERMISSAO' => $usuario['PERMISSAO'],
            'CODUSUARIO' => $usuario['CODUSUARIO']
        ]
    ];

    // Gerar o token JWT
    $chaveAcesso = JWT::encode($payload, $secretKey, 'HS256');

    // Atualizar o campo TOKEN na tabela com o novo token
    $sqlUpdate = "UPDATE SITEUSUARIO SET TOKEN = :token, ULTIMO_LOGIN = TO_CHAR(SYSDATE, 'DD-MM-YYYY HH24:MI:SS') WHERE NOME = :nome";
    $stmtUpdate = oci_parse($conexao, $sqlUpdate);
    oci_bind_by_name($stmtUpdate, ":token", $chaveAcesso);
    oci_bind_by_name($stmtUpdate, ":nome", $nome);

    if (!oci_execute($stmtUpdate)) {
        $e = oci_error($stmtUpdate);
        oci_free_statement($stmtUpdate);
        oci_close($conexao);
        return $response->withJson(['error' => 'Erro ao atualizar o token do usuário: ' . $e['message']], 500);
    }

    oci_free_statement($stmtUpdate);
    oci_close($conexao);

    // Retorna o token gerado, o nome do usuário e o tempo de expiração
    return $response->withJson([
        'nome' => $usuario['NOME'], // Nome do usuário retornado
        'PERMISSAO' => $usuario['PERMISSAO'], 
        'CODUSUARIO' => $usuario['CODUSUARIO'], 
        'chave' => $chaveAcesso,     // Token JWT
        'IDTELEGRAM' => $usuario['IDTELEGRAM'],
        'expira_em' => $expirationTime, // Tempo de expiração como timestamp
        'expira_em_formatado' => date('d-m-Y H:i:s', $expirationTime) // Data e hora de expiração formatada
    ]);
});
