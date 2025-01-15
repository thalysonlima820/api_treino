<?php
return [
    'settings' => [
        'displayErrorDetails' => true, // set to false in production
        'addContentLengthHeader' => false, // Allow the web server to send the content-length header

        // Renderer settings
        'renderer' => [
            'template_path' => __DIR__ . '/../templates/',
        ],

        // Monolog settings
        'logger' => [
            'name' => 'slim-app',
            'path' => isset($_ENV['docker']) ? 'php://stdout' : __DIR__ . '/../logs/app.log',
            'level' => \Monolog\Logger::DEBUG,
        ],

         // DB settings (Oracle)
         'db' => [
            'dsn' => "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.1.199)(PORT=1521)))(CONNECT_DATA=(SID=WINT)))",
            'username' => 'LIDER',
            'password' => 'LIDER2K18',
        ],

        //secret
        'secretKey' => '8c0374da5a9bedd116ffc145eb5adc91013d4513'


    ],
];
