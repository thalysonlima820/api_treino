<?php

namespace App\Models;
use illuminate\Database\Eloquent\Model;

class Usuario extends Model 
{

    protected $fillable = [
        'nome', 'email', 'senha', 'token', 'updated_at', 'created_at'
    ];


}
