# IGTI-PA

## Instalação

### Python

1. Baixe o instalador em [python.org](https://www.python.org/downloads/)
2. Execute o instalador e marque "Add Python to PATH"
3. Verifique a instalação:
```bash
python --version
```

### MySQL

1. Baixe em [mysql.com](https://dev.mysql.com/downloads/mysql/)
2. Execute o instalador e siga as instruções
3. Verifique a instalação:
```bash
mysql --version
```

## Configuração de Credenciais

Crie um arquivo `credenciais.json` na raiz do projeto:

```json
{
    "database": {
        "host": "localhost",
        "user": "seu_usuario",
        "password": "sua_senha"
    }
}
```

**IMPORTANTE:** Adicione `credenciais.json` ao `.gitignore` para não expor credenciais.

## Iniciar configuracao do VRA (Voo Regular Ativo)
Execute o script de configuração para inicializar todo o projeto:

```bash
python config_vra.py
```

Este script irá:
- Baixar dados de Voos Regular Ativo da ANAC, aerodromos e de empresas aéreas em arquivos csv
- Normalizar os CSVs baixados e gravar os dados em parquet 
- Criar as databases e tabelas necessárias no mysql