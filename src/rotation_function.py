import boto3
import json
import logging
import random
import string
import psycopg2


logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_client = boto3.client("secretsmanager")


# -------------------- UTILS -------------------- #

def generate_password(length=32):
    """
    Gera uma senha forte aleatória.
    """
    chars = string.ascii_letters + string.digits + "!@#$%^&*()-_=+"
    return "".join(random.choice(chars) for _ in range(length))


def get_secret_dict(secret_id, stage=None, version_id=None):
    """
    Lê o secret e devolve como dict Python.
    Pode buscar por Stage (AWSCURRENT / AWSPENDING) ou VersionId.
    """
    params = {"SecretId": secret_id}
    if version_id:
        params["VersionId"] = version_id
    elif stage:
        params["VersionStage"] = stage

    resp = secrets_client.get_secret_value(**params)
    if "SecretString" not in resp:
        raise ValueError("SecretBinary não é suportado.")
    return json.loads(resp["SecretString"])


def get_db_connection(secret_dict):
    """
    Abre conexão com o Postgres usando psycopg2.
    Espera que o secret tenha:
      - host
      - port
      - username
      - password
      - dbname (opcional, default = postgres)
    """
    return psycopg2.connect(
        host=secret_dict["host"],
        port=secret_dict.get("port", 5432),
        user=secret_dict["username"],
        password=secret_dict["password"],
        dbname=secret_dict.get("dbname", "postgres"),
        connect_timeout=5,
    )


# -------------------- HANDLER PRINCIPAL -------------------- #

def lambda_handler(event, context):
    """
    Evento padrão de rotação do Secrets Manager.

    {
      "Step": "createSecret" | "setSecret" | "testSecret" | "finishSecret",
      "SecretId": "arn:aws:secretsmanager:REGION:ACCOUNT_ID:secret:...",
      "ClientRequestToken": "token"
    }
    """
    logger.info(f"Evento recebido: {json.dumps(event)}")

    step = event["Step"]
    secret_id = event["SecretId"]
    token = event["ClientRequestToken"]

    # Valida se esse Secret está com rotação habilitada
    metadata = secrets_client.describe_secret(SecretId=secret_id)

    if not metadata.get("RotationEnabled"):
        raise ValueError("Rotação não está habilitada para esse secret.")

    # Garante que a versão faz parte das versões do secret
    versions = metadata["VersionIdsToStages"]
    if token not in versions:
        raise ValueError("ClientRequestToken não encontrado em VersionIdsToStages.")

    # Garante que essa versão está marcada como AWSPENDING
    if "AWSPENDING" not in versions[token]:
        raise ValueError("Versão não está no stage AWSPENDING.")

    # Despacha para a função correta
    if step == "createSecret":
        create_secret(secret_id, token)
    elif step == "setSecret":
        set_secret(secret_id, token)
    elif step == "testSecret":
        test_secret(secret_id, token)
    elif step == "finishSecret":
        finish_secret(secret_id, token)
    else:
        raise ValueError(f"Step inválido: {step}")


# -------------------- STEPS DA ROTAÇÃO -------------------- #

def create_secret(secret_id, token):
    """
    Step 1 - createSecret:
    Gera uma nova senha e cria a versão AWSPENDING, se ainda não existir.
    """
    logger.info("Iniciando createSecret...")

    # Se AWSPENDING já existe pra esse token, não faz nada
    try:
        secrets_client.get_secret_value(
            SecretId=secret_id,
            VersionId=token,
            VersionStage="AWSPENDING",
        )
        logger.info("AWSPENDING já existe para esse token. Nada a fazer.")
        return
    except secrets_client.exceptions.ResourceNotFoundException:
        logger.info("AWSPENDING não existe para esse token. Criando nova versão...")

    # Base: secret atual (AWSCURRENT)
    current_dict = get_secret_dict(secret_id, stage="AWSCURRENT")

    if "password" not in current_dict:
        raise ValueError(
            "Campo 'password' não encontrado no AWSCURRENT. "
            "Defina a senha inicial no secret antes de habilitar a rotação."
        )

    # Copia dados de conexão e gera nova senha
    pending_dict = current_dict.copy()
    pending_dict["password"] = generate_password()

    # Grava nova versão como AWSPENDING
    secrets_client.put_secret_value(
        SecretId=secret_id,
        ClientRequestToken=token,
        SecretString=json.dumps(pending_dict),
        VersionStages=["AWSPENDING"],
    )

    logger.info("Nova versão AWSPENDING criada com senha gerada.")


def set_secret(secret_id, token):
    """
    Step 2 - setSecret:
    Aplica a senha AWSPENDING dentro do banco (ALTER USER).
    """
    logger.info("Iniciando setSecret...")

    # Secret pendente (nova senha) - usa VersionId (token)
    pending_dict = get_secret_dict(secret_id, version_id=token)

    # Secret atual (senha velha) - usado para conectar ao banco
    current_dict = get_secret_dict(secret_id, stage="AWSCURRENT")

    if "password" not in current_dict:
        raise ValueError(
            "Campo 'password' não encontrado no AWSCURRENT. "
            "A senha atual precisa estar definida para que a Lambda consiga conectar no RDS."
        )

    conn = get_db_connection(current_dict)
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            username = pending_dict["username"]
            new_pwd = pending_dict["password"]

            # Sintaxe Postgres
            cur.execute(
                f"ALTER USER {username} WITH PASSWORD %s",
                (new_pwd,),
            )
            logger.info(f"Senha atualizada no banco para usuário {username}.")
    finally:
        conn.close()


def test_secret(secret_id, token):
    """
    Step 3 - testSecret:
    Testa se a senha AWSPENDING realmente autentica no banco.
    """
    logger.info("Iniciando testSecret...")

    pending_dict = get_secret_dict(secret_id, version_id=token)

    conn = None
    try:
        conn = get_db_connection(pending_dict)
        logger.info("Conexão com o banco usando AWSPENDING bem-sucedida.")
    finally:
        if conn:
            conn.close()


def finish_secret(secret_id, token):
    """
    Step 4 - finishSecret:
    Promove a versão AWSPENDING para AWSCURRENT.
    """
    logger.info("Iniciando finishSecret...")

    metadata = secrets_client.describe_secret(SecretId=secret_id)
    versions = metadata["VersionIdsToStages"]

    current_version = None
    for version_id, stages in versions.items():
        if "AWSCURRENT" in stages:
            current_version = version_id
            break

    # Se essa versão já é AWSCURRENT, não precisa fazer nada
    if current_version == token:
        logger.info("Versão já está marcada como AWSCURRENT. Nada a fazer.")
        return

    # Move o stage AWSCURRENT da versão antiga para a nova
    secrets_client.update_secret_version_stage(
        SecretId=secret_id,
        VersionStage="AWSCURRENT",
        MoveToVersionId=token,
        RemoveFromVersionId=current_version,
    )

    logger.info(
        f"Stage AWSCURRENT movido da versão {current_version} para a versão {token}."
    )
