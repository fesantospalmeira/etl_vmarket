import os
import logging
import smtplib
from email.message import EmailMessage

def contar_erros_warnings(log_file: str = "app.log") -> tuple[int, int]:
    """
    Conta quantos erros e warnings existem no log.
    Retorna uma tupla: (num_erros, num_warnings)
    """
    erros = 0
    warnings = 0
    if not os.path.exists(log_file):
        print(f"⚠️ O arquivo de log {log_file} não existe.")
        return (0, 0)

    with open(log_file, "r", encoding="utf-8") as f:
        for linha in f:
            if "ERROR" in linha or "❌" in linha:
                erros += 1
            elif "WARNING" in linha or "⚠️" in linha:
                warnings += 1
    return erros, warnings


def send_log_email(
    smtp_server: str,
    smtp_port: int,
    email_user: str,
    email_password: str,
    log_file: str = "app.log"
):
    """
    Envia o arquivo de log por email apenas se houver erros ou warnings,
    incluindo o número de erros e warnings no título.
    """
    if not os.path.exists(log_file):
        print(f"⚠️ O arquivo de log {log_file} não existe.")
        logging.warning(f"⚠️ O arquivo de log {log_file} não existe.")
        return

    erros, warnings = contar_erros_warnings(log_file)
    subject = f"Log do ETL VMarket - Erros: {erros}, Warnings: {warnings}"
    # Só envia email se houver algum erro ou warning
    if erros == 0 and warnings == 0:
        msg = EmailMessage()
        msg["From"] = email_user
        msg["To"] = "felipesantos7938@gmail.com"
        msg["Subject"] = subject
        msg.set_content(f"Segue em anexo o log do ETL VMarket ({log_file}).")

        with open(log_file, "rb") as f:
            file_data = f.read()
            file_name = os.path.basename(log_file)
            msg.add_attachment(file_data, maintype="text", subtype="plain", filename=file_name)

        with smtplib.SMTP_SSL(smtp_server, smtp_port) as smtp:
            smtp.login(email_user, email_password)
            smtp.send_message(msg)

        print("✅ Nenhum erro ou warning encontrado.")
        logging.info("✅ Nenhum erro ou warning encontrado.")
        return

    try:
        TO_EMAILS = os.getenv("TO_EMAIL", "").split(",")
        msg = EmailMessage()
        msg["From"] = email_user
        msg["To"] = ", ".join(TO_EMAILS)
        msg["Subject"] = subject
        msg.set_content(f"Segue em anexo o log do ETL ({log_file}).")

        with open(log_file, "rb") as f:
            file_data = f.read()
            file_name = os.path.basename(log_file)
            msg.add_attachment(file_data, maintype="text", subtype="plain", filename=file_name)

        with smtplib.SMTP_SSL(smtp_server, smtp_port) as smtp:
            smtp.login(email_user, email_password)
            smtp.send_message(msg)

        print(f"✅ Log enviado com sucesso para {', '.join(TO_EMAILS)}")
        logging.info(f"✅ Log enviado com sucesso para {', '.join(TO_EMAILS)}")

    except Exception as e:
        print(f"❌ Falha ao enviar log por email: {e}")
        logging.error(f"❌ Falha ao enviar log por email: {e}", exc_info=True)
