FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY cliente.py .

ENV PYTHONUNBUFFERED=1

# sem argumentos abre o menu; com --teste roda o roteiro automático
ENTRYPOINT ["python", "cliente.py"]