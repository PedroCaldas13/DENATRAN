FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY servico.py .

# saída do print aparece na hora no "docker compose logs"
ENV PYTHONUNBUFFERED=1
# banco SQLite fica no volume montado em /dados
ENV DB_DIR=/dados

CMD ["python", "servico.py"]