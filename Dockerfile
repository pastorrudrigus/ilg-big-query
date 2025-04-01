FROM python:3.9-slim

WORKDIR /app

# Copia os arquivos do projeto
COPY . /app

# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Comando padrão (pode ser usado para teste manual)
CMD ["python", "etl.py"]
