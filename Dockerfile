# Użyj oficjalnego, gotowego obrazu Pythona 3.12 na Debianie Bookworm
FROM python:3.12-slim-bookworm

# Instalacja Javy 17 (OpenJDK) z oficjalnych repozytoriów Debiana
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Ustawienie zmiennych środowiskowych dla Javy
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Ustawienie katalogu roboczego w kontenerze
WORKDIR /app

# Instalacja bibliotek Pythona
RUN pip install --no-cache-dir pyspark==3.5.0 delta-spark==3.1.0 duckdb pandas fastparquet

# Skopiowanie kodu projektu
COPY . /app/

# Komenda startowa
CMD ["python", "run_local.py"]
