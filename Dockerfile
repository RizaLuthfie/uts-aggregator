# Dockerfile
# ----------
# Image untuk Pub-Sub Log Aggregator
# Base: python:3.11-slim (ringan, sesuai rekomendasi soal)
# Non-root user untuk keamanan

FROM python:3.11-slim

# Set direktori kerja di dalam container
WORKDIR /app

# Buat non-root user dan folder data
RUN adduser --disabled-password --gecos '' appuser \
    && mkdir -p /app/data \
    && chown -R appuser:appuser /app

# Copy requirements dulu (supaya layer ini di-cache Docker)
# Jadi kalau kode berubah tapi requirements tidak, tidak perlu
# install ulang dari awal
COPY requirements.txt ./

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Ganti ke non-root user (keamanan)
USER appuser

# Copy source code
COPY src/ ./src/

# Port yang diekspos ke luar container
EXPOSE 8080

# Environment variables
ENV DEDUP_DB_PATH=/app/data/dedup.db
ENV PORT=8080

# Perintah untuk menjalankan aplikasi
CMD ["python", "-m", "src.main"]