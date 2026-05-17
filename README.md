# 🌐 Wikipedia Real-Time Data Pipeline

> Materi Pendamping Pelatihan Big Data Lab MCI 2026 · End-to-End Modern Data Stack Implementation

Proyek ini mengekstrak aliran data *real-time* dari **Wikipedia API**, memprosesnya dengan **Apache Spark**, mengorkestrasinya via **Apache Airflow**, menyimpannya ke **ClickHouse**, dan memvisualisasikannya di **Metabase** dan dikemas dalam satu perintah Docker.

Arsitektur mengadopsi prinsip dari *"Mining of Massive Datasets"* (MMDS) dengan pendekatan **micro-batching** setiap 10 menit.

---

## 🏗️ Arsitektur Sistem

```
Wikipedia API
     ↓  (500 edits / 10 menit)
[Ingestion — Python requests]
     ↓  simpan .parquet
[Data Lake — folder lokal]
     ↓  baca & agregasi
[Processing — Apache Spark]
     ↓  truncate-insert
[Data Warehouse — ClickHouse]
     ↓  koneksi langsung
[Dashboard — Metabase]

↻  Seluruh siklus diatur oleh Apache Airflow
```

**Metrik yang dianalisis:**
- **Top 30 Heavy Hitters** — artikel paling sering diedit / sedang edit war
- **Volume Bytes** — total karakter ditambah/dihapus per artikel
- **Bot vs Human Edits** — seberapa dominan bot dalam merawat artikel hari ini


## 🛠️ Tech Stack

| Komponen | Teknologi |
|----------|-----------|
| Orchestration | Apache Airflow 2.9 |
| Processing | Apache Spark / PySpark 3.5 |
| Data Warehouse | ClickHouse (column-oriented OLAP) |
| BI & Dashboard | Metabase |
| Infrastructure | Docker & Docker Compose |
| Language | Python 3.11 |


## 📂 Struktur Proyek

```
wikipedia-realtime-pipeline/
├── dags/
│   ├── scripts/
│   │   ├── fetch_wikipedia_stream.py       #Ekstraksi API → Data Lake
│   │   └── process_wikipedia_spark.py         #PySpark: agregasi & load ke ClickHouse
│   └── wikipedia_pipeline.py        #Definisi DAG Airflow
├── data_lake/                       #Penyimpanan sementara file .parquet
├── docker-compose.yml               #Konfigurasi seluruh service
├── Dockerfile                       #Custom Airflow image (+ Java JRE)
├── requirements.txt                 #Dependensi Python
└── .gitignore
```

---

## 🚀 Tutorial

### Pastikan
- [Docker Desktop](https://docs.docker.com/get-docker/) sudah terinstal
- menggunakan git bash

---

### Step 1 — Buat Struktur Folder

Membuat folder utama 

```bash
mkdir wikipedia-realtime-pipeline
cd wikipedia-realtime-pipeline
```

Membuat folder untuk data mentah

```bash
mkdir -p dags/scripts data_lake
```

Membuat file kosong untuk nanti

```bash
touch docker-compose.yml Dockerfile requirements.txt .gitignore
touch dags/wikipedia_pipeline.py
touch dags/scripts/fetch_wikipedia_stream.py
touch dags/scripts/process_wikipedia_spark.py
```

> `dags/` → dibaca otomatis oleh Airflow  untuk mendefinisikan jadwal dan alur kerja 
> `dags/scripts/` → logika utama kode
> `data_lake/` → penyimpanan sementara `.parquet` hasil ingest

---

### Step 2 — Isi File Konfigurasi & Kode

Isi masing-masing file dengan mengcopas code dari repo ini:

| File | Fungsi |
|------|--------|
| `requirements.txt` | Library Python yang diinstal otomatis (`requests`, `pandas`, dll) |
| `Dockerfile` | Instruksi langkah-langkah merakit container Airflow + dependensi custom |
| `docker-compose.yml` | Mengatur beberapa aplikasi sekaligus agar bisa bekerja dalam satu lingkungan. Urutan nyala service: Postgres → Airflow → ClickHouse |
| `fetch_wikipedia_stream.py` | Tarik 500 data terbaru dari API, simpan sebagai `.parquet` |
| `process_wikipedia_spark.py` | Baca `.parquet` → agregasi → load ke ClickHouse → hapus file mentah |
| `wikipedia_pipeline.py` | DAG Airflow: jadwal & urutan task |

> ⚠️ **Perhatikan**  
> • Di `fetch_wikipedia_stream.py` — ganti bagian `email` dengan email kalian sendiri

---

### Step 3 — Jalankan Docker

Build image. Jangan lupa buka dahulu docker desktop.

```bash
docker-compose build
```

Instalasi database airflow

```bash
docker-compose up airflow-init
```

Jalankan seluruh pipeline

```bash
docker-compose up -d
```

> Tunggu 1–2 menit lalu buka **http://localhost:8080**

---

### Step 4 — Aktifkan Pipeline di Airflow

1. Buka **http://localhost:8080** → login `admin` / `admin`
2. Temukan DAG **`wikipedia_realtime_stream`**, geser sakelar untuk mengaktifkan
3. Klik ▶️ **Trigger DAG** untuk memaksanya jalan sekarang

**Yang terjadi di balik layar:**

```
[Trigger]
    ↓
[Task 1: fetch_wikipedia]  →  API Wikipedia → 500 data → simpan .parquet ✅
    ↓
[Task 2: process_spark]    →  baca .parquet → agregasi → load ClickHouse → hapus .parquet ✅
    ↓
[Menunggu 10 menit berikutnya...]
```

Gambar berikut menunjukkan bahwa di dalam folder data_lake/wikipedia/ sudah muncul sebuah file bernama edits_20260430_040...parquet. Ini adalah bukti fisik bahwa data Wikipedia benar-benar berhasil disedot oleh Python dan tersimpan di laptop. Nantinya akan terhapus otomatis dan dimasukkan ke dalam ClickHouse.

<img width="1680" height="729" alt="Screenshot 2026-04-30 110014" src="https://github.com/user-attachments/assets/882321b1-4cd0-4c97-8832-01144ba016ac" />


Gambar berikut menampilkan tab Graph, yang merupakan representasi visual dari logika urutan code. Terdapat dua kotak yang saling terhubung dengan garis biru. Kotak kiri adalah tugas menyedot data (fetch_recent_changes) dan kotak kanan adalah tugas mengolah data dengan Spark (process_heavy_hitters_spark). Di dalam kedua kotak tersebut terdapat indikator kotak kecil berwarna hijau bertuliskan success. Ini membuktikan bahwa dependency (ketergantungan) dibuat berjalan lancar: Tugas 1 berhasil mencari data, lalu estafet diserahkan ke Tugas 2, dan Tugas 2 berhasil mengolah serta memasukkannya ke ClickHouse.

<img width="806" height="290" alt="Screenshot 2026-04-30 105100" src="https://github.com/user-attachments/assets/10acb628-cd54-4e21-9240-f15b19da98b8" />

Gambar berikut menujukkan bahwa istem melaporkan ringkasan kinerja. Pada bagian "DAG Runs Summary", tertulis angka 6 pada kolom Total success. Dapat diartikan sistem otomatis pencari data (scheduler) sudah terbangun dan menjalankan seluruh tugasnya dari awal sampai akhir sebanyak 6 kali putaran, dan semuanya berhasil tanpa ada error sama sekali. Di panel sebelah kiri (grid view), terlihat 6 tumpukan balok hijau tebal yang merepresentasikan 6 siklus kesuksesan tersebut. Rata-rata waktu yang dibutuhkan untuk menyelesaikan satu siklus penuh hanya 13 detik.

<img width="1487" height="627" alt="Screenshot 2026-04-30 111845" src="https://github.com/user-attachments/assets/727c6909-b759-4836-b08e-670f336ddc8d" />

Gambar berikut terlihat log dari fetch_recent_changes. Terlihat pesan yang dicetak: "INFO - Membuka keran data: API Wikipedia (Recent Changes)..." lalu terdapat "INFO - ✅ Sukses menyimpan 500 baris ke /opt/***/data_lake/wikipedia/edits_20260430_034420.parquet". Di baris bawah, tertulis Command exited with return code 0. Maka eksekusi berhasil 100% terjalankan.

<img width="1919" height="704" alt="Screenshot 2026-04-30 105204" src="https://github.com/user-attachments/assets/3433d50f-661b-4bd2-b6f7-b6cd4cf1b26e" />


---

### Step 5 — Validasi Data di ClickHouse

Masuk ke database ClickHouse

```bash
docker exec -it wikipedia-realtime-pipeline-clickhouse-server-1 clickhouse-client --user admin --password rahasia
```

Masuki database dan jalankan query untuk melihat data

```sql
SHOW DATABASES;
USE analytics;


DESCRIBE analytics.wikipedia_trending;
SELECT COUNT(*) FROM analytics.wikipedia_trending;
```

Melihat 10 artikel wikipedia yang paling banyak diedit dalam siklus tersebut.

```sql
SELECT * FROM wikipedia_trending LIMIT 10;
```

Artikel mana saja yang saat ini sedang dirawat atau diserang oleh bot otomatis

```sql
SELECT title, total_edits, bot_edits
FROM wikipedia_trending
WHERE bot_edits > 0
ORDER BY bot_edits DESC;
```

Keluar dari ClickHouse

```sql
exit
```

---

### Step 6 — Visualisasi di Metabase

1. Buka **http://localhost:3000**, isi data diri (boleh dummy)
2. Di halaman **Add your data**, pilih ClickHouse lalu isi koneksi berikut:

| Field | Value |
|-------|-------|
| Database type | ClickHouse |
| Display name | Data Warehouse Wikipedia |
| Host | `clickhouse-server` |
| Port | `8123` |
| Database name | `analytics` |
| Username | `admin` |
| Password | `rahasia` |

3. Klik **+ New → Question** → pilih **wikipedia_trending** → klik **Visualize**
4. Pilih format grafik (misal: Bar Chart)

---

### Step 7 — Matikan Infrastruktur

```bash
docker-compose down
```

---

## 🔐 Layanan

| Layanan | URL | Username | Password |
|---------|-----|----------|----------|
| Apache Airflow | http://localhost:8080 | `admin` | `admin` |
| Metabase | http://localhost:3000 | *(buat saat setup)* | — |
| ClickHouse TCP | `localhost:9000` | `admin` | `rahasia` |

---

*Dibuat untuk keperluan Pelatihan Data Engineering & Big Data Analytics.*  
