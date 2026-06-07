# Judul Project

## Repository Outline
`Bagian ini menjelaskan secara singkat konten/isi dari file yang dipush ke repository`

Contoh:
```
1. description.md - Dokumentasi utama project
2. airflow_ES.yaml - Konfigurasi Docker Compose buat menjalankan PostgreSQL, Airflow, Elasticsearch dan Kibana.
3. `dags/DAG.py` - Airflow ETL pipeline dari PostgreSQL ke Elasticsearch.
4. `dags/database_query.txt` - Pembuatan tabel PostgreSQL  dan script memasukan data mentah. 
5. `dags/data_raw.csv` - Dataset asli Airbnb.
6. `dags/extracted.csv` - Data yang diekstrak sementara dari Postgresql.
7. `dags/data_clean.csv` - Dataset bersih untuk Elasticsearch dan Kibana
8. `dags/GX.ipynb` - Great Expectations validation notebook.
9. `dags/gx/` - Great Expectations suite, checkpoint, validation result, dan data docs.
10. `images/` - Kibana dashboard screenshots dan insights.

```

## Problem Background
Project ini menganalisis data listing Airbnb dari beberapa kota besar, seperti Amsterdam, Bangkok, Barcelona, London, Paris, Rome, dan Sydney.

Dalam skenario bisnis, project ini ditujukan untuk membantu tim ekspansi short-term rental/penyewaan jangka pendek dalam memahami kesiapan pasar di setiap 7 kota besar tersebut. Analisis dilakukan dengan melihat jumlah listing, tipe kamar, tingkat ketersediaan, aktivitas review, kepemilikan lisensi, dan segmentasi host.

Dengan analisis ini, tim bisnis dapat membandingkan kota mana yang memiliki permintaan tinggi, kota mana yang lebih kompetitif, dan kota mana yang memiliki risiko operasional atau regulasi lebih besar.


## Project Output
Output utama dari project ini adalah pipeline ETL otomatis dan dashboard Kibana.

Pipeline dibuat menggunakan Apache Airflow untuk mengambil data dari PostgreSQL, membersihkan data menggunakan Python, menyimpan hasil cleaning ke CSV, lalu mengirim data bersih ke Elasticsearch. Setelah itu, data divisualisasikan menggunakan Kibana dalam bentuk dashboard.

Dashboard Kibana berisi:

- 1 markdown introduction dan objective.
- 6 visualisasi utama.
- Insight bisnis untuk setiap visualisasi.
- Kesimpulan dan rekomendasi lanjutan berdasarkan hasil eksplorasi.
`

## Data
Dataset yang digunakan adalah data listing Airbnb dari tujuh kota besar yang di ambil dari Kaggle.

Data mentah memiliki:

- Jumlah baris: `292,802`
- Jumlah kolom awal: `20`
- Jumlah kolom setelah cleaning: `24`
- Jumlah duplikat setelah cleaning: `0`
- Missing value setelah cleaning: `0`

Kolom tambahan yang dibuat setelah proses cleaning untuk membantu visualisasi dan eksplorasi:

- `has_price`
- `has_license`
- `host_segment`
- `listing_key`

Distribusi jumlah listing per kota:

| Kota | Jumlah Listing |
|---|---:|
| London | 96,871 |
| Paris | 81,853 |
| Rome | 37,652 |
| Bangkok | 28,806 |
| Barcelona | 19,410 |
| Sydney | 17,730 |
| Amsterdam | 10,480 |

Catatan penting: data harga untuk Paris dan Sydney tidak tersedia, jadi rekomendasi terkait pricing hanya dapat digunakan untuk kota yang memiliki data harga.

## Method

1. Data mentah dimasukkan ke PostgreSQL ke dalam tabel `table_m3`.
2. Airflow menjalankan DAG dengan tiga task utama:
   - `fetch_from_postgresql`
   - `data_cleaning`
   - `post_to_elasticsearch`
3. Data diekstrak dari PostgreSQL dan disimpan sebagai file sementara.
4. Data dibersihkan menggunakan Python dan Pandas.
5. Proses cleaning mencakup:
   - Menghapus data duplikat.
   - Menormalisasi nama kolom menjadi lowercase dan snake_case.
   - Mengisi missing value.
   - Mengubah format tanggal.
   - Membuat kolom tambahan untuk analisis.
6. Data bersih divalidasi menggunakan Great Expectations.
7. Data bersih dikirim ke Elasticsearch.
8. Data divisualisasikan menggunakan Kibana.

## Stacks

- Python
- Pandas
- Apache Airflow
- PostgreSQL
- SQLAlchemy
- Great Expectations
- Elasticsearch
- Kibana
- Docker Compose

## Reference

- Url Dataset dari Kaggle("https://www.kaggle.com/datasets/darkmatternet/airbnb-listings-nyc-london-paris-tokyo-and-more")