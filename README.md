# Facebook Page Post Cleaner

Aplikasi lokal aman untuk menghapus postingan Facebook Page secara bulk.

## Fitur
- Token hanya hidup di memory server (tidak disimpan ke disk)
- Multi-page selection
- Filter tanggal + limit postingan
- Dry Run mode (simulasi)
- Delay antar request (anti rate limit)
- Penghapusan paralel terbatas (default 3, maksimum 5 permintaan bersamaan)
- Progress log

## Cara Pakai

### 1. Install dependency
```bash
cd fb-page-cleaner
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Jalankan
```bash
python app.py
```
Buka http://127.0.0.1:8000

### 3. Gunakan
1. Paste **User Access Token** atau **System User Token** yang sudah punya permission `pages_manage_posts`.
2. Load Pages → pilih page → Load Posts.
3. Centang postingan yang mau dihapus.
4. Pastikan **Dry Run** aktif dulu untuk tes.
5. Matikan Dry Run lalu hapus.
6. Mulai dengan **Paralel = 3** dan **Delay = 300 ms**. Jika muncul rate limit/error dari Meta, turunkan paralel ke 1–2. Jangan langsung memakai nilai maksimum untuk semua Page.

## Catatan Performa
- Penghapusan sekarang memakai sejumlah kecil permintaan bersamaan, bukan satu per satu.
- Timeout tidak diulang otomatis karena Meta mungkin sudah memproses penghapusan; periksa status postingan sebelum mencoba lagi.
- Kecepatan aktual tetap bergantung pada respons dan rate limit Meta Graph API.

## Catatan Keamanan
- Jalankan hanya di mesin lokal kamu.
- Jangan expose port ini ke internet.
- Token tidak pernah dikirim ke pihak ketiga.
- Setelah selesai, tutup server (token hilang dari memory).

## Permission yang Dibutuhkan
Token harus bisa akses:
- `pages_show_list`
- `pages_read_engagement`
- `pages_manage_posts`

Kalau token dari app Meta Ads kamu sebelumnya sudah punya permission ini, langsung bisa dipakai.
