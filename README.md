# 🌱 MangroveChain: Blockchain untuk Konservasi Mangrove Berbasis Data

------

# 1. Pendahuluan

## 1.1 Latar Belakang

**Mangrove** adalah ekosistem hutan pesisir tropis yang tumbuh di kawasan pasang-surut seperti muara sungai, teluk, dan laguna. Vegetasi mangrove memiliki kemampuan unik dalam menstabilkan garis pantai, menyaring polutan, serta menjadi tempat berkembang biak bagi berbagai spesies ikan, kepiting, burung, dan biota laut lainnya. Lebih dari itu, mangrove berperan penting sebagai penyerap karbon biru (*blue carbon*) yang sangat efektif dalam mitigasi perubahan iklim.

Indonesia merupakan negara dengan ekosistem mangrove terluas di dunia, mencakup lebih dari **3,3 juta hektare** atau sekitar **23% dari total luas mangrove global**. Spesies utama seperti *Rhizophora spp.*, *Avicennia spp.*, dan *Sonneratia spp.* tidak hanya menopang biodiversitas tinggi, tetapi juga menyediakan sumber daya ekonomi bagi jutaan masyarakat pesisir. Indonesia memiliki ekosistem mangrove terbesar di dunia, mencakup lebih dari 3,3 juta hektar hutan pesisir yang tersebar dari Aceh hingga Papua. Ekosistem ini memainkan peran vital dalam penyerapan emisi karbon, perlindungan garis pantai dari abrasi dan tsunami, penyediaan habitat bagi berbagai spesies, serta sebagai sumber penghidupan bagi masyarakat pesisir. Namun demikian, lebih dari separuh hutan mangrove di Indonesia telah mengalami degradasi akibat berbagai tekanan, termasuk alih fungsi lahan, reklamasi pesisir, pencemaran air, serta lemahnya pengawasan terhadap pelaksanaan proyek restorasi. Salah satu tantangan besar dalam konservasi mangrove adalah minimnya integrasi data antarwilayah, keterbatasan sistem pelaporan yang transparan, dan sulitnya memverifikasi dampak ekologis serta sosial dari proyek konservasi secara objektif.

Untuk menjawab tantangan tersebut, MangroveChain dikembangkan sebagai sebuah sistem inovatif yang menggabungkan kekuatan teknologi blockchain dengan platform analitik konservasi berbasis data. Sistem ini dirancang untuk meningkatkan transparansi, akuntabilitas, dan efisiensi dalam pengelolaan proyek konservasi mangrove secara nasional. Dengan memanfaatkan ledger blockchain yang tidak dapat diubah (immutable ledger), MangroveChain mencatat setiap transaksi kredit karbon, status kepatuhan terhadap regulasi, aktivitas komunitas, dan hasil pemantauan biodiversitas di lapangan. Platform ini saat ini menganalisis lebih dari 30 proyek konservasi aktif dari berbagai wilayah di Indonesia, mencakup dimensi spasial, sosial, ekologis, dan ekonomi.

MangroveChain tidak hanya menampilkan visualisasi data konservasi, tetapi juga mendukung pengambilan keputusan berbasis bukti melalui integrasi model statistik dan pembelajaran mesin, seperti ARIMA untuk peramalan partisipasi komunitas dan XGBoost untuk mengklasifikasikan daya tarik proyek dalam kerangka keuangan hijau. Di samping itu, platform ini menyediakan indikator yang komprehensif seperti jumlah spesies (species count), kepadatan pohon (tree density), kualitas air, dan keterlibatan masyarakat, yang diolah dari dataset terstandarisasi. Setiap proyek dianalisis dari sisi kelengkapan izin, kejelasan batas lahan, serta performa dalam menurunkan emisi karbon yang dikonversi ke dalam kredit karbon, sehingga memungkinkan skema perdagangan karbon yang terverifikasi secara digital.

Melalui pendekatan ini, MangroveChain bertujuan menjadi katalis transformasi dalam tata kelola konservasi mangrove yang lebih terbuka, kolaboratif, dan terdesentralisasi. Sistem ini juga diharapkan dapat memperkuat kepercayaan donor, lembaga internasional, dan pemerintah daerah terhadap efektivitas proyek yang mereka danai, sekaligus membuka akses pembiayaan inovatif melalui skema carbon offset, green bonds, dan tokenisasi aset karbon di masa depan. Dengan menjembatani teknologi dan ekologi, MangroveChain mendorong Indonesia menuju masa depan konservasi yang lebih adil, berbasis data, dan berkelanjutan.

---

## 1.2 Identifikasi Masalah 

1. Terdapat inkonsistensi hasil konservasi biodiversitas antar proyek mangrove di Indonesia, yang diduga berkaitan dengan perbedaan status regulasi, kepemilikan lahan, dan kejelasan batas hukum wilayah konservasi.
   
2. Meningkatnya kebutuhan investor terhadap transparansi, keamanan data, dan bukti dampak lingkungan menghadirkan tantangan dalam memastikan alokasi dana konservasi mangrove benar-benar mendukung proyek yang efisien, kredibel, dan memenuhi prinsip-prinsip tata kelola berbasis blockchain.
   
3. Alokasi sumber daya proyek konservasi mangrove belum sepenuhnya mempertimbangkan keterlibatan masyarakat sebagai indikator awal keberlanjutan, meskipun data historis menunjukkan hubungan kuat antara partisipasi komunitas dan keberhasilan jangka panjang proyek.

4. Banyak proyek konservasi mangrove di Indonesia beroperasi di wilayah dengan status izin yang tidak pasti, batas lahan yang belum didefinisikan secara hukum, dan konflik kepemilikan tanah adat, yang secara signifikan meningkatkan potensi sengketa hukum dan menghambat keberlanjutan program konservasi.

5. Arsitektur data blockchain dalam proyek konservasi mangrove belum sepenuhnya dioptimalkan, mengakibatkan aliran informasi yang tidak efisien, keterbatasan akses data antar pihak, serta rendahnya volume transaksi karbon pada wilayah dengan keterbukaan data yang terbatas.


## 1.3 Rumusan Masalah

1. Bagaimana efektivitas regulasi konservasi — termasuk status persetujuan izin, kepemilikan lahan, dan batas hukum — mempengaruhi keberhasilan ekologis proyek mangrove seperti kualitas air, kerapatan pohon, dan keanekaragaman spesies?
  
2. Bagaimana mengidentifikasi proyek konservasi mangrove yang optimal dalam hal efisiensi penyerapan karbon, kepatuhan terhadap tata kelola data berbasis blockchain, dan adanya persetujuan masyarakat, guna menjamin akuntabilitas penggunaan dana investasi hijau?
   
3. Bagaimana membangun model prediktif berbasis data keterlibatan masyarakat dalam enam bulan pertama untuk memproyeksikan keberlanjutan dan kinerja ekologis proyek konservasi mangrove?

4. Bagaimana membangun sistem klasifikasi risiko hukum berbasis data multidimensi untuk mengidentifikasi proyek konservasi mangrove yang rentan terhadap konflik hukum secara lebih proaktif dan terukur?

5. Bagaimana menganalisis pola jaringan data blockchain dan hubungan antar stakeholder untuk merancang sistem berbagi informasi yang efisien, adil, dan mendorong peningkatan transaksi karbon melalui desain smart contract yang tepat?

---

## 1.4 Tujuan

1. Menganalisis hubungan antara efektivitas regulasi dan kepemilikan lahan terhadap dampak biodiversitas proyek konservasi.
2. Mengoptimalkan alokasi pendanaan ke proyek mangrove berbasis blockchain yang memenuhi standar dampak dan integritas data.
3. Memprediksi keberlanjutan proyek berdasarkan pola keterlibatan masyarakat di tahap awal pelaksanaan.
4. Mengidentifikasi dan memetakan proyek berisiko hukum tinggi melalui analisis multi-dimensi dan spasial.
5. Memahami pola aliran dan distribusi data blockchain untuk mengoptimalkan arsitektur sistem konservasi.

---

## 1.5 Manfaat

### 1.Bagi Eco-Techno Leaders
- Akses ke sistem monitoring real-time berbasis blockchain.
- Pemahaman teknis tentang desain data dan smart contract dalam proyek konservasi.
- Model prediksi berbasis keterlibatan sosial dan data ekologis.

### 2.Bagi Pemerintah
- Penguatan sistem evaluasi berbasis data untuk kebijakan lingkungan.
- Dukungan teknis untuk pencapaian target NDC dan program karbon nasional.
- Sistem klasifikasi risiko proyek untuk intervensi kebijakan yang tepat sasaran.

### 3.Bagi Investor Hijau
- Verifikasi digital atas dampak karbon dan transparansi dana.
- Identifikasi proyek dengan performa tertinggi dan risiko terendah.
- Jaminan keamanan data dan partisipasi komunitas lokal yang terdokumentasi.

-----

## 2.Pembahasan

### **2.1 Analisis Efektivitas Regulasi & Dampak Biodiversitas**

Analisis ini bertujuan untuk menjawab pertanyaan kunci mengenai hubungan antara kerangka regulasi dan hasil ekologis. Alur kerja ini dimulai dari pengambilan dan agregasi data menggunakan SQL di database PostgreSQL, yang kemudian diekspor untuk dianalisis lebih lanjut menggunakan Python dan divisualisasikan dalam bentuk heatmap.

#### **1. Pengambilan Data dan Wawasan Awal dari Database (PostgreSQL)**

Data untuk analisis ini bersumber dari database PostgreSQL yang mengintegrasikan data dari beberapa tabel relasional.

**a. SQL Query untuk Ekstraksi Data Analisis**

Query berikut digunakan untuk menggabungkan data dari tabel `regulatory_permits`, `land_tenure_records`, dan `biodiversity_monitoring`. Hasil dari query ini diekspor menjadi file `conservation_db.csv` yang kemudian digunakan oleh skrip Python untuk analisis korelasi. Query ini juga melakukan transformasi pada kolom `kualitas_air` dari teks menjadi nilai numerik untuk analisis statistik.

```sql
-- Query ini mengambil data mentah yang telah digabungkan untuk analisis di Python
SELECT
    rp.permit_status,
    ltr.land_type,
    ltr.boundary_defined,
    bm.species_count,
    bm.tree_density,
    CASE
       WHEN bm.water_quality = 'Poor' THEN 1
       WHEN bm.water_quality = 'Moderate' THEN 2
       WHEN bm.water_quality = 'Good' THEN 3
    ELSE 0
    END AS kualitas_air
FROM regulatory_permits rp
JOIN land_tenure_records ltr ON rp.conservation_id = ltr.conservation_id
JOIN biodiversity_monitoring bm ON rp.conservation_id = bm.conservation_id;
```

**b. SQL Query untuk Wawasan Awal (Agregasi)**

Sebelum melakukan analisis korelasi mendalam, query agregasi sederhana dapat memberikan wawasan tingkat tinggi secara langsung dari database untuk menguji hipotesis awal.

```sql
-- Query ini memberikan ringkasan dampak berdasarkan status izin
SELECT 
    rp.permit_status AS Status_Izin,
    AVG (CASE
        WHEN bm.water_quality = 'Poor' THEN 1
        WHEN bm.water_quality = 'Moderate' THEN 2
        WHEN bm.water_quality = 'Good' THEN 3
    ELSE 0
    END) AS Rataan_Kualitas_Air,
    AVG(bm.tree_density) AS Rataan_Kerapatan_Pohon,
    COUNT(*) AS Jumlah_Proyek
FROM regulatory_permits rp
JOIN land_tenure_records ltr ON rp.conservation_id = ltr.conservation_id
JOIN biodiversity_monitoring bm ON rp.conservation_id = bm.conservation_id
GROUP BY rp.permit_status
ORDER BY Rataan_Kerapatan_Pohon DESC;
```

**Hasil Query Agregasi (Contoh):**

| Status_Izin | Rataan_Kualitas_Air | Rataan_Kerapatan_Pohon | Jumlah_Proyek |
|:---|:---:|:---:|:---:|
| Approved | 2.10 | 1255.5 | 20 |
| Pending | 2.35 | 875.0 | 10 |

Hasil agregasi ini memberikan indikasi awal bahwa proyek dengan status izin **'Approved'** memiliki **rataan kerapatan pohon yang jauh lebih tinggi** dibandingkan yang berstatus **'Pending'**, yang mendukung hipotesis awal.

#### **2. Analisis Korelasi Mendalam (Python)**

Data yang diekspor dari SQL (`conservation_db.csv`) selanjutnya dianalisis menggunakan Python untuk memvisualisasikan korelasi secara lebih detail.

**a. Visualisasi Matriks Korelasi (Fokus & Penuh)**

*   **Heatmap :** Menunjukkan hubungan langsung antara faktor regulasi dan metrik biodiversitas dan Memberikan konteks lengkap dengan menunjukkan semua hubungan antar variabel.

![filtered_mangrove_correlation_heatmap](https://github.com/Asfa-Asfialana/MangroveChain-Conservation-Analytics/blob/main/Studi%20Kasus%201/filtered_mangrove_correlation_heatmap.png)

**b. Kode Analisis Python (studikasus1fix.ipynb)**

[**Klik disini**](https://github.com/Asfa-Asfialana/MangroveChain-Conservation-Analytics/blob/main/Studi%20Kasus%201/Studi%20Kasus%201.sql)

#### **3. Kesimpulan Akhir Analisis**

Dengan menggabungkan wawasan dari agregasi SQL dan analisis korelasi Python, kesimpulan yang lebih kuat dapat ditarik:

*   **Bukti Terkonfirmasi:** Baik agregasi SQL awal maupun analisis korelasi mendalam menunjukkan bahwa **status izin yang disetujui (`Approved`)** secara konsisten berhubungan dengan **kerapatan pohon (`tree_density`) yang lebih tinggi**.
*   **Kompleksitas Terungkap:** Kualitas air (`water_quality`) menunjukkan hubungan yang lebih lemah dengan faktor regulasi, seperti yang terlihat pada koefisien korelasi yang rendah di heatmap. Ini memperkuat dugaan bahwa ada faktor eksternal lain (misalnya polusi dari luar area proyek) yang lebih signifikan mempengaruhinya, sebuah wawasan yang sulit didapat tanpa analisis data multi-dimensi.
*   **Pentingnya Batas Lahan:** Faktor kejelasan batas (`boundary_defined`) secara konsisten menunjukkan korelasi positif kecil di semua metrik, menandakan perannya sebagai fondasi penting untuk keberhasilan ekologis.

---

### **2.2 Optimalisasi Pendanaan Berbasis Blockchain**

Analisis ini dirancang untuk memenuhi kebutuhan konsorsium investor hijau yang menuntut verifikasi dampak, integritas data, dan transparansi sebelum mengalokasikan dana sebesar $15 juta. Alur kerja ini menggunakan serangkaian query SQL untuk mengkuantifikasi kinerja, memverifikasi kepatuhan, dan mengidentifikasi proyek berkinerja terbaik, yang hasilnya kemudian divisualisasikan untuk presentasi yang jelas.

#### **1. Pengambilan dan Analisis Data dari Database (PostgreSQL)**

Database menjadi sumber utama untuk menjawab setiap kriteria yang ditetapkan oleh investor.

**a. Kuantifikasi Penyerapan CO2 per Rupiah Investasi**

Query pertama ini menghitung efisiensi penyerapan karbon untuk setiap proyek dengan metrik **"CO2 per Juta Rupiah"**. Ini adalah KPI inti untuk investor dampak.

```sql
-- Query untuk mengukur efisiensi karbon per investasi
SELECT 
    fs.conservation_id,	
    fs.source_name,
    fs.amount_idr,
    ei.co2_sequestration_tonnes,
    -- Menghitung penyerapan CO2 (ton) per juta Rupiah
    ei.co2_sequestration_tonnes / (fs.amount_idr / 1000000) AS co2_per_juta_rupiah
FROM funding_sources fs
JOIN environmental_impact ei ON fs.conservation_id = ei.conservation_id
WHERE ei.impact_type = 'Carbon Storage'
ORDER BY co2_per_juta_rupiah DESC;
```

**b. Verifikasi Kepatuhan Tata Kelola Data Blockchain**

Query kedua ini digunakan untuk mengaudit kepatuhan tata kelola data. Query ini menghitung persentase catatan yang memenuhi standar keamanan (`encryption_level = 'High'`) dan persetujuan masyarakat (`consent_obtained = 'Yes'`).

```sql
-- Query untuk mengukur persentase kepatuhan data
WITH compliance_stats AS (
    SELECT 
        conservation_id,
        COUNT(*) AS total_records,
        SUM(CASE
            WHEN encryption_level = 'High' AND consent_obtained = 'Yes' THEN 1
            ELSE 0
            END) AS compliant_records
    FROM blockchain_data_compliance 
    GROUP BY conservation_id    
)
SELECT
    conservation_id,
    total_records,
    compliant_records,
    (compliant_records::float / total_records) * 100 as compliance_percentage
FROM compliance_stats
ORDER BY compliance_percentage DESC;
```

**c. Identifikasi Proyek Berkinerja Terbaik (Query Gabungan)**

Query final ini menggabungkan semua kriteria investor untuk menyaring dan mengidentifikasi proyek-proyek yang paling layak mendapatkan pendanaan. Hasil dari query inilah yang diekspor menjadi `2. Query 3.csv` untuk visualisasi.

```sql
-- Query final untuk mengidentifikasi proyek terbaik berdasarkan semua kriteria
SELECT 
    fs.conservation_id,
    fs.source_name,
    fs.amount_idr,
    ei.co2_sequestration_tonnes,
    ei.co2_sequestration_tonnes / (fs.amount_idr / 1000000) AS co2_per_juta_rupiah,
    bdc.encryption_level,
    bdc.consent_obtained
FROM funding_sources fs
JOIN environmental_impact ei ON fs.conservation_id = ei.conservation_id
JOIN blockchain_data_compliance bdc ON fs.conservation_id = bdc.conservation_id
WHERE 
    ei.impact_type = 'Carbon Storage'
    AND bdc.encryption_level = 'High'
    AND bdc.consent_obtained = 'Yes'
    AND fs.amount_idr > 50000000 -- Filter tambahan untuk proyek signifikan
ORDER BY co2_per_juta_rupiah DESC, fs.amount_idr DESC;
```

#### **2. Visualisasi dan Laporan Analisis (Python)**

Data yang dihasilkan dari query final di atas kemudian divisualisasikan menggunakan Python untuk memudahkan pemahaman.

**a. Visualisasi Scatter Plot 3D**

Visualisasi ini menampilkan hubungan tiga dimensi antara tingkat partisipasi masyarakat, luas area konservasi, dan jumlah kredit karbon yang dihasilkan oleh masing-masing proyek. Tujuannya adalah untuk mengidentifikasi pola proyek yang efisien, di mana keterlibatan komunitas tinggi menghasilkan dampak karbon yang signifikan di area konservasi yang luas.

![efisiensi_pendanaan](https://github.com/Asfa-Asfialana/MangroveChain-Conservation-Analytics/blob/main/Studi%20Kasus%202/efisiensi_pendanaan.png)

**b. Kode Analisis Python (Test.ipynb)**

Notebook ini berisi rangkaian kode Python yang digunakan untuk melakukan:

- Pembersihan dan penggabungan data dari beberapa sumber,
- Transformasi variabel seperti skala logaritmik,
- Pembuatan scatter plot 3D dengan Matplotlib dan seaborn,
- Simpan visualisasi dalam format PNG dan interaktif.

Kode ini menjadi fondasi utama untuk analisis data eksploratif dalam proyek MangroveChain.
*[Letakkan blok kode Python di sini](https://github.com/Asfa-Asfialana/MangroveChain-Conservation-Analytics/blob/main/Studi%20Kasus%202/studi_kasus2.ipynb)*

**c. Data yang Digunakan**

Dataset ini merupakan hasil akhir dari query SQL yang menggabungkan beberapa tabel seperti community_engagement, carbon_credits, dan project_metadata. Data mencakup kolom-kolom berikut:

- conservation_id
- source_name
- amount_idr
- co2_sequestration_tonnes
- co2_per_juta_rupiah
- encryption_level
- consent_obtained

Dataset ini digunakan sebagai input utama dalam visualisasi dan model prediktif berbasis keterlibatan masyarakat.

[📌 Klik di sini untuk mengunduh dataset](https://github.com/Asfa-Asfialana/MangroveChain-Conservation-Analytics/blob/main/Studi%20Kasus%202/efisiensi_pendanaan.csv
)

#### **3. Kesimpulan Akhir Analisis**

Dengan menggabungkan analisis SQL dan visualisasi Python, laporan komprehensif dapat disajikan kepada investor:

*   **Efisiensi Terbukti:** Analisis SQL dan visualisasi mengonfirmasi bahwa proyek-proyek yang dianalisis memiliki **efisiensi karbon yang konsisten dan tinggi**, yaitu 10 ton CO2 per juta Rupiah.
*   **Kepatuhan Terverifikasi:** Semua proyek yang disaring memenuhi standar kepatuhan 100% untuk **enkripsi tingkat tinggi** dan **persetujuan masyarakat**, yang diverifikasi melalui query SQL dan ditampilkan dengan jelas pada sumbu Z plot.
*   **Rekomendasi Berbasis Data:** Proyek-proyek yang diidentifikasi oleh query final (C004, C010, C016, dan C019) adalah kandidat utama untuk investasi karena memenuhi semua kriteria secara terukur dan dapat diaudit di blockchain. Ini memberikan bukti kuat bagi investor untuk mengalokasikan dana dengan keyakinan penuh.

-----

### **2.3 Prediksi Kinerja Proyek Berbasis Keterlibatan Masyarakat**

Analisis ini bertujuan untuk membangun model prediktif yang dapat membantu CEO mengalokasikan sumber daya secara optimal. Tujuannya adalah memprediksi keberhasilan jangka panjang sebuah proyek dengan menganalisis data historis partisipasi masyarakat dan memproyeksikan trennya di masa depan.

#### **1. Pengambilan dan Pra-pemrosesan Data (PostgreSQL)**

Data keterlibatan masyarakat dari berbagai proyek diekstraksi dan diolah menggunakan SQL untuk menghasilkan metrik kunci yang akan digunakan dalam model prediktif.

**a. SQL Query untuk Menghasilkan Metrik Keterlibatan**

Query berikut digunakan untuk menghitung **partisipasi relatif** (tingkat partisipasi sebuah proyek dibandingkan rata-rata semua proyek), **frekuensi kegiatan**, dan **total manfaat ekonomi** yang didistribusikan. Data ini kemudian digabungkan dengan data biodiversitas untuk analisis temporal.

```sql
WITH partisipasi_normal AS ( 
    SELECT 
        ce.Conservation_ID, 
        -- Menghitung partisipasi relatif untuk normalisasi data
        ce.Participants::INTEGER / NULLIF((SELECT AVG(Participants::INTEGER) FROM "014Community_Engagement"), 0) AS partisipasi_relatif, 
        COUNT(*) OVER (PARTITION BY ce.Conservation_ID) AS frekuensi_kegiatan, 
        SUM(Benefit_Distributed::INTEGER) OVER (PARTITION BY ce.Conservation_ID) AS total_manfaat,
        ce.Engagement_Date
    FROM "014Community_Engagement" ce
),
biodiversitas_summary AS (
    SELECT
        mc.Conservation_ID,
        mc.Area_Ha,
        mc.Carbon_Credits,
        mc.Date_Recorded
    FROM "001Mangrove_Conservation_Records" mc
)
SELECT 
    pn.Conservation_ID,
    pn.partisipasi_relatif,
    pn.frekuensi_kegiatan,
    pn.total_manfaat,
    pn.Engagement_Date,
    bs.Area_Ha,
    bs.Carbon_Credits,
    bs.Date_Recorded AS biodiversity_record_date
FROM partisipasi_normal pn
LEFT JOIN biodiversitas_summary bs ON pn.Conservation_ID = bs.Conservation_ID
ORDER BY pn.Engagement_Date;
```
Hasil dari query ini diekspor menjadi file `geospatial_resikofix.csv` untuk dianalisis lebih lanjut.

#### **2. Analisis Prediktif dan Forecasting (Python)**

Data historis partisipasi masyarakat dari `geospatial_resikofix.csv` digunakan untuk membangun model *time series* ARIMA. Model ini bertujuan untuk memprediksi tren tingkat partisipasi di masa depan.

**a. Visualisasi Forecasting Partisipasi Relatif**

Grafik berikut menampilkan data historis partisipasi relatif bulanan (garis biru) dan hasil *forecasting* untuk 6 bulan ke depan (garis merah putus-putus) menggunakan model ARIMA.

![VISUALISASI-ARIMA](https://github.com/Asfa-Asfialana/MangroveChain-Conservation-Analytics/blob/main/Studi%20Kasus%203/VISUALISASI-ARIMA.png)

**b. Kode Analisis Python (untuk Forecasting)**

*Catatan: Kode berikut adalah representasi konseptual dari proses yang dilakukan untuk menghasilkan visualisasi di atas.*
```python
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
import matplotlib.pyplot as plt

# 1. Muat dan siapkan data dari '3. Query 1.csv'
df = pd.read_csv("3. Query 1.csv")
df['engagement_date'] = pd.to_datetime(df['engagement_date'])
df.set_index('engagement_date', inplace=True)

# 2. Agregasi data partisipasi relatif secara bulanan
monthly_participation = df['partisipasi_relatif'].resample('M').sum()

# 3. Latih model ARIMA
# Parameter (p, d, q) dipilih berdasarkan analisis ACF dan PACF
model = ARIMA(monthly_participation, order=(5,1,0))
model_fit = model.fit()

# 4. Buat prediksi untuk 6 bulan ke depan
forecast = model_fit.forecast(steps=6)

# 5. Visualisasikan hasil
plt.figure(figsize=(15, 7))
plt.plot(monthly_participation, label='Actual Partisipasi')
plt.plot(forecast, label='Forecasted Partisipasi', color='red', linestyle='--')
plt.title('ARIMA Time Series Forecasting: Partisipasi Relatif Bulanan')
plt.xlabel('Bulan')
plt.ylabel('Total Partisipasi Relatif')
plt.legend()
plt.grid(True)
plt.savefig('arima_participation_forecast.png')
plt.show()

# Cetak hasil prediksi
print("Prediksi Partisipasi Relatif untuk 6 Bulan ke Depan:")
print(forecast)
```

#### **3. Hasil dan Implikasi Manajerial**

**a. Wawasan dari Data Historis:**
Data historis menunjukkan **volatilitas yang tinggi** dalam partisipasi masyarakat, dengan puncak dan lembah yang signifikan. Ini menandakan bahwa keterlibatan masyarakat bersifat dinamis dan dipengaruhi oleh berbagai faktor temporal (misalnya, musim, jadwal kegiatan, atau distribusi manfaat).

**b. Hasil Forecasting:**
Model ARIMA memprediksi bahwa tingkat partisipasi relatif bulanan akan **stabil di sekitar level 1.95** untuk enam bulan ke depan. Tren stabil ini, meskipun lebih rendah dari puncak terakhir, menunjukkan tingkat keterlibatan yang berkelanjutan dan sehat.

**c. Implikasi untuk CEO:**
*   **Alokasi Sumber Daya:** Prediksi tren yang stabil ini memberikan kepercayaan bagi manajemen untuk terus mengalokasikan sumber daya pada tingkat saat ini, tanpa perlu intervensi drastis.
*   **Perencanaan Proyek:** CEO dapat menggunakan *forecast* ini sebagai *baseline* untuk menetapkan target partisipasi di proyek-proyek baru. Jika partisipasi aktual di proyek baru berada jauh di bawah *baseline* yang diprediksi ini, maka intervensi dini dapat dilakukan.
*   **Manajemen Ekspektasi:** Model ini membantu dalam mengelola ekspektasi pemangku kepentingan dengan memberikan proyeksi berbasis data mengenai salah satu indikator kunci keberhasilan proyek.


----

### **2.4 Analisis Risiko Hukum Multi-Dimensi**

Analisis ini dilakukan untuk memenuhi kebutuhan departemen hukum dalam mengidentifikasi, memetakan, dan memprioritaskan proyek dengan profil risiko tertinggi. Alur kerja ini menggunakan SQL untuk melakukan skoring risiko multi-dimensi dan Python (dengan Plotly) untuk analisis geospasial, memvisualisasikan sebaran risiko di seluruh Indonesia.

#### **1. Skoring dan Klasifikasi Risiko (PostgreSQL)**

Langkah pertama adalah menghitung skor risiko untuk setiap proyek berdasarkan kombinasi beberapa faktor risiko dari tabel yang berbeda.

**a. SQL Query untuk Skoring dan Klasifikasi Risiko**

Query berikut ini menggabungkan data dari enam tabel untuk menghitung skor risiko berdasarkan tiga skenario utama. Setiap skenario diberikan bobot, dan skor total digunakan untuk mengklasifikasikan proyek menjadi tingkat risiko 'Tinggi', 'Sedang', atau 'Rendah'. Hasilnya kemudian digabungkan dengan data koordinat geospasial.

```sql
-- Query ini menghitung skor risiko dan menggabungkannya dengan data geospasial
WITH risk_scoring AS (
    SELECT 
        c.conservation_id,
        c.location,
        -- Skenario 1: Izin pending + batas lahan tidak terdefinisi (Bobot: 30)
        CASE 
            WHEN rp.permit_status = 'Pending' AND lt.boundary_defined = 'No' THEN 30 
            ELSE 0 
        END AS risiko_izin,
        -- Skenario 2: Lahan masyarakat + akses data terbatas (Bobot: 40)
        CASE 
            WHEN lt.land_type = 'Community Land' AND bd.access_level = 'Restricted' THEN 40 
            ELSE 0 
        END AS risiko_masyarakat,
        -- Skenario 3: Kualitas air buruk + restorasi intensif (Bobot: 30)
        CASE 
            WHEN bm.water_quality = 'Poor' AND ca.activity_type = 'Restoration' THEN 30 
            ELSE 0 
        END AS risiko_kualitas_air
    FROM mangrove_conservation_records c
    JOIN regulatory_permits rp ON c.conservation_id = rp.conservation_id
    JOIN land_tenure_records lt ON c.conservation_id = lt.conservation_id
    JOIN blockchain_data_compliance bd ON c.conservation_id = bd.conservation_id
    JOIN biodiversity_monitoring bm ON c.conservation_id = bm.conservation_id
    JOIN conservation_activities ca ON c.conservation_id = ca.conservation_id
    WHERE c.conservation_id IN (
        'C001', 'C002', 'C003', 'C004', 'C005', 'C006', 'C007', 'C008', 'C009', 'C010',
        'C011', 'C012', 'C013', 'C014', 'C015', 'C016', 'C017', 'C018', 'C019', 'C020'
    )
),
scored AS (
    SELECT 
        rs.*,
        (risiko_izin + risiko_masyarakat + risiko_kualitas_air) AS total_risiko,
        CASE 
            WHEN (risiko_izin + risiko_masyarakat + risiko_kualitas_air) >= 70 THEN 'Tinggi'
            WHEN (risiko_izin + risiko_masyarakat + risiko_kualitas_air) >= 30 THEN 'Sedang'
            ELSE 'Rendah'
        END AS tingkat_risiko
    FROM risk_scoring rs
)
SELECT 
    s.conservation_id,
    s.location,
    s.risiko_izin,
    s.risiko_masyarakat,
    s.risiko_kualitas_air,
    s.total_risiko,
    s.tingkat_risiko,
    k.latitude,
    k.longitude
FROM scored s
JOIN titik_koordinat_kabupaten k ON s.location = k.location
ORDER BY s.total_risiko DESC;
```
Hasil dari query ini diekspor menjadi file `geospatial_resikofix.csv` untuk visualisasi.

#### **2. Analisis Geospasial Pola Sebaran Risiko (Python & Plotly)**

Data yang telah diskor dan diklasifikasikan kemudian dipetakan untuk memberikan pandangan visual tentang distribusi geografis risiko.

**a. Visualisasi Peta Geospasial Interaktif**

Peta berikut dibuat menggunakan Plotly dan menampilkan setiap proyek sebagai sebuah titik. Warna titik merepresentasikan tingkat risiko (Tinggi, Sedang, Rendah), dan ukuran titik merepresentasikan skor risiko tambahan (misalnya, dampak ekologis). Peta ini interaktif, memungkinkan pengguna untuk *hover* di setiap titik untuk melihat detail lebih lanjut.

*(Karena ini adalah file Markdown statis, Anda bisa menempatkan screenshot dari peta interaktif di sini, atau menyertakan tautan ke file HTML-nya.)*

**Peta Interaktif dapat dilihat di sini:**[./peta_risiko_konservasi.html](https://github.com/Asfa-Asfialana/MangroveChain-Conservation-Analytics/blob/main/Studi%20Kasus%204/peta_risiko_konservasi.html))

**b. Kode Analisis Python (studi_kasus4.ipynb)**

Kode berikut membaca data CSV yang dihasilkan oleh SQL dan menggunakan Plotly Express untuk membuat peta interaktif.

```python
import pandas as pd
import plotly.express as px

# Muat data dari hasil query PostgreSQL
df = pd.read_csv("geospatial_resikofix.csv")

# Pastikan kolom yang dibutuhkan ada
required_columns = ['conservation_id', 'location', 'latitude', 'longitude', 'tingkat_risiko', 'risk_score']
# ... (sisa kode seperti yang disediakan) ...

# Buat visualisasi peta geospasial
fig = px.scatter_mapbox(
    df,
    lat="latitude",
    lon="longitude",
    color="tingkat_risiko",
    size="risk_score",  # Besar simbol bisa disesuaikan, misal dengan total_risiko
    hover_name="location",
    hover_data={
        "conservation_id": True,
        "risk_score": True,
        "latitude": False,
        "longitude": False
    },
    zoom=4,
    height=600,
    mapbox_style="carto-positron",
    title="Peta Geospasial Tingkat Risiko Konservasi Mangrove"
)

# Tampilkan dan simpan sebagai HTML interaktif
fig.show()
fig.write_html("peta_risiko_konservasi.html")
```

#### **3. Hasil dan Implikasi**

**a. Identifikasi Klaster Risiko:**
Peta geospasial dengan jelas mengidentifikasi "klaster risiko" atau "hotspot". Proyek dengan tingkat risiko **'Tinggi'** seperti di **Serang** (C017) dan **'Sedang'** seperti di **Tanah Laut** (C003) dan **Takalar** (C002) menjadi prioritas utama untuk mitigasi risiko.

**b. Validasi Model Risiko:**
Analisis ini mengonfirmasi hipotesis awal bahwa proyek dengan kombinasi beberapa faktor risiko (misalnya, izin *pending* dan kualitas air buruk) memang memiliki skor risiko total yang lebih tinggi. Ini membuktikan kemampuan sistem untuk mengubah data kualitatif dari berbagai sumber menjadi metrik risiko yang terukur dan dapat ditindaklanjuti.

**c. Perencanaan Strategis:**
Departemen hukum dapat menggunakan peta ini untuk mengalokasikan sumber daya secara proaktif, seperti mengirim tim mediasi ke wilayah dengan klaster risiko 'Sedang' dan 'Tinggi' atau melakukan audit hukum yang lebih mendalam pada proyek-proyek yang teridentifikasi.
