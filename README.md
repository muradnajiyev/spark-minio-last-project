# 🚀 Spark + MinIO + Delta Lake + Apache Iceberg

## 📌 Layihə haqqında

Bu layihədə **Apache Spark, MinIO, Delta Lake və Apache Iceberg** istifadə edilərək transaction məlumatları üzərində tam Data Engineering prosesi həyata keçirilmişdir.

Layihədə **Medallion Architecture** yanaşmasından istifadə olunmuş və məlumatlar **Bronze, Silver və Gold** layer-lərinə ayrılmışdır.

Bundan əlavə, Delta Lake və Apache Iceberg üzərində **CRUD əməliyyatları, MERGE/UPSERT, History, Snapshot, Time Travel və Schema Evolution** imkanları yoxlanılmış və müqayisə edilmişdir.

---

## 🛠️ İstifadə olunan texnologiyalar

* 🐍 Python
* ⚡ Apache Spark / PySpark
* 🗄️ MinIO
* 🔷 Delta Lake
* 🧊 Apache Iceberg
* 🐳 Docker
* 📓 Jupyter Notebook
* 📄 CSV

---

# 🏗️ Layihə arxitekturası

```text
                  transactions_raw.csv
                          │
                          ▼
                  ┌───────────────┐
                  │    BRONZE     │
                  │               │
                  │   Raw Data    │
                  │ Delta +       │
                  │ Iceberg       │
                  └───────┬───────┘
                          │
                          ▼
                  ┌───────────────┐
                  │    SILVER     │
                  │               │
                  │ Data Cleaning │
                  │ Deduplication │
                  │ Type Casting  │
                  │ Delta +       │
                  │ Iceberg       │
                  └───────┬───────┘
                          │
                          ▼
                  ┌───────────────┐
                  │     GOLD      │
                  │               │
                  │ Transaction   │
                  │ Summary       │
                  │ Delta +       │
                  │ Iceberg       │
                  └───────────────┘
```

---

# 📂 Layihə strukturu

```text
spark-minio-last-project/
│
├── data/
│   └── transactions_raw.csv
│
├── docker/
│   └── docker-compose.yml
│
├── notebooks/
│   └── transaction_project.ipynb
│
├── screenshots/
│   ├── project1/
│   ├── project2/
│   ├── project3/
│   ├── project4/
│   └── project5/
│
└── README.md
```

---

# 1. ⚙️ Mühitin qurulması

Layihə mühiti **Docker** vasitəsilə qurulmuşdur. Məlumatların saxlanılması üçün **MinIO**, Spark kodlarının işlədilməsi üçün isə **Jupyter Notebook** istifadə edilmişdir.

MinIO S3-compatible object storage kimi istifadə olunmuş və Spark ilə MinIO arasında bağlantı konfiqurasiya edilmişdir.

---

# 2. ⚡ Spark Session

Spark Session **Delta Lake** və **Apache Iceberg** dəstəyi aktiv edilərək yaradılmışdır.

Iceberg catalog MinIO üzərində yerləşən warehouse-a qoşulmuş, S3A konfiqurasiyası vasitəsilə Spark və MinIO arasında əlaqə yaradılmışdır.

Iceberg üçün aşağıdakı namespace-lər yaradılmışdır:

```text
iceberg.bronze
iceberg.silver
iceberg.gold
```

---

# 3. 📊 Dataset

Layihədə `transactions_raw.csv` faylından istifadə edilmişdir.

Dataset Spark vasitəsilə oxunmuş, schema müəyyən edilmiş və ilkin məlumat analizi aparılmışdır.

Dataset üzərində aşağıdakı Data Quality problemləri araşdırılmışdır:

* Duplicate qeydlər
* NULL dəyərlər
* Mənfi və uyğunsuz dəyərlər
* Data type-lar
* Boş string dəyərləri

---

# 4. 🥉 Bronze Layer

Raw transaction məlumatları heç bir dəyişiklik edilmədən Bronze layer-də saxlanılmışdır.

Məlumatlar həm **Delta**, həm də **Iceberg** formatında yazılmışdır.

### Delta

```text
s3a://warehouse/delta/bronze/transactions
```

### Iceberg

```text
iceberg.bronze.transactions
```

Bundan əlavə, MinIO daxilində yaranan fayl və qovluq strukturları araşdırılmışdır.

---

# 5. 🥈 Silver Layer

Bronze layer-dəki raw məlumatlardan təmizlənmiş Silver layer yaradılmışdır.

Data cleaning zamanı:

* `id` NULL olan qeydlər silinmişdir
* `customer_id` NULL olan qeydlər silinmişdir
* `dt` NULL olan qeydlər silinmişdir
* `amount` NULL olan qeydlər silinmişdir
* Duplicate qeydlər `id` əsasında silinmişdir
* `id` → `integer`
* `customer_id` → `integer`
* `amount` → `double`
* `dt` → `date`
* `currency` trim edilərək böyük hərflərə çevrilmişdir
* `status` trim edilərək kiçik hərflərə çevrilmişdir
* Boş string dəyərlər `NULL` ilə əvəz edilmişdir

Bronze və Silver sətr sayı müqayisə edilərək neçə sətrin data cleaning nəticəsində silindiyi müəyyən edilmişdir.

Təmizlənmiş Silver data həm Delta, həm də Iceberg formatında saxlanılmışdır.

---

# 6. 🥇 Gold Layer

Silver layer-dəki təmizlənmiş məlumatlar əsasında **gündəlik və currency üzrə transaction summary** yaradılmışdır.

Aşağıdakı göstəricilər hesablanmışdır:

| Sütun                | İzah                         |
| -------------------- | ---------------------------- |
| `total_transactions` | Ümumi transaction sayı       |
| `total_amount`       | Ümumi transaction məbləği    |
| `avg_amount`         | Orta transaction məbləği     |
| `min_amount`         | Minimum transaction məbləği  |
| `max_amount`         | Maksimum transaction məbləği |

Gold məlumatları həm Delta, həm də Iceberg formatında saxlanılmışdır.

### Delta

```text
s3a://warehouse/delta/gold/transaction_summary
```

### Iceberg

```text
iceberg.gold.transaction_summary
```

---

# 7. 🔄 CRUD Operations

Delta və Iceberg Gold cədvəlləri üzərində aşağıdakı əməliyyatlar yoxlanılmışdır:

## INSERT

Yeni transaction summary qeydləri həm Delta, həm də Iceberg cədvəllərinə əlavə edilmiş və nəticə yoxlanılmışdır.

## UPDATE

Mövcud transaction məlumatları `dt` və `currency` əsasında dəyişdirilmiş və yenilənmiş nəticələr göstərilmişdir.

## DELETE

Seçilmiş transaction qeydləri hər iki texnologiyada silinmiş və silinmə nəticəsi yoxlanılmışdır.

## MERGE / UPSERT

MERGE əməliyyatı vasitəsilə **UPSERT** məntiqi tətbiq edilmişdir.

Əgər uyğun qeyd mövcuddursa, məlumat **UPDATE** edilmişdir.

Əgər uyğun qeyd mövcud deyilsə, yeni məlumat **INSERT** edilmişdir.

---

# 8. 🕒 History & Time Travel

Delta və Iceberg-in keçmiş vəziyyətləri və versiyaları araşdırılmışdır.

## Delta Lake

Delta cədvəlinin history məlumatları `DESCRIBE HISTORY` vasitəsilə əldə edilmişdir.

Müxtəlif versiyalar `versionAsOf` istifadə edilərək oxunmuş və versiyalar arasında fərqlər yoxlanılmışdır.

Time Travel vasitəsilə Delta cədvəlinin əvvəlki vəziyyəti oxunmuşdur.

## Apache Iceberg

Iceberg cədvəlinin snapshot məlumatları araşdırılmışdır.

Snapshot-lar `committed_at`, `snapshot_id` və `operation` məlumatlarına əsasən müqayisə edilmişdir.

Time Travel istifadə edilərək əvvəlki Iceberg snapshot-ı oxunmuşdur.

---

# 9. 📋 Yekun hesabat

## Medallion Architecture

Layihədə **Medallion Architecture** yanaşmasından istifadə edilmişdir. Raw transaction məlumatları əvvəlcə Bronze layer-də saxlanılmış, daha sonra Silver layer-də təmizlənmiş və transformasiya edilmişdir. Silver məlumatları əsasında gündəlik və currency üzrə transaction summary yaradılaraq Gold layer formalaşdırılmışdır. Məlumatlar həm Delta Lake, həm də Apache Iceberg formatlarında saxlanılmışdır.

## Data Quality nəticələri

Data Quality mərhələsində NULL dəyərlər, duplicate qeydlər, boş string-lər, data type-lar və uyğunsuz məlumatlar yoxlanılmışdır. Problemli məlumatlar Silver layer-də təmizlənmiş və uyğun data type-lara çevrilmişdir. Bronze və Silver sətr sayı müqayisə edilərək cleaning nəticəsində silinən sətrlərin sayı müəyyən edilmişdir.

## Delta vs Iceberg fərqləri

Eyni Gold məlumatları həm Delta Lake, həm də Apache Iceberg formatında saxlanılmış və müqayisə edilmişdir. Hər iki texnologiyada INSERT, UPDATE, DELETE, MERGE/UPSERT, History/Snapshot, Time Travel və Schema Evolution əməliyyatları yoxlanılmışdır. Hər iki texnologiya ACID transaction və schema evolution imkanlarını dəstəkləyir, lakin metadata idarəetməsi, transaction log və snapshot yanaşmalarında fərqlənirlər.

## Qarşılaşılan problemlər və həlli

Layihə zamanı INSERT əməliyyatında `dt` sütununun data type-ı ilə bağlı problem yaranmışdır. Yeni DataFrame-də `dt` `STRING`, mövcud Gold cədvəlində isə `DATE` olduğu üçün data uyğunluğu xətası baş vermişdir.

Problem `F.to_date()` istifadə edilərək `dt` sütununun `DATE` tipinə çevrilməsi ilə həll edilmişdir.

---

# ⭐ Bonus — Schema Evolution

Schema Evolution həm Iceberg, həm də Delta üzərində tətbiq edilmişdir.

### Iceberg

Iceberg cədvəlində:

```text
dt → date
```

sütun adı dəyişdirilmişdir.

### Delta

Delta cədvəlinə:

```text
customer_comments STRING
```

adlı yeni sütun əlavə edilmişdir.

Daha sonra hər iki cədvəlin schema və data nəticələri müqayisə edilmişdir.

Bu test nəticəsində həm Delta, həm də Iceberg-də mövcud məlumatları silmədən schema-nı dəyişdirməyin mümkün olduğu göstərilmişdir.

---

# 📸 Screenshots

Layihə zamanı MinIO bucket-larında və müxtəlif mərhələlərdə əldə edilmiş nəticələrin screenshot-ları `screenshots/` qovluğunda saxlanılmışdır.

```text
screenshots/
├── project1/
├── project2/
├── project3/
├── project4/
└── project5/
```

---

# 🎯 Nəticə

Bu layihədə raw transaction məlumatlarının qəbul edilməsindən başlayaraq **Bronze → Silver → Gold** mərhələlərindən keçən tam Data Engineering pipeline qurulmuşdur.

Layihə çərçivəsində **Apache Spark, MinIO, Delta Lake və Apache Iceberg** birlikdə istifadə edilmişdir. Data cleaning, aggregation, CRUD, MERGE/UPSERT, History, Snapshot, Time Travel və Schema Evolution kimi əsas lakehouse əməliyyatları praktiki olaraq yoxlanılmışdır.

Layihənin əsas məqsədi Delta Lake və Apache Iceberg texnologiyalarının real data pipeline üzərində istifadəsini və onların əsas imkanlarını praktiki şəkildə öyrənməkdir.
