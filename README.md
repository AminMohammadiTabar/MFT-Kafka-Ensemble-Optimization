# بهینه سازی ارسال داده در ارتباطات بین میکروسرویس ها در معماری رویداد محور با استفاده از مدل های ensemble یادگیری ماشین

**English:** Optimizing Data Transmission Between Microservices in an Event-Driven Architecture Using Ensemble Machine Learning Models

**[فارسی](#راهنمای-فارسی) · [English](#english-documentation)**

> **Project type:** Experimental machine-learning research / Kafka producer configuration recommendation  
> **Core technologies:** Apache Kafka, Python, scikit-learn, Random Forest, XGBoost, Gaussian Process Regression, LightGBM, Stacking  
> **Research report:** [`docs/reports/project-report-fa.pdf`](docs/reports/project-report-fa.pdf) · [`Word`](docs/reports/project-report-fa.docx)

---

<a id="راهنمای-فارسی"></a>

## 🇮🇷 راهنمای فارسی

### ۱. معرفی پروژه

این پروژه با هدف **پیش‌بینی و افزایش توان عملیاتی ارسال رویدادها (Throughput)** در ارتباطات غیرهمگام میان میکروسرویس‌ها توسعه یافته است. در معماری رویدادمحور، تولیدکننده (Producer) پیام‌ها را به **Apache Kafka** ارسال می‌کند و مصرف‌کننده‌ها (Consumers) آن‌ها را مستقل از تولیدکننده دریافت و پردازش می‌کنند. عملکرد مسیر ارسال، تحت تأثیر تنظیمات تولیدکننده و حجم کار است.

در این پژوهش، به‌جای انتخاب ثابت تنظیمات Kafka، رابطه بین چهار ورودی و نرخ ارسال پیام با مدل‌های رگرسیون یادگیری ماشین آموخته می‌شود. سپس برای تعداد اسناد درخواستی، **۴۱۶ ترکیب** از سه پارامتر پیکربندی بررسی و ترکیب‌های دارای بیشترین Throughput **پیش‌بینی‌شده** پیشنهاد می‌شوند.

**نکته محدوده پروژه:** کدها و نوت‌بوک‌های موجود برای تحلیل داده، آموزش مدل و پیشنهاد تنظیمات هستند. گزارش، بستر آزمایش را با Java 21، Spring Boot 3.5.4، Kafka 3.9.2، MongoDB و PostgreSQL توصیف می‌کند؛ **سورس Java، سرویس‌های Kafka، کانفیگ استقرار و کد جمع‌آوری داده در این آرشیو وجود ندارند**. بنابراین این مخزن، یک پروژه کامل قابل استقرار برای میکروسرویس‌ها نیست و بازتولید آزمایش‌های زیرساختی به محیط اصلی نیاز دارد.

### ۲. مسئله، هدف و جریان کار

```text
[Producer / Microservice]
         |
         |  batch.size + linger.ms + compression.type
         v
   [Apache Kafka] -----> [Consumer Microservices]
         |
         v
[Experimental throughput measurements]
         |
         v
[JSON dataset] -> [Preprocessing] -> [Regression models]
                                            |
                                            v
                               [Stacking / Recommendation]
                                            |
                                            v
                             [Ranked producer configurations]
```

این شکل، **جریان مفهومی پژوهش** است، نه نمودار اجزای قابل اجرای موجود در ZIP. هدف مدل، `throughput` بر حسب **تعداد اسناد در ثانیه (`docs/sec`)** است؛ این پروژه به‌صورت مستقیم تأخیر، هزینه منابع، مصرف حافظه یا تحویل تضمین‌شده رویدادها را بهینه نمی‌کند.

سه تنظیم قابل پیشنهاد، متعلق به **Kafka Producer** هستند: `batchSize` (متناظر با `batch.size`)، `lingerMs` (متناظر با `linger.ms`) و `compressionType` (متناظر با `compression.type`). ویژگی `billSize` اندازه بار کاری به‌شکل **تعداد اسناد JSON** است و جزء سه تنظیم پیشنهادی محسوب نمی‌شود.

### ۳. داده‌ها و طراحی آزمایش

طبق گزارش پژوهش، پیام‌ها در سه بار کاری حدود ۵۰، ۱۰۰ و ۱۵۰ مگابایت (به‌ترتیب ۳۰۹٬۰۰۰، ۶۱۸٬۰۰۰ و ۹۲۷٬۰۰۰ سند) ارسال شده‌اند. هر ترکیب تنظیمات در سه تکرار آزمایش شده و در گزارش، میانگین‌گیری برای کاهش نویز اندازه‌گیری توضیح داده شده است.

| ستون | نوع | نقش و واحد |
|---|---|---|
| `batchSize` | عدد صحیح | حداکثر اندازه batch تولیدکننده، **بایت** |
| `lingerMs` | عدد صحیح | انتظار تولیدکننده برای تشکیل batch، **میلی‌ثانیه** |
| `compressionType` | دسته‌ای | روش فشرده‌سازی پیام: `none`، `snappy`، `lz4`، `gzip` |
| `billSize` | عدد صحیح | تعداد اسناد JSON در بار کاری |
| `throughput` | عددی پیوسته | متغیر هدف: **سند در ثانیه** |

مقادیر مشاهده‌شده در مجموعه‌داده تحویلی:

- `batchSize`: **16384، 32768، 49152، 65536** بایت؛
- `lingerMs`: از **5 تا 505** میلی‌ثانیه با گام **20** (۲۶ مقدار)؛
- `compressionType`: چهار گزینه بالا؛
- `billSize`: **309000، 618000، 927000** سند.

پس برای هر مقدار `billSize`، تعداد تنظیمات برابر **۴ × ۲۶ × ۴ = ۴۱۶** و تعداد ردیف‌های فایل داده برابر **۴۱۶ × ۳ = ۱۲۴۸** است. داده آماده‌شده شامل **۱۲۴۸ ردیف با ترکیب‌های یکتا** است؛ فایل‌های مستقل سه تکرار اولیه در آرشیو قرار ندارند. نباید این ۱۲۴۸ ردیف را با ۳۷۴۴ مشاهده خامِ تکرارشده اشتباه گرفت.

**فایل‌ها:**

- `data/raw/mongo_test.test-result.json`: ورودی JSON تحلیل (۱۲۴۸ رکورد؛ `batchSize` در آن به‌شکل رشته نیز ثبت شده است).
- `data/processed/cleaned_data.json`: نسخه تبدیل‌نوع‌یافته داده‌ها (۱۲۴۸ رکورد).
- `data/processed/scaled_data.json`: داده ویژگی‌های نرمال‌شده **فقط بخش آموزش** (۹۹۸ ردیف، ۷ ستون)؛ نه مجموعه‌داده خام و نه شامل متغیر هدف.

### ۴. پیش‌پردازش و روش یادگیری

۱. تبدیل داده JSON به DataFrame و تبدیل `batchSize` به نوع عدد صحیح؛ در ماژول `src/kafka_preprocessing.py` امکان گروه‌بندی و میانگین‌گیری بر اساس چهار ورودی وجود دارد. برای فایل تحویلیِ دارای ترکیب‌های یکتا، این میانگین‌گیری تغییری در تعداد ردیف‌ها نمی‌دهد.

۲. جداسازی `throughput` از چهار ویژگی ورودی و تقسیم داده با `test_size=0.2` و `random_state=42` به **۹۹۸ نمونه آموزش** و **۲۵۰ نمونه آزمون**.

۳. اجرای `StandardScaler` برای `batchSize`، `lingerMs` و `billSize` و `OneHotEncoder` برای `compressionType` با ترتیب ثابت `none → snappy → lz4 → gzip`؛ تعداد ویژگی‌های خروجی **۷** است.

۴. برازش پیش‌پردازنده روی آموزش و تبدیل هر دو مجموعه با همان پارامترها. برای **GPR**، مقیاس‌بندی مجزای متغیر هدف و معکوس‌کردن مقیاس پیش‌بینی‌ها نیز انجام می‌شود.

۵. آموزش چهار مدل **Random Forest** (با `MultiOutputRegressor`)، **XGBoost**، **Gaussian Process Regression** و **LightGBM**؛ سپس استفاده از **Stacking** با پیش‌بینی‌های خارج از fold در **اعتبارسنجی پنج‌بخشی** و فرا‌مدل `Ridge(alpha=1.0)`.

**نسخه اصلاح‌شده Stacking:** در `notebooks/05_stacking.ipynb` فقط **Random Forest + XGBoost + GPR** به‌عنوان مدل‌های پایه به `Ridge(alpha=1.0)` متصل هستند. این معماری مطابق نسخه منتخب گزارش Word/PDF است؛ دلیل مطرح‌شده در گزارش برای حذف LightGBM همبستگی زیاد آن با XGBoost (`0.9860`) است. **LightGBM به‌عنوان مدل مستقل حفظ شده** و در جدول ارزیابی به‌صورت جداگانه مقایسه می‌شود. نسخه اصلی چهارمدلی با خروجی‌های تاریخی آن در `notebooks/archive/05_stacking_4_models_legacy.ipynb` موجود است. نتایج زیر متعلق به **نسخه چهارمدلی تاریخی** هستند، نه اجرای جدید Stacking سه‌مدلی؛ برای به‌دست‌آوردن معیارهای سه‌مدلی باید نوت‌بوک اصلاح‌شده اجرا شود.

### ۵. نتایج ارزیابی ثبت‌شده

| مدل | R² ↑ | MAE ↓ (docs/sec) | RMSE ↓ (docs/sec) |
|---|---:|---:|---:|
| Random Forest | 0.8620 | 6666.97 | 10430.15 |
| XGBoost | 0.8824 | 6584.96 | 9627.00 |
| GPR | 0.8211 | 7565.13 | 11876.04 |
| LightGBM | 0.8556 | 7314.17 | 10667.87 |
| **Stacking (چهارمدلی، تاریخی / four-model legacy)** | **0.8849** | **6350.22** | **9527.09** |

- **Stacking چهارمدلی تاریخی** بهترین مقدار R² و کمترین خطای ثبت‌شده را دارد؛ بهبود آن نسبت به XGBoost **اندک** است. نتیجه نسخه سه‌مدلی باید جداگانه محاسبه شود.
- **XGBoost** بهترین مدل منفرد در این مقایسه است.
- **GPR** امکان تخمین عدم‌قطعیت را فراهم می‌کند؛ مقدار نمونه گزارش‌شده `±0.3482` در **فضای مقیاس‌بندی‌شده هدف** است و نباید به‌عنوان `docs/sec` یا تضمین آماری عملیاتی تعبیر شود.
- این اعداد مربوط به **پیش‌بینی روی داده آزمون** هستند؛ بیانگر افزایش Throughput تأییدشده پس از استقرار روی Kafka نیستند.

### ۶. ساختار پوشه‌ها

```text
MFT-Kafka-Ensemble-Optimization/
├── README.md
├── requirements.txt
├── .gitignore
├── src/
│   └── kafka_preprocessing.py
├── notebooks/
│   ├── 01_random_forest.ipynb
│   ├── 02_xgboost.ipynb
│   ├── 03_gaussian_process_regression.ipynb
│   ├── 04_lightgbm.ipynb
│   ├── 05_stacking.ipynb      # پیاده‌سازی اصلی سه‌مدلی / three-model design
│   └── archive/
│       └── 05_stacking_4_models_legacy.ipynb
├── data/
│   ├── raw/
│   │   └── mongo_test.test-result.json
│   └── processed/
│       ├── cleaned_data.json
│       └── scaled_data.json
├── artifacts/
│   ├── models/           # 4 trained estimator .pkl files
│   ├── preprocessing/    # preprocessor.pkl, target_scaler.pkl
│   └── splits/           # X/y train/test snapshots (.pkl)
└── docs/
    ├── reports/
    │   ├── project-report-fa.docx
    │   └── project-report-fa.pdf
    └── references/       # 5 related research PDF files
```

تمام فایل‌های اصلی پژوهش، مدل‌ها، داده‌ها، گزارش Word/PDF و مقالات در خروجی حفظ شده‌اند. تنها فایل‌های جانبی و قابل بازتولید IDE/Jupyter/Python، یعنی `.idea/`، `.ipynb_checkpoints/` و `__pycache__/` از بسته انتشار حذف شده‌اند. برای خوانایی، نام نوت‌بوک‌ها شماره‌گذاری و نام گزارش‌ها استاندارد شده است.

### ۷. نصب و راه‌اندازی

**نیازمندی‌ها:** Python **3.11 یا جدیدتر** (برای سازگاری بهتر با فایل‌های ذخیره‌شده، Python 3.13 پیشنهاد می‌شود)، کتابخانه‌های `requirements.txt` و Jupyter Notebook. برای تحلیل داده‌های **موجود**، اجرای Kafka، Java یا پایگاه داده الزامی نیست. برای تولید دوباره داده‌های آزمایشی، محیط Kafka و کدهای Producer/Consumer جداگانه لازم‌اند.

از **ریشه پروژه** دستورهای زیر را اجرا کنید:

```bash
# Linux / macOS
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
jupyter notebook notebooks/
```

```powershell
# Windows PowerShell
py -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
jupyter notebook notebooks/
```

در محیط Jupyter، نوت‌بوک‌ها را به‌ترتیب شماره اجرا کنید:

1. `01_random_forest.ipynb` — آموزش Random Forest و ذخیره `rf_model.pkl`؛
2. `02_xgboost.ipynb` — آموزش XGBoost و ذخیره `xgb_model.pkl`؛
3. `03_gaussian_process_regression.ipynb` — آموزش GPR و ذخیره `gpr_model.pkl`؛
4. `04_lightgbm.ipynb` — آموزش LightGBM و ذخیره مدل، پیش‌پردازنده و مجموعه‌های train/test؛
5. `05_stacking.ipynb` — بارگذاری مدل‌ها، ساخت سه فراویژگی OOF، آموزش مجدد Ridge سه‌مدلی، ارزیابی و پیشنهاد تنظیمات.

کدهای مسیر‌یابی ابتدای نوت‌بوک‌ها طوری تنظیم شده‌اند که اجرا از **ریشه پروژه یا پوشه `notebooks/`** امکان‌پذیر باشد. برای استفاده از مدل‌های ذخیره‌شده بدون آموزش مجدد، artifactها از پیش داخل ZIP وجود دارند؛ اما نوت‌بوک Stacking همچنان **فرا‌مدل Ridge را در حافظه می‌آموزد** و فایل `stacking_model.pkl` مستقلی در آرشیو وجود ندارد. اجرای GPR و اعتبارسنجی متقاطع Stacking ممکن است مصرف پردازنده و حافظه بالایی داشته باشد.

**تست مستقل پایپ‌لاین پیش‌پردازش با داده مصنوعی:**

```bash
python src/kafka_preprocessing.py
```

این دستور **داده مصنوعی** تولید می‌کند و معادل آموزش یا ارزیابی مدل‌های اصلی پروژه نیست. برای فراخوانی تابع پیش‌پردازش روی داده واقعی در ریشه پروژه:

```python
import sys
sys.path.insert(0, "src")
from kafka_preprocessing import preprocess

results = preprocess("data/raw/mongo_test.test-result.json")
X_train_s, X_test_s = results[0], results[1]
print(X_train_s.shape, X_test_s.shape)  # (998, 7), (250, 7)
```

### ۸. استفاده از پیشنهاددهنده

پس از اجرای `05_stacking.ipynb` و ساخته‌شدن متغیرهای فرا‌مدل، تابع زیر را داخل همان نوت‌بوک صدا بزنید:

```python
recommend_stacking(bill_size=309000, top_n=3)
```

این تابع ۴۱۶ ترکیب را با مدل فعلی نوت‌بوک امتیازدهی می‌کند و `batchSize`، `lingerMs`، `compressionType` و `predicted_throughput` را به‌ترتیب نزولی نشان می‌دهد. برای مدل‌های منفرد نیز توابع `recommend(...)`، `recommend_xgb(...)`، `recommend_gpr(...)` و `recommend_lgbm(...)` در نوت‌بوک‌های مربوط وجود دارند.

**اصلاح صحت داده:** در نسخه اولیه، مقدار `23768` به‌اشتباه در شبکه پیشنهاددهی نوت‌بوک‌ها درج شده بود، درحالی‌که داده آموزشی مقدار **`32768`** دارد؛ این اشتباه در **کد شبکه‌های پیشنهاددهی** اصلاح شده است. خروجی‌های پیشنهاد ذخیره‌شده پیش از اصلاح ممکن است تاریخی باشند؛ پس برای دریافت رتبه‌بندی معتبر با شبکه جدید، سلول‌های پیشنهاددهی را **دوباره اجرا کنید**. تنظیمات پیشنهادی برای `billSize` خارج از حجم‌های مشاهده‌شده، پیش‌بینی برون‌یابی‌شده هستند و نیاز به آزمایش عملی دارند.

### ۹. محدودیت‌ها، بازتولیدپذیری و نکات ایمنی

- داده‌های آرشیو فقط از **سه مقدار `billSize`** و یک بازه مشخص پیکربندی جمع‌آوری شده‌اند؛ نتایج الزاماً به سخت‌افزار، نسخه Kafka یا بار کاری دیگر تعمیم پیدا نمی‌کنند.
- با وجود شرح تکرار آزمایش‌ها در گزارش، **فایل‌های خام سه اجرای مستقل** و پروژه Java جمع‌آوری‌کننده در ZIP موجود نیستند؛ بازتولید مرحله تولید داده از روی این مخزن به‌تنهایی ممکن نیست.
- **اعداد تاریخی نسخه چهارمدلی** را به **نسخه سه‌مدلی اصلاح‌شده** نسبت ندهید؛ فرا‌مدل جدید نیازمند اجرا و ارزیابی مجدد است.
- اختلاف نسخه کتابخانه‌ها ممکن است باعث هشدار یا ناسازگاری در بارگذاری مدل‌های `joblib`/`.pkl` شود؛ درصورت لزوم مدل‌ها را در محیط سازگار بازآموزی کنید.
- فایل‌های `.pkl` را **فقط از منابع مورد اعتماد** باز کنید؛ قالب pickle می‌تواند هنگام بارگذاری کد اجرا کند.
- «بهترین تنظیم پیش‌بینی‌شده» برابر «بهبود قطعی پس از استقرار» نیست. پیش از استفاده عملی، تأخیر انتهابه‌انتها، بار CPU، مصرف حافظه، پایداری و الزامات تحویل را نیز بسنجید.
- مجوز انتشار/استفاده مجدد در فایل‌های اولیه مشخص نشده است؛ وجود این ZIP به‌تنهایی مجوز متن‌باز محسوب نمی‌شود.

### ۱۰. گزارش، منابع و انتساب

گزارش کامل پژوهش در `docs/reports/` (دو نسخه Word و PDF) و پنج فایل PDF منابع پژوهشی در `docs/references/` قرار دارد. مطابق جلد گزارش: **محمد امین محمدی تبار**، پروژه دوره «یادگیری ماشین با پایتون»، **مهر ۱۴۰۵**. برای روش‌های آزمایش، مبانی نظری و جداول کامل به گزارش مراجعه کنید.

---

<a id="english-documentation"></a>

## 🇬🇧 English Documentation

### 1. Overview and scope

This research project explores **Kafka producer throughput prediction and configuration recommendation** for asynchronous inter-service messaging in an **event-driven microservices architecture**. Rather than relying on one static producer configuration, regression models learn the relationship between Kafka producer settings, workload size, and the observed throughput (JSON documents transmitted per second).

Given a workload size (`billSize`), the recommender scores **416 candidate producer configurations** and ranks them by **predicted** throughput. This is an experiment-driven ML repository, **not a deployable end-to-end microservices application**. The written report describes an experimental stack using **Java 21, Spring Boot 3.5.4, Apache Kafka 3.9.2, MongoDB, and PostgreSQL**, but the associated Java services, Kafka deployment files, and original data-collection application are **not provided** in this archive. The included executable assets are Python preprocessing and Jupyter ML notebooks.

**Research question:** Can supervised regression, particularly ensemble stacking, estimate Kafka message-sending throughput from producer configuration choices and workload size, and use those predictions to recommend higher-throughput settings?

The target is **throughput (`docs/sec`)**. Latency, CPU utilization, memory use, reliability, and cost are **not** jointly optimized by the current predictor.

### 2. Conceptual workflow

```text
Producer microservice --(batch.size / linger.ms / compression.type)--> Kafka
                                                                  |
                                                                  v
                                                       Consumer microservices

Measured throughput -> Experimental JSON -> Data preprocessing
                                             |
                                             v
                     Random Forest / XGBoost / GPR / LightGBM
                                             |
                                             v
                                 Stacking + Ridge regression
                                             |
                                             v
                           Rank 416 producer configurations
```

This depicts the **research workflow**, not a collection of runnable components shipped in the repository. The knobs belong to the **Kafka producer** rather than the Kafka broker.

### 3. Dataset and experimental design

The report describes workloads of approximately **50 MB, 100 MB, and 150 MB**, represented by **309,000**, **618,000**, and **927,000** JSON documents. Experiments were repeated three times and averaged to reduce measurement noise. The archive contains the **resulting 1,248 unique configurations**, **not the individual raw files from all three repetitions**.

| Field | Type | Meaning |
|---|---|---|
| `batchSize` | Integer | Producer batch-size setting, **bytes** |
| `lingerMs` | Integer | Producer batching wait, **milliseconds** |
| `compressionType` | Categorical | `none`, `snappy`, `lz4`, or `gzip` |
| `billSize` | Integer | Workload size, measured as number of JSON documents |
| `throughput` | Float | Regression target, **documents/second** |

The archived dataset covers four `batchSize` values (**16384, 32768, 49152, 65536**), 26 `lingerMs` values (**5 through 505**, step 20), four compression codecs, and three `billSize` values. Therefore **4 × 26 × 4 = 416** configurations per workload and **416 × 3 = 1,248** rows overall.

Data artifacts:

- `data/raw/mongo_test.test-result.json`: input JSON, **1,248 rows**, with some numeric values represented as strings;
- `data/processed/cleaned_data.json`: type-cleaned data, **1,248 rows**;
- `data/processed/scaled_data.json`: scaled **training features only**, **998 rows × 7 features**. It is neither the complete dataset nor the target data.

### 4. Feature engineering and model architecture

The preprocessing pipeline casts the batch size to integer, optionally averages measurements grouped by the four configuration/workload fields, separates input features from `throughput`, and uses an **80/20 split** (`random_state=42`): **998 train** and **250 test** examples. Since the supplied input already has unique configurations, averaging it does not reduce its row count.

`StandardScaler` transforms the three numerical predictors, while `OneHotEncoder` maps compression to four binary columns in a fixed `none`, `snappy`, `lz4`, `gzip` order; the resulting design matrix has **seven features**. The transformer is fitted to the training portion and then applied to the test portion. GPR also uses a separate target scaler and inverse transformation to report throughput in original units.

Four base estimators are compared: **Random Forest**, **XGBoost**, **Gaussian Process Regression (GPR)**, and **LightGBM**. The stacking notebook constructs out-of-fold training predictions with **five-fold cross-validation**, then trains a **Ridge meta-regressor (`alpha=1.0`)** to combine the predictors.

**Corrected Stacking implementation:** `notebooks/05_stacking.ipynb` now builds its Ridge meta-features from **Random Forest + XGBoost + GPR only**, in line with the report's final three-model description. The report's rationale for excluding LightGBM is its high correlation with XGBoost (`0.9860`). **LightGBM remains an independent comparison model** and does not feed into Ridge. The original four-model notebook, including its historical outputs, is retained at `notebooks/archive/05_stacking_4_models_legacy.ipynb`. The metrics below belong to the **historical four-model run**, **not** the corrected three-model design; run the corrected notebook to compute fresh test-set metrics and recommendation rankings.

### 5. Recorded evaluation results

| Model | R² ↑ | MAE ↓ (docs/sec) | RMSE ↓ (docs/sec) |
|---|---:|---:|---:|
| Random Forest | 0.8620 | 6666.97 | 10430.15 |
| XGBoost | 0.8824 | 6584.96 | 9627.00 |
| GPR | 0.8211 | 7565.13 | 11876.04 |
| LightGBM | 0.8556 | 7314.17 | 10667.87 |
| **Stacking (چهارمدلی، تاریخی / four-model legacy)** | **0.8849** | **6350.22** | **9527.09** |

**Legacy four-model Stacking** achieves the best archived test-set metrics, but its improvement over **XGBoost**, the strongest individual model, is modest. Metrics for the corrected three-model Stacking have not been recomputed in this package. GPR offers an uncertainty estimate; the sample `±0.3482` value is reported in **scaled-target units**, not documents per second. These values describe **predictive accuracy on the saved test split**, not experimentally demonstrated throughput gains in a new production deployment.

### 6. Repository layout

```text
MFT-Kafka-Ensemble-Optimization/
├── README.md                      # Persian + English documentation
├── requirements.txt               # Python dependencies
├── .gitignore
├── src/
│   └── kafka_preprocessing.py     # Reusable preprocessing + synthetic smoke test
├── notebooks/
│   ├── 01_random_forest.ipynb
│   ├── 02_xgboost.ipynb
│   ├── 03_gaussian_process_regression.ipynb
│   ├── 04_lightgbm.ipynb
│   ├── 05_stacking.ipynb      # پیاده‌سازی اصلی سه‌مدلی / three-model design
│   └── archive/
│       └── 05_stacking_4_models_legacy.ipynb
├── data/
│   ├── raw/
│   │   └── mongo_test.test-result.json
│   └── processed/
│       ├── cleaned_data.json
│       └── scaled_data.json
├── artifacts/
│   ├── models/                    # Saved RF, XGBoost, GPR, LightGBM estimators
│   ├── preprocessing/             # Saved input and target scalers
│   └── splits/                    # Saved train/test arrays and targets
└── docs/
    ├── reports/                   # Original research report: DOCX and PDF
    └── references/                # Five supporting research PDFs
```

All substantive original project files, model artifacts, datasets, both report formats, and references have been preserved. Only disposable local environment files (`.idea`, `.ipynb_checkpoints`, and `__pycache__`) were excluded. Notebook names are numbered for execution order, and report filenames have been standardized.

### 7. Installation and execution

**Prerequisites:** Python **3.11+** (Python **3.13** is recommended when using archived Python artifacts), the packages in `requirements.txt`, and Jupyter Notebook. Kafka/Java/databases are **not needed** to train on the provided JSON data; they would be required, together with missing producer/consumer application code, to recreate the experimental system.

Run from the **project root**:

```bash
# Linux / macOS
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
jupyter notebook notebooks/
```

```powershell
# Windows PowerShell
py -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
jupyter notebook notebooks/
```

**Notebook order** (run cells top to bottom):

1. `01_random_forest.ipynb` — train and save the RF model;
2. `02_xgboost.ipynb` — train and save the XGBoost model;
3. `03_gaussian_process_regression.ipynb` — train and save the GPR model;
4. `04_lightgbm.ipynb` — train LightGBM; persist scalers and train/test data;
5. `05_stacking.ipynb` — generate three out-of-fold meta-features, train a fresh three-input Ridge, evaluate, and recommend settings.

The first cell of each notebook resolves project-relative data, model, and preprocessing paths when Jupyter is opened from the **repository root or `notebooks/`**. Saved artifacts are included for convenience. However, the Ridge **meta-model is trained interactively** by the stacking notebook, and there is **no standalone `stacking_model.pkl`**. GPR and cross-validated stacking can be computationally expensive.

For a **synthetic-data preprocessing smoke test** (not a model benchmark), run:

```bash
python src/kafka_preprocessing.py
```

To preprocess the **provided experimental data** programmatically from the repository root:

```python
import sys
sys.path.insert(0, "src")
from kafka_preprocessing import preprocess

results = preprocess("data/raw/mongo_test.test-result.json")
X_train_s, X_test_s = results[0], results[1]
print(X_train_s.shape, X_test_s.shape)  # (998, 7), (250, 7)
```

### 8. Generating recommendations

Once `05_stacking.ipynb` has been executed, run this in the **same notebook session**:

```python
recommend_stacking(bill_size=309000, top_n=3)
```

The function evaluates the **416** candidates and prints their producer `batchSize`, `lingerMs`, `compressionType`, and `predicted_throughput`, sorted by predicted throughput. Individual notebooks also contain their respective `recommend`, `recommend_xgb`, `recommend_gpr`, and `recommend_lgbm` functions.

**Grid bug fix:** The original notebook recommendation grids incorrectly used **`23768`**, even though the training dataset uses **`32768`**. The search grids in this organized copy have been corrected to **`32768`**. The legacy notebook retains its historical recommendation outputs; rerun the corrected notebook for three-model recommendations. Recommendations for workload sizes outside the recorded three sizes rely on extrapolation and should be validated experimentally.

### 9. Reproducibility, caveats, and security

- The measurements cover a **limited configuration grid** and only **three workload sizes** on the reported environment; results may not generalize to another broker, hardware setup, software version, or message distribution.
- The report describes repeated measurements, but the individual repeated raw files and experimental Java code are **not included**. The original data collection cannot be reproduced from this archive alone.
- `05_stacking.ipynb` now implements the report's **three-model design**; the separate legacy notebook preserves **four-model results**. Do not report the old R²/MAE/RMSE as three-model scores without rerunning the corrected experiment.
- Model `.pkl` files depend on compatible Python/library versions. If loading fails, prefer rebuilding the artifacts in a suitable environment. **Never unpickle untrusted files:** joblib/pickle loading can execute code.
- A higher **predicted** throughput is not a guaranteed real deployment improvement. Also evaluate latency, resource use, robustness, and delivery requirements before operational rollout.
- No explicit open-source license was supplied with the original project; do not assume unrestricted redistribution or reuse.

### 10. Reports and attribution

The original Persian research report is available as [`PDF`](docs/reports/project-report-fa.pdf) and [`Word`](docs/reports/project-report-fa.docx). Five supporting paper PDFs are in [`docs/references/`](docs/references/). The report credits **Mohammad Amin Mohammadi Tabar**, a **Machine Learning with Python** course project, dated **Mehr 1405** (Iranian calendar). Consult the report for the theoretical background, experimental discussion, and full tables.
