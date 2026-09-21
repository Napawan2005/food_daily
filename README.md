# food_daily

End-to-end data pipeline สำหรับ dataset **Online Food Delivery** — Airflow → MinIO → ClickHouse → dbt

Dataset: [food_daily.csv](https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/blob/main/food_daily.csv)

## Business Scenario

**โจทย์:** rating เฉลี่ยของแอปต่ำ (rating 2 มากที่สุด ~32% ของ order) ทีม dev อยากรู้ว่าควรแก้ตรงไหนก่อน เพื่อนำไปเสนอ CEO ขอ budget พัฒนา

**วิธีคิด:** ใช้ feedback ของลูกค้าเป็นตัวชี้ทาง

1. __จัดหมวดหมู่ feedback__ — map ข้อความ feedback เข้าหมวด เช่น Food, Delivery, App & System, Price (`int_feedback_category`)
2. __หาหมวดที่ถูกพูดถึงมากที่สุด__ — นับ order ต่อหมวด + rating เฉลี่ยต่อหมวด (`mart_ratings_distribution`) → บอกได้ว่าปัญหาใหญ่อยู่ที่ร้าน / ขนส่ง / แอป
3. __เจาะหมวดนั้นเป็น positive / negative__ — ดูข้อความ feedback ที่ซ้ำกันมากที่สุดในแต่ละฝั่ง (`mart_feedback_top_platform_app`)
   - negative → สิ่งที่ต้องแก้ (เช่น "Difficult to order", "Complicated procedure")
   - positive → สิ่งที่ทำดีอยู่แล้ว ต้องรักษาไว้ (เช่น "Easy to order")

**ผลลัพธ์ที่ส่งให้ CEO:** หมวดที่ควรลงทุนก่อน 1 หมวด พร้อมรายการปัญหา top-N ที่ลูกค้าพูดถึงมากที่สุด และคาดว่าถ้าแก้แล้ว rating จะขยับจากกลุ่มไหน

![Dashboard: feedback by category](docs/dashboard.png)

link : [dashboard.png](https://datastudio.google.com/s/o7FIRzjEYng)

**dbt models ที่รองรับ**

| ขั้น | model |
|---|---|
| จัดหมวด feedback | `int_feedback_category` |
| นับ / rating ต่อหมวด | `mart_ratings_distribution` |
| top feedback positive / negative ในหมวด | `mart_feedback_top_platform_app` |
| รายละเอียดราย order (drill-down) | `mart_order_feedback_detail` |

## Architecture

```ini
dataset/food_daily.csv
   │  extract_validate_csv        เช็ค column ครบ
   ▼
MinIO  raw-food-daily/<date>/food_daily.csv
   │  convert_csv_to_parquet
   ▼
MinIO  parquet-food-daily/<date>/food_daily.parquet
   │  create_food_daily_table + load_data_to_food_daily  (ClickHouse s3() → INSERT)
   ▼
ClickHouse  food_daily (raw table)
   │  dbt run + dbt test   (รันแยกผ่าน service `dbt` หรือ manual)
   ▼
ClickHouse  staging → intermediate → facts/dims → marts
```

ทั้งหมดอยู่ใน DAG เดียว `food_daily_pipeline` (`dags/food_daily.py`, `@daily`) — dbt ไม่ได้อยู่ใน DAG

## Project Structure

```ini
food_daily/
├── docker-compose.yml              # ClickHouse, MinIO, Postgres, Airflow
├── Dockerfile.airflow              # Airflow image + dbt-clickhouse, pandas, pyarrow, amazon provider
├── .env.example                    # template → copy เป็น .env
├── dataset/
│   └── food_daily.csv              # CSV ต้นทาง
├── airflow_config/
│   └── simple_auth_manager_passwords.json.generated   # password ที่ Airflow generate (gitignore)
├── TROUBLESHOOTING.md              # ปัญหาที่เจอบ่อย + วิธีแก้
├── dags/                           # Airflow
│   ├── food_daily.py               # DAG food_daily_pipeline: CSV → MinIO raw → parquet → ClickHouse (@daily)
│   ├── minio_client.py             # helper boto3: get_minio_client, list_minio_buckets
│   └── tests/                      # pytest ของ DAG/helper
└── food_daily/                     # dbt project (mount เป็น /opt/airflow/dbt)
    ├── dbt_project.yml
    ├── profiles.yml                # อ่าน CLICKHOUSE_* จาก env
    ├── packages.yml
    ├── models/
    │   ├── staging/                # stg_food_daily__order + sources.yml
    │   ├── intermediate/           # int_feedback_category (+ csv mapping)
    │   ├── facts/                  # fct_orders
    │   │   └── dimenstions/        # dim_category, dim_order_status, dim_payment_mode, dim_restaurant
    │   └── marts/                  # mart_order_feedback_detail, mart_ratings_distribution, mart_feedback_top_platform_app
    └── tests/                      # singular tests (assert_*.sql)
```

## Setup

```sh
# 1. env
cp .env.example .env            # แก้ค่า change_me_* ทุกตัว

# 2. build image (ทำครั้งแรก และทุกครั้งที่แก้ Dockerfile.airflow)
docker compose build

# 3. start ทั้งหมด
docker compose up -d

# 4. เช็คว่าขึ้นครบ (airflow-init, minio-bootstrap จะ Exited 0 — ปกติ)
docker compose ps
```

รอ ~1 นาทีให้ Airflow พร้อม

## Credentials

| service | URL | user / password |
|---|---|---|
| Airflow UI | http://localhost:8080 | `AIRFLOW_ADMIN_USER` / ดูใน `airflow_config/simple_auth_manager_passwords.json.generated` |
| MinIO console | http://localhost:9002 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| ClickHouse HTTP | http://localhost:8123 | `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` |
| dbt | — | ใช้ค่า ClickHouse เดียวกัน (อ่านจาก env ผ่าน `profiles.yml`) |

หมายเหตุ

- Airflow (SimpleAuthManager) __generate password เอง__ ทุกครั้งที่สร้าง container ใหม่ — `AIRFLOW_ADMIN_PASSWORD` ใน `.env` ไม่ได้ถูกใช้ ดูค่าจริงด้วย `cat airflow_config/simple_auth_manager_passwords.json.generated`
- ClickHouse password ถูกตั้งตอนสร้าง volume ครั้งแรกเท่านั้น แก้ `.env` ทีหลังไม่มีผล ต้อง `docker compose down -v` (ข้อมูลหาย) แล้ว up ใหม่

## Run pipeline

1. เปิด Airflow UI → unpause `food_daily_pipeline` → trigger
2. เช็คผล: MinIO มีไฟล์ใน `raw-food-daily/` และ `parquet-food-daily/`, ClickHouse มี table `food_daily` ใน `CLICKHOUSE_DB`
3. รัน dbt (ข้อถัดไป) เพื่อสร้าง staging → marts

## dbt

```sh
# ผ่าน container Airflow (มี dbt-clickhouse ติดตั้งแล้ว, mount ที่ /opt/airflow/dbt)
docker compose exec airflow_scheduler bash -c "cd /opt/airflow/dbt && dbt deps && dbt run && dbt test"
```

> service `dbt` ใน `docker-compose.yml` mount `./dbt/food_daily` ซึ่งไม่มีอยู่ (โปรเจกต์ dbt จริงคือ `./food_daily`) — ใช้คำสั่งข้างบนแทน หรือแก้ path ใน compose

## Stop / reset

```sh
docker compose down          # หยุด เก็บข้อมูล
docker compose down -v       # หยุด + ลบข้อมูลทั้งหมด (ClickHouse, MinIO, Airflow DB)
```

## Reflection

### What did you learn from this project?

- เข้าใจคร่าว ๆ ว่า Airflow, dbt และ MinIO ต้องใช้ Docker image อะไรบ้าง
- เรียนรู้การใช้ Airflow แบบ DAG เดียว และการแยกออกเป็นหลาย DAG แล้วต่อกันด้วย Asset
- เข้าใจว่า data pipeline ที่ดีควรมี flow แบบไหน (ingest → raw → parquet → warehouse → dbt)
- เรียนรู้ flow การอัปโหลดข้อมูลขึ้น MinIO และการโหลดข้อมูลเข้า ClickHouse
- เรียนรู้วิธีคิดในการแบ่ง layer ของ dbt (staging → intermediate → facts/dims → marts) ว่าแต่ละชั้นควรทำหน้าที่อะไร

### How would you improve it?

- ใช้ NLP มาช่วยจัดหมวดหมู่ feedback และวิเคราะห์ sentiment แทนการ map ด้วย keyword
- เพิ่ม data quality check ฝั่ง raw (schema, row count) ก่อนเข้า dbt

### If you had to do it all over again, what would you do differently?

- เลือก dataset ที่ซับซ้อนและมีข้อมูลเยอะกว่านี้ เพื่อให้ได้ลองใช้ tool ใหม่ ๆ
- นำ AI/ML เข้ามาใช้ในการวิเคราะห์มากขึ้น เพื่อทบทวนความเข้าใจของตัวเอง
- มีการรีเฟคเตอร์ของของ airflow เพื่อมีการแยก dag แต่ละหน้าที่ เช่น dag ของ miniO ก็ทำส่วนการ upload csv -> parquet clickhouse ก็ทำส่วน upload ข้อมูลใน clickhouse
- มีการวางแผนที่ชัดเจนเพื่อประหยัดเวลาในการเขียนเพื่อไม่ใช่หลง scope งาน
