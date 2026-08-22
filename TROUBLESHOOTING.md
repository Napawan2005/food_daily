# Troubleshooting: Airflow UI (localhost:8080) ไม่ขึ้น

## อาการ
เปิด `http://localhost:8080` เพื่อเข้า Airflow UI ไม่ได้

## สาเหตุ
Container `airflow-webserver` หลุดออกจาก Docker network `food_daily_default`
(ตรวจสอบด้วย `docker inspect airflow-webserver --format '{{json .NetworkSettings.Networks}}'`
แล้วพบว่าเป็น `{}` ทั้งที่ควรอยู่ใน `food_daily_default` เหมือน container อื่นๆ)

แม้ port mapping `8080:8080` ใน `docker-compose.yml` จะถูกต้อง แต่เพราะไม่มี network
webserver จึง resolve hostname `airflow_postgres` ไม่ได้:

```
sqlalchemy.exc.OperationalError: (psycopg2.OperationalError) could not translate host name "airflow_postgres" to address: Temporary failure in name resolution
```

ทำให้เชื่อมต่อฐานข้อมูลไม่ได้ และ process ภายใน container crash วนซ้ำ
ผลคือไม่มีอะไรฟังอยู่ที่ port 8080 จริง แม้ `docker ps` จะโชว์สถานะ container เป็น "running"

## วิธีตรวจสอบ

```bash
# เช็คว่า container ไหนหลุด network
docker compose ps --format '{{.Name}}: {{.Networks}}'

# ดู log ว่า crash เพราะอะไร
docker logs airflow-webserver --tail 40
```

## วิธีแก้

Recreate container ให้กลับเข้า network ที่ถูกต้อง:

```bash
docker compose up -d --force-recreate airflow_webserver
```

## วิธียืนยันว่าใช้ได้แล้ว

```bash
curl -sI localhost:8080
```
ได้ response กลับมา (เช่น `405 Method Not Allowed` บน HEAD ถือว่าปกติ เพราะ endpoint
รองรับเฉพาะ GET) แปลว่า server ตอบสนองแล้ว เปิดผ่าน browser ด้วย GET จะใช้งานได้ปกติ

## Login เข้า Airflow UI

- Username: `admin`
- Password: ดูได้จากไฟล์ `airflow_config/simple_auth_manager_passwords.json.generated`

⚠️ **ข้อควรระวัง**: ไฟล์ password ที่ generate ขึ้นมานี้เป็น credential จริง
ต้องเพิ่มเข้า `.gitignore` ก่อน commit เพื่อไม่ให้หลุดเข้า repository
