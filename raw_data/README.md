# raw_data/

ไฟล์ log ที่ pipeline จะโหลดเข้า `raw.event_logs` (รับ `*.json`, `*.json.gz`, `*.jsonl`, `*.csv` ทุกไฟล์ในโฟลเดอร์นี้)

| ไฟล์ | ที่มา |
|---|---|
| `sparkify_events_sample.json.gz` | ข้อมูลจำลอง 250 users / ~188k events สร้างด้วย `scripts/generate_sample_data.py` (schema เดียวกับของจริง) |

## ใช้ข้อมูลจริง (Udacity Sparkify)
1. ดาวน์โหลด `mini_sparkify_event_data.json` (~128 MB) จาก Kaggle (ค้นหา "Sparkify mini dataset") หรือ S3 สาธารณะของ Udacity
   `https://udacity-dsnd.s3.amazonaws.com/sparkify/mini_sparkify_event_data.json`
2. วางไฟล์ไว้ที่โฟลเดอร์นี้ และ **ย้ายไฟล์ sample ออก** (ไม่งั้นจะโหลดทั้งสองไฟล์ปนกัน)
3. Trigger DAG `sparkify_churn_pipeline` ใหม่ — ไม่ต้องแก้โค้ด
