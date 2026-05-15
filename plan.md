# Plan: Hoàn thành Lab Sales Data Quality Pipeline

## Tổng quan

Lab yêu cầu xây dựng một Apache Airflow DAG đọc CSV đơn hàng, validate dữ liệu, sinh file JSON summary, gửi thông báo Discord, và fail pipeline nếu có lỗi.

## Hiện trạng starter code

| File | Trạng thái |
|---|---|
| `src/config.py` | ✅ Hoàn chỉnh — paths, valid statuses, webhook URL, env vars |
| `src/validation.py` | ✅ Hoàn chỉnh — `read_rows`, `build_summary`, `write_summary`, `send_discord_message`, `run_lab_check` |
| `scripts/run_local_check.py` | ✅ Hoàn chỉnh — CLI wrapper chạy không cần Airflow |
| `data/orders_passed.csv` | ✅ 10 rows sạch |
| `data/orders_failed.csv` | ✅ 10 rows có lỗi (missing customer_id, amount <= 0, invalid status) |
| `expected/validation_summary_passed.json` | ✅ Expected output |
| `expected/validation_summary_failed.json` | ✅ Expected output |
| `dags/sales_data_quality_pipeline.py` | ❌ `validate_orders_task()` chỉ có `NotImplementedError` |

## Các bước thực hiện

### Bước 1 — Implement `validate_orders_task` trong DAG

File: `starter_project/dags/sales_data_quality_pipeline.py`

Logic cần viết trong hàm `validate_orders_task`:

1. Lấy đường dẫn input CSV từ biến môi trường `AIRFLOW_INPUT_FILE` (config đã define sẵn)
2. Gọi `run_lab_check(input_path, allow_failure=False, skip_discord=False)` từ `src.validation`
3. `run_lab_check` sẽ tự động: đọc CSV → validate → ghi JSON → gửi Discord → raise `LabValidationError` nếu failed

### Bước 2 — Chạy local check với cả 2 datasets

```bash
# Passing dataset
python3 starter_project/scripts/run_local_check.py starter_project/data/orders_passed.csv --skip-discord

# Failing dataset
python3 starter_project/scripts/run_local_check.py starter_project/data/orders_failed.csv --allow-failure --skip-discord
```

### Bước 3 — So sánh output với expected

File sinh ra tại `starter_project/output/validation_summary.json`.
So sánh với `starter_project/expected/validation_summary_passed.json` và `validation_summary_failed.json`.

### Bước 4 — Test với Airflow (nếu có môi trường)

Set env `AIRFLOW_INPUT_FILE` trước khi trigger DAG để chọn dataset.

### Bước 5 — Kiểm tra Discord webhook

Bỏ flag `--skip-discord` và đảm bảo `DISCORD_WEBHOOK_URL` đã được set.

## Cấu trúc deliverable

```
starter_project/
├── dags/
│   └── sales_data_quality_pipeline.py   # DAG hoàn chỉnh
├── data/
│   ├── orders_failed.csv                # Dataset lỗi
│   └── orders_passed.csv                # Dataset sạch
├── output/
│   └── validation_summary.json          # Generated tự động
└── src/
    ├── config.py                        # Config hoàn chỉnh
    └── validation.py                    # Logic validation hoàn chỉnh
```

## Rubric mapping

| Tiêu chí | Điểm | Cách đạt |
|---|---|---|
| 1. DAG Structure | 2 | DAG tên `sales_data_quality_pipeline`, schedule=None, có PythonOperator |
| 2. CSV Ingestion | 2 | Dùng `read_rows` + `csv.DictReader` đọc đủ 4 cột |
| 3. Validation Logic | 3 | Kiểm tra customer_id trống, amount <= 0, status không hợp lệ |
| 4. Output JSON | 1 | Ghi file JSON tự động với đúng schema |
| 5. Discord Alert | 1 | Gửi webhook thành công/failure rõ ràng |
| 6. Failure Handling | 1 | Raise exception khi failed nhưng vẫn giữ file summary |

## Lưu ý

- Không được tự tạo `validation_summary.json` bằng tay — file phải được sinh tự động
- Dùng `AIRFLOW_INPUT_FILE` env var để chọn dataset khi chạy trên Airflow
- Khi test local: dùng `--skip-discord` nếu chưa có webhook, dùng `--allow-failure` với dataset lỗi
