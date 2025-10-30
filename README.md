## Reminder Bot (Jira → FPT Chat)

### Mục đích
- Tự động quét Jira theo lịch và gửi nhắc việc qua FPT Chat theo các luật đã yêu cầu.

### Tính năng (đúng theo yêu cầu)
- Quét Jira theo lịch (mặc định 15 phút) và áp dụng các rule:
  - CI Testing nhưng chưa có worklog trong X phút (config) → nhắc Assignee.
  - Thiếu Description → nhắc Reporter.
  - Có Fix Version/s:
    - Trước ngày release N ngày (config), chưa lên UAT → nhắc Assignee.
    - Sau release date, chưa lên Production → cảnh báo Assignee (+ Leader nếu cấu hình mapping).
- Lịch sử gửi để tránh gửi trùng (CSV + TTL), có thể gửi lại sau Y giờ (config).
- Gửi FPT Chat ưu tiên theo email (userEmails), nếu không được sẽ fallback sang groupId (mapping trong file CSV).

### Công nghệ
- Python 3.10+
- Thư viện: requests, python-dotenv, pandas, APScheduler, loguru, pytest

### Kiến trúc & Vai trò từng file
- `jira_utils.py`: Kết nối Jira (REST API), tìm issue mới cập nhật, chuẩn hóa dữ liệu về task object gồm: `key, summary, assignee_email, reporter_email, status, description, fixVersions, fixVersion_dates, has_worklog, last_status_changed_at, is_uat_done, is_production, task_url`.
- `rules.py`: 4 rule evaluator trả về mã sự kiện: `missing_logtime, missing_description, pre_version_reminder, post_version_alert`. Có unit tests.
- `chat_api.py`: Gọi API FPT Chat theo đúng định dạng bạn cung cấp. Thử gửi theo `userEmails` trước; nếu thất bại và có `groupId` thì fallback gửi theo `groupId`.
- `reminder_bot.py`: Luồng chính: tải config, lấy danh sách task, áp dụng rule, build message, gửi chat, ghi lịch sử để chống trùng, hỗ trợ chạy `--once`.
- `scheduler.py`: Chạy định kỳ bằng APScheduler theo `SCHEDULE_INTERVAL_MINUTES` hoặc `SCHEDULE_CRON`.
- `logger.py`: Cấu hình log chuẩn.
- `rules_config.json`: Tham số rule: `check_interval_minutes, ci_testing_wait_minutes, pre_version_days, domains_allowed, resend_after_hours`.
- `requirements.txt`: Danh sách dependency.
- `employees.csv` (tùy chọn): mapping `email → groupId` (điền vào cột `chat_id`) để fallback khi gửi theo email không được.

### Cài đặt
1) Tạo file `.env` từ `.env.example` và điền giá trị phù hợp:
   - `JIRA_URL, JIRA_USERNAME, JIRA_TOKEN`
   - `SCHEDULE_INTERVAL_MINUTES` hoặc `SCHEDULE_CRON`
   - `EMPLOYEES_FILE` (mặc định `employees.csv`)
   - `JIRA_PROJECTS` (ví dụ `FC,FSS`)
   - `REMINDER_HISTORY_FILE` (mặc định `data/reminder_logs.csv`)
   - `FPT_CHAT_BASE_URL` (vd: `https://api-chat.fpt.com/bot-external-api/ext-bot`)
   - `FPT_CHAT_BOT_ID` (vd: `6891cd78e10685dd16c0192b%3A1af870730fb1d7afca1df39f2155eca0`)

2) (Tùy chọn) Chuẩn bị `employees.csv` với cột: `email, chat_id`
   - `email`: email trên Jira của người nhận.
   - `chat_id`: groupId của nhóm/phòng ban để fallback khi gửi theo email không được.

3) Cài dependency:
```bash
pip install -r requirements.txt
```

### Chạy thử một lần
```bash
python reminder_bot.py --once
```
Kết quả mong đợi: log hiện “Attempts: X, Sent: Y” và file lịch sử tại `data/reminder_logs.csv` được ghi lại.

### Chạy theo lịch
```bash
python scheduler.py
```
- Ưu tiên `SCHEDULE_CRON` nếu có; nếu không có sẽ dùng `SCHEDULE_INTERVAL_MINUTES`.

### Cơ chế gửi FPT Chat
- Endpoint: `${FPT_CHAT_BASE_URL}/bot/${FPT_CHAT_BOT_ID}/send-message`
- Thứ tự gửi:
  1) `userEmails`: `["email@fpt.com"]`
  2) Fallback `groupId`: từ cột `chat_id` trong `employees.csv`

### Tránh gửi trùng & TTL
- Lưu `task_key, rule_type, to, sent_at, status, response` vào CSV `REMINDER_HISTORY_FILE`.
- Không gửi lại cùng (task, rule, người nhận) trong khoảng `resend_after_hours` (config trong `rules_config.json`).

### Tiêu chí nghiệm thu (Acceptance)
- `python reminder_bot.py --once` chạy được: lấy task, evaluate rule, thử gửi, ghi log và lịch sử.
- `scheduler.py` chạy được theo khoảng thời gian cấu hình.
- Lịch sử ngăn trùng lặp theo TTL.
- Nếu gửi theo email không được, bot fallback sang `groupId` đúng như API bạn cung cấp.

