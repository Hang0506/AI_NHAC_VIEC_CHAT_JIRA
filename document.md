'
# Tài liệu nhiệm vụ: get_lc_tasks_with_worklog_final copy.py

## Mục đích
Script lấy task từ Jira theo khoảng thời gian, phân tích worklog/changelog, hợp nhất dữ liệu cha–con và xuất báo cáo CSV/TXT (theo nhân viên, dự án, component, dự án thực tế) để theo dõi logwork và hiệu suất.

## Luồng tổng quát
1. Đọc `.env` (Jira URL/username/password) và file Excel nhân viên.
2. Hỏi tham số lọc (ngày, dự án Jira, trạng thái, loại issue, v.v.).
3. Với từng nhân viên: gọi Jira (search, worklog, changelog), tính giờ, estimate, tiết kiệm; gán dự án thực tế; lưu CSV/TXT.
4. Hợp nhất toàn bộ task; cập nhật task cha dựa trên sub-task; lọc cha nếu con không có update (tuỳ chọn).
5. Xuất báo cáo tổng hợp (CSV/TXT) theo dự án Jira, dự án thực tế, component và nhân viên.

## Hàm chính
- get_worklog(): lấy worklog + project/parent.
- get_update_reason(): lấy changelog, tìm cập nhật quan trọng, người cập nhật cuối.
- get_employee_tasks(): dựng JQL, gọi API, lọc, lấy worklog/changelog, tính giờ/tiết kiệm, gán actual_project.
- update_story_worklog_from_subtasks(): dồn logwork/estimate từ con lên cha khi cần.
- filter_parent_tasks_without_updated_children(): loại cha nếu toàn bộ con không có update.
- get_actual_project(): map Jira project + components về "dự án thực tế" (PKT→Kho Tổng + PIM; WAK→Web App KHLC; PPFP→Payment FPT Pay; FSS→Noti + Loyalty + Core Cust; FC tách theo component).
- create_employee_detailed_report(): báo cáo TXT theo nhân viên.
- create_project_report(): báo cáo TXT theo dự án thực tế.
- create_projects_summary_report(): báo cáo tổng hợp TXT/CSV cho tất cả dự án.
- check_consistency(): đối chiếu số liệu giữa tổng hợp và chi tiết dự án.
- synchronize_reports(): đồng bộ số liệu báo cáo (nếu dùng).
- main(): điều phối toàn bộ quy trình và ghi log tiến trình.

## Đầu vào
- `.env`: JIRA_URL, JIRA_USERNAME, JIRA_PASSWORD.
- Excel nhân viên: `resource/projects_employees.xlsx` (cần cột EMAIL; nên có NAME, SKILL_GROUP, PROJECTNAME).
- Tham số nhập qua console: ngày, dự án Jira include/exclude, status/type filter, chỉ lấy cập nhật assignee, chỉ lấy thay đổi status, bỏ qua Fix Version/Sprint, delay request, lọc cha không có update con, danh sách email/SKILL_GROUP loại trừ, v.v.

## Đầu ra (thư mục `data/tasks/`)
- CSV tổng hợp task: `*_summary_YYYYMMDD_HHMMSS.csv`
- TXT báo cáo tổng quan: `*_report_YYYYMMDD_HHMMSS.txt`
- TXT log: `*_log_YYYYMMDD_HHMMSS.txt`
- CSV worklog chi tiết: `*_hours_YYYYMMDD_HHMMSS.csv`
- CSV thống kê theo dự án Jira: `*_project_stats_*.csv`
- CSV thống kê theo dự án thực tế: `*_actual_project_stats_*.csv`
- CSV thống kê theo component: `*_component_stats_*.csv`
- TXT cây phân cấp task–subtask: `*_hierarchy_*.txt`
- TXT/CSV báo cáo theo dự án thực tế (thư mục con `project_reports/`), và file cá nhân theo nhân viên.

## Lưu ý
- `time_saved_hours`: -1 (không có logwork), -2 (có logwork nhưng không có estimate).
- Bộ lọc "bỏ qua Fix Version/Sprint/RemoteIssueLink/Components" dựa trên changelog để tìm cập nhật có ý nghĩa.
- Task cha được cập nhật từ con nếu cha thiếu logwork/estimate, đảm bảo báo cáo phản ánh đúng thực tế.

## Cách chạy
```bash
python "get_lc_tasks_with_worklog_final copy.py"
```
Chuẩn bị `.env` và Excel nhân viên; trả lời các câu hỏi lọc trong console.
'