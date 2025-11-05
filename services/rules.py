from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
import re

# Timezone Việt Nam
VN_TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")

def get_vn_now() -> datetime:
    """Lấy datetime hiện tại theo giờ Việt Nam."""
    return datetime.now(VN_TIMEZONE)

# Rule identifiers
# Rule: Kiểm tra task ở status "READY CI TESTING" nhưng chưa có worklog sau X phút
MISSING_LOGTIME = "missing_logtime"

# Rule: Kiểm tra task không có description hoặc description rỗng
MISSING_DESCRIPTION = "missing_description"

# Rule: Nhắc nhở trước khi release - task có fixVersion và ngày release trong vòng X ngày tới
PRE_VERSION_REMINDER = "pre_version_reminder"

# Rule: Cảnh báo sau khi release - ngày release đã qua nhưng task chưa ở status Complete
POST_VERSION_ALERT = "post_version_alert"

# Rule: Cảnh báo khi assignee được thay đổi trong vòng X phút gần đây
ASSIGNEE_CHANGED = "assignee_changed"

# Rule: Cảnh báo task quá hạn due date
DUE_DATE_OVERDUE = "due_date_overdue"

# Rule: Cảnh báo task mới được tạo trong vòng X phút gần đây
RECENTLY_CREATED = "recently_created"

# Test email list - chỉ test với các email này (tạm thời)
TEST_EMAILS = [
    "hangnt60@fpt.com",
    "nhutlm1@fpt.com",
    "haovtm2@fpt.com",
    "tuttc8@fpt.com",
    "phucnvh6@fpt.com",
    "quyennt94@fpt.com",
    "loinp5@fpt.com",
    "kieuntd@fpt.com",
    "minhnlv2@fpt.com"
]
TEST_MODE_ENABLED = len(TEST_EMAILS) > 0  # Chỉ bật test mode khi có email trong danh sách; rỗng = chạy all

# Cấu hình project bị loại trừ cho từng rule
# Mỗi rule có thể có danh sách project không được áp dụng rule đó
RULE_EXCLUDED_PROJECTS = {
    MISSING_LOGTIME: ["IPTPE,TADS"],  # Ví dụ: ["FC", "FSS"] - rule này sẽ không chạy với tasks thuộc project FC và FSS
    MISSING_DESCRIPTION: [],
    PRE_VERSION_REMINDER: ["IPTPE,TADS"],
    POST_VERSION_ALERT: ["IPTPE,TADS"],
    ASSIGNEE_CHANGED: [],
    DUE_DATE_OVERDUE: [],
    RECENTLY_CREATED: [],
}

# Cấu hình project được phép áp dụng (whitelist) cho từng rule
# Nếu danh sách rỗng -> áp dụng cho tất cả project (mặc định)
RULE_INCLUDED_PROJECTS = {
    MISSING_LOGTIME: [],
    MISSING_DESCRIPTION: [],
    PRE_VERSION_REMINDER: [],
    POST_VERSION_ALERT: [],
    ASSIGNEE_CHANGED: [],
    DUE_DATE_OVERDUE: ["IPTPE,TADS"],
    RECENTLY_CREATED: [],
}


def is_project_excluded(project: str, rule_code: str) -> bool:
    """Kiểm tra project có bị loại trừ cho rule không.
    
    Args:
        project: Project key của task (ví dụ: "FC", "FSS")
        rule_code: Mã rule (ví dụ: MISSING_LOGTIME)
    
    Returns:
        True nếu project bị loại trừ (rule không chạy), False nếu không
    """
    if not project:
        return False
    
    excluded_projects = RULE_EXCLUDED_PROJECTS.get(rule_code, [])
    if not excluded_projects:
        return False
    
    project_upper = str(project).strip().upper()
    excluded_upper = [p.upper() for p in excluded_projects]
    return project_upper in excluded_upper


def is_project_allowed(project: str, rule_code: str) -> bool:
    """Kiểm tra project có nằm trong danh sách cho phép (nếu có cấu hình) của rule không.
    - Nếu danh sách allow rỗng: cho phép tất cả (trả về True)
    - Nếu có danh sách: chỉ cho phép project nằm trong danh sách
    """
    included_projects = RULE_INCLUDED_PROJECTS.get(rule_code, [])
    # Không cấu hình -> allow all
    if not included_projects:
        return True
    if not project:
        return False
    project_upper = str(project).strip().upper()
    included_upper = [p.upper() for p in included_projects]
    return project_upper in included_upper


def is_test_email(email: str) -> bool:
    """Kiểm tra email có trong danh sách test không.
    Nếu TEST_EMAILS không trống, chỉ cho phép email trong danh sách.
    Nếu TEST_EMAILS trống, cho phép tất cả (auto chạy all, không filter theo email).
    """
    if not email:
        return False
    # Nếu danh sách test trống, cho phép tất cả
    if not TEST_EMAILS:
        return True
    # Chỉ cho phép email trong danh sách test
    email_lower = email.strip().lower()
    return any(test_email.lower() == email_lower for test_email in TEST_EMAILS)


def _log_task_preview(prefix, task):
    try:
        preview = json.dumps(task, ensure_ascii=False, default=str)
    except Exception as e:
        print(f"{prefix} task_preview=<unserializable> error={e}")
        return
    max_len = 2000
    if len(preview) > max_len:
        preview = preview[:max_len] + "...(truncated)"
    print(f"{prefix} task_preview={preview}")


def _parse_date_from_fixversion_name(name: str):
    """
    Parse date từ tên fixVersion.
    Tên có dạng: "ICT release/20251112-v2.1.2.45" hoặc "release/20251105-v8.14.4-lc"
    Trả về datetime object nếu tìm thấy, None nếu không.
    """
    if not name:
        print(f"[Rules] _parse_date_from_fixversion_name: name is empty")
        return None
    
    # Normalize: strip whitespace và convert to string
    name_str = str(name).strip()
    print(f"[Rules] _parse_date_from_fixversion_name: searching pattern in name={repr(name_str)}")
    
    # Tìm pattern 8 chữ số liên tiếp (YYYYMMDD)
    match = re.search(r'(\d{8})', name_str)
    if not match:
        print(f"[Rules] _parse_date_from_fixversion_name: no 8-digit pattern found in {repr(name_str)}")
        return None
    
    date_str = match.group(1)  # YYYYMMDD
    print(f"[Rules] _parse_date_from_fixversion_name: found date_str={repr(date_str)}")
    try:
        # Parse YYYYMMDD thành datetime
        release_date = datetime.strptime(date_str, "%Y%m%d")
        print(f"[Rules] _parse_date_from_fixversion_name: parsed successfully -> {release_date}")
        return release_date
    except Exception as e:
        print(f"[Rules] _parse_date_from_fixversion_name: parsing failed -> {e}")
        return None


def evaluate_missing_logtime(task, ci_testing_wait_minutes):
    _log_task_preview("[Rules] evaluate_missing_logtime input:", task)
    project_key = task.get('project')
    if not is_project_allowed(project_key, MISSING_LOGTIME):
        print(f"[Rules] evaluate_missing_logtime: project {project_key} is not allowed -> SKIP")
        return None
    if is_project_excluded(project_key, MISSING_LOGTIME):
        print(f"[Rules] evaluate_missing_logtime: project {project_key} is excluded -> SKIP")
        return None
    status_raw = task.get('status')
    status_norm = (status_raw or "").strip().upper()
    print(f"[Rules] evaluate_missing_logtime: key={task.get('key')} project={project_key} status={status_raw} (norm={status_norm}) has_worklog={task.get('has_worklog')} last_status_changed_at={task.get('last_status_changed_at')} wait={ci_testing_wait_minutes}m")
    # Chấp nhận các biến thể như "READY CI TESTING", "IN CI TESTING"...
    if "READY CI TESTING" not in status_norm:
        print("[Rules] -> skip: status does not contain 'CI TESTING'")
        return None
    if task.get("has_worklog"):
        print("[Rules] -> skip: already has worklog")
        return None
    changed_at_str = task.get("last_status_changed_at")
    if not changed_at_str:
        print("[Rules] -> hit: no last_status_changed_at")
        return MISSING_LOGTIME
    try:
        changed_at = datetime.strptime(changed_at_str, "%Y-%m-%dT%H:%M:%S%z")
    except Exception:
        # Fallback: try without tz
        try:
            changed_at = datetime.strptime(changed_at_str, "%Y-%m-%dT%H:%M:%S")
        except Exception:
            print("[Rules] -> hit: cannot parse last_status_changed_at")
            return MISSING_LOGTIME
    now = get_vn_now()
    if changed_at.tzinfo:
        now = now.astimezone(changed_at.tzinfo)
    if now - changed_at >= timedelta(minutes=ci_testing_wait_minutes):
        print("[Rules] -> hit: exceeded wait window")
        return MISSING_LOGTIME
    print("[Rules] -> no hit")
    return None


def evaluate_missing_description(task):
    _log_task_preview("[Rules] evaluate_missing_description input:", task)
    project_key = task.get('project')
    if not is_project_allowed(project_key, MISSING_DESCRIPTION):
        print(f"[Rules] evaluate_missing_description: project {project_key} is not allowed -> SKIP")
        return None
    if is_project_excluded(project_key, MISSING_DESCRIPTION):
        print(f"[Rules] evaluate_missing_description: project {project_key} is excluded -> SKIP")
        return None
    task_key = task.get('key')
    print(f"[Rules] evaluate_missing_description START: key={task_key} project={project_key}")
    
    description = task.get("description")
    if description is None:
        print(f"[Rules] evaluate_missing_description END: key={task_key} -> one HIT")
        return MISSING_DESCRIPTION
    
    description_str = str(description)
    
    description_stripped = description_str.strip()
    print(f"[Rules]   step 3: strip whitespace -> {repr(description_stripped)}")
    print(f"[Rules]   step 3: length after strip={len(description_stripped)}")
    
    if description_stripped == "":
        print(f"[Rules]   step 4: description is empty string -> HIT")
        print(f"[Rules] evaluate_missing_description END: key={task_key} -> HIT (empty)")
        return MISSING_DESCRIPTION
    
    print(f"[Rules]   step 4: description has content -> NO HIT")
    print(f"[Rules] evaluate_missing_description END: key={task_key} -> NO HIT")
    return None


def evaluate_pre_version_reminder(task, pre_version_days):
    _log_task_preview("[Rules] evaluate_pre_version_reminder input:", task)
    project_key = task.get('project')
    if not is_project_allowed(project_key, PRE_VERSION_REMINDER):
        print(f"[Rules] evaluate_pre_version_reminder: project {project_key} is not allowed -> SKIP")
        return None
    if is_project_excluded(project_key, PRE_VERSION_REMINDER):
        print(f"[Rules] evaluate_pre_version_reminder: project {project_key} is excluded -> SKIP")
        return None
    task_key = task.get('key')
    print(f"[Rules] evaluate_pre_version_reminder START: key={task_key} project={project_key} pre_version_days={pre_version_days}")
    
    # Need fixVersion dates and ensure status not in UAT phases
    fix_versions_raw = task.get("fixVersions")
    print(f"[Rules]   step 1: get fixVersions from task")
    print(f"[Rules]   step 1: fixVersions type={type(fix_versions_raw)}")
    print(f"[Rules]   step 1: fixVersions raw={repr(fix_versions_raw)}")
    
    fix_versions = fix_versions_raw or []
    print(f"[Rules]   step 1: fixVersions normalized (or []): {repr(fix_versions)}")
    print(f"[Rules]   step 1: fixVersions count={len(fix_versions)}")
    
    fv_dates_raw = task.get("fixVersion_dates")
    print(f"[Rules]   step 2: get fixVersion_dates from task")
    print(f"[Rules]   step 2: fixVersion_dates type={type(fv_dates_raw)}")
    print(f"[Rules]   step 2: fixVersion_dates raw={repr(fv_dates_raw)}")
    
    fv_dates = fv_dates_raw or {}
    print(f"[Rules]   step 2: fixVersion_dates normalized (or {{}}): {repr(fv_dates)}")
    
    status_raw = task.get("status")
    print(f"[Rules]   step 3: get status from task")
    print(f"[Rules]   step 3: status raw={repr(status_raw)}")
    
    status_norm = (status_raw or "").strip().upper()
    print(f"[Rules]   step 3: status normalized={repr(status_norm)}")
    
    print(f"[Rules]   step 4: check if fixVersions is empty")
    if not fix_versions:
        print(f"[Rules]   step 4: fixVersions is empty -> SKIP")
        print(f"[Rules] evaluate_pre_version_reminder END: key={task_key} -> SKIP (no fixVersions)")
        return None
    
    print(f"[Rules]   step 4: fixVersions has {len(fix_versions)} items -> CONTINUE")
    
    now = get_vn_now()
    print(f"[Rules]   step 5: current datetime={now}")
    print(f"[Rules]   step 5: iterate through {len(fix_versions)} fixVersions")
    
    # Các status cần kiểm tra khi release trong 2 ngày tới
    deploy_uat_statuses = {"DEPLOYING", "UAT", "UAT TESTING", "READY UAT"}
    print(f"[Rules]   step 5: deploy_uat_statuses={deploy_uat_statuses}")
    print(f"[Rules]   step 5: current status_norm={repr(status_norm)}")
    
    for idx, fv in enumerate(fix_versions):
        print(f"[Rules]     fixVersion[{idx}]: processing fv={repr(fv)}")
        print(f"[Rules]     fixVersion[{idx}]: fv type={type(fv)}")
        
        if isinstance(fv, dict):
            name = fv.get("name")
            print(f"[Rules]     fixVersion[{idx}]: is dict, get name -> {repr(name)}")
        else:
            name = fv
            print(f"[Rules]     fixVersion[{idx}]: is not dict, use as name -> {repr(name)}")
        
        if not name:
            print(f"[Rules]     fixVersion[{idx}]: name is empty -> SKIP")
            continue
        
        print(f"[Rules]     fixVersion[{idx}]: parsing date from name={repr(name)}")
        release_date = _parse_date_from_fixversion_name(name)
        print(f"[Rules]     fixVersion[{idx}]: parsed date={repr(release_date)}")
        
        if not release_date:
            print(f"[Rules]     fixVersion[{idx}]: cannot parse date from name '{name}' -> SKIP")
            # Fallback: thử lấy từ fv_dates nếu có
            date_str = fv_dates.get(name)
            if date_str:
                print(f"[Rules]     fixVersion[{idx}]: trying fallback from fv_dates={repr(date_str)}")
                try:
                    release_date = datetime.fromisoformat(date_str)
                    print(f"[Rules]     fixVersion[{idx}]: fallback parsed successfully -> {release_date}")
                except Exception as e:
                    print(f"[Rules]     fixVersion[{idx}]: fallback parsing failed -> {e}")
                    print(f"[Rules]     fixVersion[{idx}]: no valid releaseDate -> SKIP")
                    continue
            else:
                print(f"[Rules]     fixVersion[{idx}]: no releaseDate -> SKIP")
                continue
        
        print(f"[Rules]     fixVersion[{idx}]: calculating days_until")
        print(f"[Rules]     fixVersion[{idx}]: release_date={release_date}, now={now}")
        # Tính days_until theo date, không theo datetime
        now_date = now.date()
        release_date_only = release_date.date()
        days_until = (release_date_only - now_date).days
        print(f"[Rules]     fixVersion[{idx}]: now_date={now_date}, release_date_only={release_date_only}, days_until={days_until}")
        
        # Kiểm tra: nếu release trong khoảng từ 0 đến 2 ngày tới VÀ status không phải là DEPLOYING/UAT/READY UAT thì gửi remind
        if 0 <= days_until <= 2:
            print(f"[Rules]     fixVersion[{idx}]: release trong 2 ngày tới (days={days_until})")
            if status_norm not in deploy_uat_statuses:
                print(f"[Rules]     fixVersion[{idx}]: status={repr(status_norm)} KHÔNG trong {deploy_uat_statuses} -> HIT (gửi remind)")
                print(f"[Rules] evaluate_pre_version_reminder END: key={task_key} -> HIT (fv={name}, days={days_until}, status={status_norm})")
                return {
                    "code": PRE_VERSION_REMINDER,
                    "fv_name": name,
                    "days": days_until,
                    "status": status_norm,
                }
            else:
                print(f"[Rules]     fixVersion[{idx}]: status={repr(status_norm)} đang trong {deploy_uat_statuses} -> SKIP (không cần remind)")
        
        # Logic cũ: kiểm tra nếu days_until <= pre_version_days (cho các trường hợp khác)
        print(f"[Rules]     fixVersion[{idx}]: checking if {days_until} <= {pre_version_days}")
        if 0 < days_until <= pre_version_days:
            print(f"[Rules]     fixVersion[{idx}]: condition met -> HIT")
            print(f"[Rules] evaluate_pre_version_reminder END: key={task_key} -> HIT (fv={name}, days={days_until})")
            return {
                "code": PRE_VERSION_REMINDER,
                "fv_name": name,
                "days": days_until,
            }
        else:
            print(f"[Rules]     fixVersion[{idx}]: condition not met -> CONTINUE")
    
    print(f"[Rules]   step 6: no fixVersion matched -> NO HIT")
    print(f"[Rules] evaluate_pre_version_reminder END: key={task_key} -> NO HIT")
    return None


def evaluate_post_version_alert(task):
    _log_task_preview("[Rules] evaluate_post_version_alert input:", task)
    project_key = task.get('project')
    if not is_project_allowed(project_key, POST_VERSION_ALERT):
        print(f"[Rules] evaluate_post_version_alert: project {project_key} is not allowed -> SKIP")
        return None
    if is_project_excluded(project_key, POST_VERSION_ALERT):
        print(f"[Rules] evaluate_post_version_alert: project {project_key} is excluded -> SKIP")
        return None
    # After release date and status not Complete
    fix_versions = task.get("fixVersions") or []
    fv_dates = task.get("fixVersion_dates") or {}
    status_raw = task.get("status")
    status_norm = (status_raw or "").strip().upper()
    if not fix_versions:
        print("[Rules] -> skip: no fixVersions")
        return None
    # Skip if already Complete
    if status_norm == "COMPLETE":
        print("[Rules] -> skip: status is Complete")
        return None
    now = get_vn_now()
    for fv in fix_versions:
        name = fv.get("name") if isinstance(fv, dict) else fv
        if not name:
            print(f"[Rules]   skip fv: name is empty")
            continue
        
        # Ưu tiên parse date từ tên fixVersion
        release_date = _parse_date_from_fixversion_name(name)
        
        if not release_date:
            # Fallback: thử lấy từ fv_dates nếu có
            print(f"[Rules]   cannot parse date from name, trying fallback from fv_dates")
            date_str = fv_dates.get(name)
            if date_str:
                print(f"[Rules]   fallback date_str={repr(date_str)}")
                try:
                    release_date = datetime.fromisoformat(date_str)
                    print(f"[Rules]   fallback parsed successfully -> {release_date}")
                except Exception as e:
                    print(f"[Rules]   fallback parsing failed -> {e}")
                    print(f"[Rules]   skip fv '{name}': no valid releaseDate")
                    continue
            else:
                print(f"[Rules]   skip fv '{name}': no releaseDate")
                continue

        print(f"[Rules]   comparing: now={now}, release_date={release_date}")
        print(f"[Rules]   comparing: now type={type(now)}, release_date type={type(release_date)}")
        # So sánh chỉ phần date, không so sánh time
        now_date = now.date()
        release_date_only = release_date.date()
        print(f"[Rules]   comparing dates: now_date={now_date}, release_date_only={release_date_only}")
        if now_date > release_date_only:
            print(f"[Rules] -> hit: past release date for {name}")
            return {
                "code": POST_VERSION_ALERT,
                "fv_name": name,
                "release_date": release_date.date().isoformat(),
            }
    print("[Rules] -> no hit")
    return None


def evaluate_assignee_changed(task, assignee_change_wait_minutes):
    """
    Kiểm tra nếu assignee được thay đổi trong vòng X phút.
    Cần có last_assignee_changed_at trong task (lấy từ changelog).
    """
    _log_task_preview("[Rules] evaluate_assignee_changed input:", task)
    project_key = task.get('project')
    if not is_project_allowed(project_key, ASSIGNEE_CHANGED):
        print(f"[Rules] evaluate_assignee_changed: project {project_key} is not allowed -> SKIP")
        return None
    if is_project_excluded(project_key, ASSIGNEE_CHANGED):
        print(f"[Rules] evaluate_assignee_changed: project {project_key} is excluded -> SKIP")
        return None
    assignee_email = task.get("assignee_email")
    
    if not assignee_email:
        print("[Rules] -> skip: no assignee")
        return None
    
    changed_at_str = task.get("last_assignee_changed_at")
    if not changed_at_str:
        print("[Rules] -> skip: no last_assignee_changed_at")
        return None
    
    try:
        changed_at = datetime.strptime(changed_at_str, "%Y-%m-%dT%H:%M:%S.%f%z")
    except Exception:
        try:
            changed_at = datetime.strptime(changed_at_str, "%Y-%m-%dT%H:%M:%S%z")
        except Exception:
            try:
                changed_at = datetime.strptime(changed_at_str, "%Y-%m-%dT%H:%M:%S")
            except Exception:
                print("[Rules] -> skip: cannot parse last_assignee_changed_at")
                return None
    
    now = get_vn_now()
    if changed_at.tzinfo:
        now = now.astimezone(changed_at.tzinfo)
    time_diff = now - changed_at
    
    # Chỉ kiểm tra thay đổi trong quá khứ và trong vòng X phút
    if time_diff >= timedelta(0) and time_diff <= timedelta(minutes=assignee_change_wait_minutes):
        print(f"[Rules] -> hit: assignee changed {time_diff.total_seconds()/60:.1f}m ago")
        return {
            "code": ASSIGNEE_CHANGED,
            "assignee_email": assignee_email,
            "changed_at": changed_at_str,
        }
    
    print("[Rules] -> no hit: change too old")
    return None


def evaluate_due_date_overdue(task):
    """
    Kiểm tra task có quá hạn due date không.
    Hỗ trợ các key due date phổ biến: 'duedate', 'dueDate', 'due_date', 'due'.
    - So sánh theo ngày (date-only), không xét time.
    - Trả về dict kèm số ngày trễ hạn nếu quá hạn, ngược lại trả về None.
    """
    _log_task_preview("[Rules] evaluate_due_date_overdue input:", task)
    project_key = task.get('project')
    if not is_project_allowed(project_key, DUE_DATE_OVERDUE):
        print(f"[Rules] evaluate_due_date_overdue: project {project_key} is not allowed -> SKIP")
        return None
    if is_project_excluded(project_key, DUE_DATE_OVERDUE):
        print(f"[Rules] evaluate_due_date_overdue: project {project_key} is excluded -> SKIP")
        return None

    task_key = task.get('key')
    print(f"[Rules] evaluate_due_date_overdue START: key={task_key} project={project_key}")

    # Lấy due date với các key khả dĩ
    due_keys = ["duedate", "dueDate", "due_date", "due"]
    due_raw = None
    for k in due_keys:
        if k in task and task.get(k) is not None:
            due_raw = task.get(k)
            print(f"[Rules]   step 1: found due date with key '{k}' -> {repr(due_raw)}")
            break

    if due_raw is None:
        print(f"[Rules]   step 1: no due date found -> SKIP")
        print(f"[Rules] evaluate_due_date_overdue END: key={task_key} -> SKIP (no due)")
        return None

    # Chuẩn hóa về date (yyyy-mm-dd)
    due_date_only = None
    try:
        if isinstance(due_raw, datetime):
            due_date_only = due_raw.date()
            print(f"[Rules]   step 2: due is datetime -> use date {due_date_only}")
        elif getattr(due_raw, "__class__", None) and due_raw.__class__.__name__ == "date":
            # Là kiểu date
            due_date_only = due_raw
            print(f"[Rules]   step 2: due is date -> {due_date_only}")
        elif isinstance(due_raw, str):
            s = due_raw.strip()
            print(f"[Rules]   step 2: due is str={repr(s)}")
            # Ưu tiên định dạng Jira 'YYYY-MM-DD'
            try:
                due_date_only = datetime.strptime(s[:10], "%Y-%m-%d").date()
                print(f"[Rules]   step 2: parsed with %Y-%m-%d -> {due_date_only}")
            except Exception:
                try:
                    # Thử ISO khác, nếu là datetime ISO thì lấy .date()
                    dt = datetime.fromisoformat(s)
                    due_date_only = dt.date()
                    print(f"[Rules]   step 2: parsed with fromisoformat -> {due_date_only}")
                except Exception as e:
                    print(f"[Rules]   step 2: cannot parse due string -> {e}")
        else:
            print(f"[Rules]   step 2: unsupported due type={type(due_raw)} -> cannot parse")
    except Exception as e:
        print(f"[Rules]   step 2: unexpected error while parsing due -> {e}")

    if not due_date_only:
        print(f"[Rules]   step 3: no valid due date after parsing -> SKIP")
        print(f"[Rules] evaluate_due_date_overdue END: key={task_key} -> SKIP (invalid due)")
        return None

    today = get_vn_now().date()
    print(f"[Rules]   step 3: today={today}, due_date={due_date_only}")

    if today > due_date_only:
        days_overdue = (today - due_date_only).days
        print(f"[Rules] -> hit: task is overdue by {days_overdue} day(s)")
        print(f"[Rules] evaluate_due_date_overdue END: key={task_key} -> HIT")
        return {
            "code": DUE_DATE_OVERDUE,
            "due_date": due_date_only.isoformat(),
            "days_overdue": days_overdue,
        }

    print(f"[Rules] -> no hit: not overdue")
    print(f"[Rules] evaluate_due_date_overdue END: key={task_key} -> NO HIT")
    return None


def evaluate_created_recently(task, created_wait_minutes):
    """
    Kiểm tra nếu task được tạo trong vòng X phút gần đây.
    Lấy thời gian tạo từ các key phổ biến: 'created', 'created_at', 'createdAt'.
    """
    _log_task_preview("[Rules] evaluate_created_recently input:", task)
    project_key = task.get('project')
    if not is_project_allowed(project_key, RECENTLY_CREATED):
        print(f"[Rules] evaluate_created_recently: project {project_key} is not allowed -> SKIP")
        return None
    if is_project_excluded(project_key, RECENTLY_CREATED):
        print(f"[Rules] evaluate_created_recently: project {project_key} is excluded -> SKIP")
        return None

    created_keys = ["created", "created_at", "createdAt"]
    created_at_str = None
    for k in created_keys:
        v = task.get(k)
        if v:
            created_at_str = v
            print(f"[Rules]   step 1: found created with key '{k}' -> {repr(v)}")
            break

    if not created_at_str:
        print("[Rules] -> skip: no created timestamp")
        return None

    # Parse created_at
    created_at = None
    try:
        if isinstance(created_at_str, datetime):
            created_at = created_at_str
            print(f"[Rules]   step 2: created is datetime -> {created_at}")
        elif isinstance(created_at_str, str):
            s = created_at_str.strip()
            print(f"[Rules]   step 2: created is str={repr(s)} -> try multiple formats")
            try:
                created_at = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f%z")
            except Exception:
                try:
                    created_at = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S%z")
                except Exception:
                    try:
                        created_at = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
                    except Exception:
                        try:
                            created_at = datetime.fromisoformat(s)
                        except Exception as e:
                            print(f"[Rules]   step 2: cannot parse created -> {e}")
        else:
            print(f"[Rules]   step 2: unsupported created type={type(created_at_str)}")
    except Exception as e:
        print(f"[Rules]   step 2: unexpected error while parsing created -> {e}")

    if not created_at:
        print("[Rules] -> skip: cannot parse created timestamp")
        return None

    now = get_vn_now()
    if getattr(created_at, 'tzinfo', None):
        now = now.astimezone(created_at.tzinfo)
    time_diff = now - created_at

    if time_diff >= timedelta(0) and time_diff <= timedelta(minutes=created_wait_minutes):
        minutes_ago = time_diff.total_seconds() / 60
        print(f"[Rules] -> hit: task created {minutes_ago:.1f}m ago")
        return {
            "code": RECENTLY_CREATED,
            "created_at": created_at_str if isinstance(created_at_str, str) else created_at.isoformat(),
        }

    print("[Rules] -> no hit: created too old or in the future")
    return None
