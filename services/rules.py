from datetime import datetime, timedelta
import json

# Rule identifiers
MISSING_LOGTIME = "missing_logtime"
MISSING_DESCRIPTION = "missing_description"
PRE_VERSION_REMINDER = "pre_version_reminder"
POST_VERSION_ALERT = "post_version_alert"
ASSIGNEE_CHANGED = "assignee_changed"


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


def evaluate_missing_logtime(task, ci_testing_wait_minutes):
    _log_task_preview("[Rules] evaluate_missing_logtime input:", task)
    status_raw = task.get('status')
    status_norm = (status_raw or "").strip().upper()
    print(f"[Rules] evaluate_missing_logtime: key={task.get('key')} status={status_raw} (norm={status_norm}) has_worklog={task.get('has_worklog')} last_status_changed_at={task.get('last_status_changed_at')} wait={ci_testing_wait_minutes}m")
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
    if datetime.now(changed_at.tzinfo) - changed_at >= timedelta(minutes=ci_testing_wait_minutes):
        print("[Rules] -> hit: exceeded wait window")
        return MISSING_LOGTIME
    print("[Rules] -> no hit")
    return None


def evaluate_missing_description(task):
    _log_task_preview("[Rules] evaluate_missing_description input:", task)
    description = task.get("description")
    print(f"[Rules] evaluate_missing_description: key={task.get('key')} has_desc={bool(description and str(description).strip())}")
    if description is None or str(description).strip() == "":
        print("[Rules] -> hit: missing description")
        return MISSING_DESCRIPTION
    print("[Rules] -> no hit")
    return None


def evaluate_pre_version_reminder(task, pre_version_days):
    _log_task_preview("[Rules] evaluate_pre_version_reminder input:", task)
    # Need fixVersion dates and ensure status not in UAT phases
    fix_versions = task.get("fixVersions") or []
    fv_dates = task.get("fixVersion_dates") or {}
    status_raw = task.get("status")
    status_norm = (status_raw or "").strip().upper()
    print(f"[Rules] evaluate_pre_version_reminder: key={task.get('key')} fv_count={len(fix_versions)} status={status_raw} (norm={status_norm}) pre_days={pre_version_days}")
    if not fix_versions:
        print("[Rules] -> skip: no fixVersions")
        return None
    # Skip if already in UAT phases
    if status_norm in {"UAT", "UAT TESTING"}:
        print("[Rules] -> skip: status is UAT/UAT TESTING")
        return None
    now = datetime.now()
    for fv in fix_versions:
        name = fv.get("name") if isinstance(fv, dict) else fv
        date_str = fv_dates.get(name)
        if not date_str:
            print(f"[Rules]   skip fv '{name}': no releaseDate")
            continue
        try:
            release_date = datetime.fromisoformat(date_str)
        except Exception:
            print(f"[Rules]   skip fv '{name}': invalid releaseDate={date_str}")
            continue
        days_until = (release_date - now).days
        if 0 <= days_until <= pre_version_days:
            print(f"[Rules] -> hit: fv={name} days_until={days_until}")
            return {
                "code": PRE_VERSION_REMINDER,
                "fv_name": name,
                "days": days_until,
            }
    print("[Rules] -> no hit")
    return None


def evaluate_post_version_alert(task):
    _log_task_preview("[Rules] evaluate_post_version_alert input:", task)
    # After release date and status not Complete
    fix_versions = task.get("fixVersions") or []
    fv_dates = task.get("fixVersion_dates") or {}
    status_raw = task.get("status")
    status_norm = (status_raw or "").strip().upper()
    print(f"[Rules] evaluate_post_version_alert: key={task.get('key')} fv_count={len(fix_versions)} status={status_raw} (norm={status_norm})")
    if not fix_versions:
        print("[Rules] -> skip: no fixVersions")
        return None
    # Skip if already Complete
    if status_norm == "COMPLETE":
        print("[Rules] -> skip: status is Complete")
        return None
    now = datetime.now()
    for fv in fix_versions:
        name = fv.get("name") if isinstance(fv, dict) else fv
        date_str = fv_dates.get(name)
        if not date_str:
            print(f"[Rules]   skip fv '{name}': no releaseDate")
            continue
        try:
            release_date = datetime.fromisoformat(date_str)
        except Exception:
            print(f"[Rules]   skip fv '{name}': invalid releaseDate={date_str}")
            continue
        if now > release_date:
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
    assignee_email = task.get("assignee_email")
    print(f"[Rules] evaluate_assignee_changed: key={task.get('key')} assignee={assignee_email} wait={assignee_change_wait_minutes}m")
    
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
    
    now = datetime.now(changed_at.tzinfo) if changed_at.tzinfo else datetime.now()
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
