from datetime import datetime, timedelta, timezone
from rules import (
    evaluate_missing_logtime,
    evaluate_missing_description,
    evaluate_pre_version_reminder,
    evaluate_post_version_alert,
    MISSING_LOGTIME,
    MISSING_DESCRIPTION,
    PRE_VERSION_REMINDER,
    POST_VERSION_ALERT,
)


def test_missing_logtime_true():
    changed = (datetime.now(timezone.utc) - timedelta(minutes=120)).strftime("%Y-%m-%dT%H:%M:%S%z")
    task = {"status": "CI Testing", "has_worklog": False, "last_status_changed_at": changed}
    assert evaluate_missing_logtime(task, 60) == MISSING_LOGTIME


def test_missing_logtime_false_due_to_worklog():
    task = {"status": "CI Testing", "has_worklog": True}
    assert evaluate_missing_logtime(task, 60) is None


def test_missing_description_true():
    assert evaluate_missing_description({"description": "  "}) == MISSING_DESCRIPTION


def test_pre_version_reminder_in_window():
    release = (datetime.now() + timedelta(days=2)).date().isoformat()
    task = {
        "fixVersions": [{"name": "1.0"}],
        "fixVersion_dates": {"1.0": f"{release}T00:00:00"},
        "is_uat_done": False,
    }
    res = evaluate_pre_version_reminder(task, 3)
    assert isinstance(res, dict) and res["code"] == PRE_VERSION_REMINDER


def test_post_version_alert_after_release():
    past = (datetime.now() - timedelta(days=1)).date().isoformat()
    task = {
        "fixVersions": [{"name": "1.0"}],
        "fixVersion_dates": {"1.0": f"{past}T00:00:00"},
        "is_production": False,
    }
    res = evaluate_post_version_alert(task)
    assert isinstance(res, dict) and res["code"] == POST_VERSION_ALERT

