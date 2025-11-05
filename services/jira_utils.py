import os
import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import requests
from requests.auth import HTTPBasicAuth

from logger import get_logger


class JiraClient:
    """
    JiraClient

    - Kết nối Jira (Cloud/Nội bộ) qua REST API
    - Tìm kiếm issues bằng JQL (có phân trang)
    - Lấy chi tiết issue kèm worklog
    - Ghi log đầy đủ request/response (thời gian, headers, curl, body, JSON)
    - Chuẩn hoá dữ liệu về dạng task object thống nhất

    Cách dùng cơ bản:
        client = JiraClient(jira_url, username, password)
        issues = client.search_issues("assignee = 'user@domain' AND updatedDate >= '2025-10-01'")
        for issue in issues:
            task = client.build_task_object(issue)
    """

    def __init__(
        self,
        jira_url: str,
        username: str,
        password_or_token: str,
        projects: Optional[List[str]] = None,
        *,
        auth_type: str = "basic",
        timeout_seconds: int = 30,
        verify_ssl: Optional[bool] = None,
        default_headers: Optional[Dict[str, str]] = None,
        log_file: Optional[str] = None,
        log_response_json: bool = False,
    ) -> None:
        self.jira_url = jira_url.rstrip("/")
        self.username = username
        # Fallback: nếu dùng basic mà không truyền password/token, lấy từ env JIRA_PASSWORD
        if (auth_type or "basic").lower() == "basic" and (not password_or_token):
            password_or_token = os.getenv("JIRA_PASSWORD", "")
        self.password_or_token = password_or_token
        self.auth_type = (auth_type or "basic").lower()
        # Auth setup
        self.auth = None
        if self.auth_type == "basic":
            self.auth = HTTPBasicAuth(username, password_or_token)
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        # SSL verify: env overrides default if not specified
        if verify_ssl is None:
            env_v = os.getenv("JIRA_VERIFY_SSL")
            if env_v is not None:
                verify_ssl = env_v.strip().lower() not in ("0", "false", "no")
            else:
                verify_ssl = True
        self.session.verify = verify_ssl

        # Proxies: inherit from environment automatically
        # requests will read HTTP[S]_PROXY, no extra code needed
        self.default_headers = {
            "Accept": "application/json",
        }
        if self.auth_type in ("token", "bearer", "oauth"):
            self.default_headers["Authorization"] = f"Bearer {password_or_token}"
        if default_headers:
            self.default_headers.update(default_headers)

        self.logger = get_logger()
        self.log_response_json = log_response_json
        self.log_file = log_file
        if self.log_file:
            # Đảm bảo thư mục tồn tại
            os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
        # Cached project list for convenience in reminder flows
        self.projects = [p.strip().upper() for p in (projects or []) if p and p.strip()]
        if not self.projects:
            # Tự động lấy tất cả projects từ Jira
            try:
                self.projects = self.get_all_projects()
                if self.projects:
                    print(f"[Jira] Đã lấy tất cả projects từ Jira: {', '.join(self.projects)}")
                else:
                    # Fallback nếu không lấy được
                    self.projects = ["FC", "FSS", "PKT", "WAK", "PPFP"]
                    print(f"[Jira] Không lấy được projects từ Jira, dùng mặc định: {', '.join(self.projects)}")
            except Exception as ex:
                # Fallback nếu có lỗi
                self.projects = ["FC", "FSS", "PKT", "WAK", "PPFP"]
                print(f"[Jira] Lỗi khi lấy projects, dùng mặc định: {', '.join(self.projects)}")
                self.logger.warning(f"Failed to get all projects: {ex}")

    # -----------------------------
    # Low-level helpers
    # -----------------------------
    def _write_log_file(self, message: str) -> None:
        if not self.log_file:
            return
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(message + "\n")

    def _curl_from_request(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        body: Optional[Union[str, Dict[str, Any]]],
    ) -> str:
        parts: List[str] = ["curl", "-X", method.upper(), f"\"{url}\""]
        for k, v in headers.items():
            # Che password/basic header
            if k.lower() == "authorization":
                v = "***redacted***"
            parts += ["-H", f"\"{k}: {v}\""]
        if body is not None:
            if isinstance(body, (dict, list)):
                body_str = json.dumps(body, ensure_ascii=False)
            else:
                body_str = str(body)
            parts += ["--data", f"\"{body_str}\""]
        return " ".join(parts)

    def _request(
        self,
        method: str,
        url_or_path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        url = url_or_path
        if not url_or_path.lower().startswith("http"):
            url = f"{self.jira_url}{url_or_path}"

        merged_headers = dict(self.default_headers)
        if headers:
            merged_headers.update(headers)

        curl_cmd = self._curl_from_request(method, url, merged_headers, json_body)
        start = time.time()
        self.logger.info(f"Jira API {method.upper()} {url}")
        print(f"[Jira] {method.upper()} {url}")
        self.logger.debug(f"curl: {curl_cmd}")
        print(f"[Jira] curl: {curl_cmd}")
        self._write_log_file(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {method.upper()} {url}")
        self._write_log_file(f"curl: {curl_cmd}")

        resp = self.session.request(
            method=method,
            url=url,
            auth=self.auth,
            headers=merged_headers,
            json=json_body,
            params=params,
            timeout=self.timeout_seconds,
        )

        duration = (time.time() - start) * 1000
        self.logger.info(f"Status: {resp.status_code} | {duration:.1f} ms")
        print(f"[Jira] → {resp.status_code} ({duration:.1f} ms)")
        self._write_log_file(f"Status: {resp.status_code} | {duration:.1f} ms")

        if self.log_response_json:
            try:
                pretty = json.dumps(resp.json(), ensure_ascii=False, indent=2)
                self._write_log_file(pretty)
            except Exception:
                # Not JSON
                self._write_log_file(resp.text[:4000])

        return resp

    def ping(self) -> bool:
        """Kiểm tra kết nối/JWT/BASIC hợp lệ bằng endpoint /myself."""
        try:
            resp = self._request("GET", "/secure/ViewProfile.jspa")
            if resp.status_code == 200:
                print("[Jira] Ping OK: authenticated")
                return True
            print(f"[Jira] Ping FAILED: {resp.status_code} - {resp.text[:200]}")
            return False
        except Exception as ex:
            print(f"[Jira] Ping error: {ex}")
            return False

    def get_all_projects(self) -> List[str]:
        """Lấy tất cả project keys từ Jira."""
        try:
            print("[Jira] Đang lấy danh sách tất cả projects từ Jira...")
            resp = self._request("GET", "/rest/api/2/project")
            if resp.status_code != 200:
                self.logger.warning(f"get_all_projects failed: {resp.status_code} - {resp.text[:200]}")
                return []
            projects_data = resp.json()
            project_keys = [p.get("key", "").upper() for p in projects_data if p.get("key")]
            project_keys = [p for p in project_keys if p]  # Loại bỏ empty strings
            print(f"[Jira] Lấy được {len(project_keys)} projects: {', '.join(project_keys)}")
            return project_keys
        except Exception as ex:
            self.logger.warning(f"get_all_projects error: {ex}")
            print(f"[Jira] Lỗi khi lấy danh sách projects: {ex}")
            return []

    # -----------------------------
    # Public API methods
    # -----------------------------
    def search_issues(
        self,
        jql: str,
        *,
        fields: Optional[List[str]] = None,
        expand: Optional[List[str]] = None,
        max_results: int = 1000,
        start_at: int = 0,
        show_first_url: bool = True,
    ) -> List[Dict[str, Any]]:
        """Truy vấn issues bằng JQL (tự động phân trang)."""
        collected: List[Dict[str, Any]] = []
        page = start_at

        # =========================
        # OLD PREFLIGHT (đã đóng tạm)
        # Mục đích: gọi thử search 1 issue trước khi chạy vòng lặp chính.
        # Lý do đóng: thay bằng bản mới gọn hơn bên dưới. Khi cần có thể mở lại.
        #
        # try:
        #     if os.getenv("JIRA_PREFLIGHT_PPF933", "0").strip().lower() in ("1", "true", "yes"):
        #         pre_jql = "key = PPFP-933"
        #         print(f"[Jira] Preflight search enabled. Testing with: {pre_jql}")
        #         pre_params = {
        #             "jql": pre_jql,
        #             "maxResults": 1,
        #             "startAt": 0,
        #             "fields": "key,summary,status",
        #         }
        #         pre_resp = self._request("GET", "/rest/api/2/search", params=pre_params)
        #         if pre_resp.status_code == 200:
        #             pre_data = pre_resp.json() or {}
        #             pre_count = len(pre_data.get("issues", []))
        #             print(f"[Jira] Preflight OK: {pre_count} issue(s) matched PPFP-933")
        #         else:
        #             print(f"[Jira] Preflight FAILED: {pre_resp.status_code} - {pre_resp.text[:200]}")
        # except Exception as ex:
        #     print(f"[Jira] Preflight error: {ex}")

        # =========================
        # NEW PREFLIGHT (đang dùng)
        # - Giả lập/gọi test nhanh với điều kiện search của ticket PPFP-933
        # - Luôn chạy 1 lần trước vòng lặp chính để kiểm tra kết nối/quyền truy cập
        # - Không ảnh hưởng tới kết quả search chính (không sửa 'jql' ban đầu)
        
        # === PHẦN 1: PREFLIGHT TEST (Kiểm tra nhanh trước khi tìm kiếm thực tế) ===
        # Mục đích: Thực hiện một lần tìm kiếm test với ticket PPFP-933 để kiểm tra:
        # - Kết nối tới Jira API có hoạt động không
        # - Xác thực (authentication) có hợp lệ không
        # - Quyền truy cập API có đủ không
        # Nếu test này thành công, vòng lặp chính sẽ chạy an toàn hơn
        #try:
            ## Chuẩn bị tham số cho request test: tìm ticket PPFP-933
            #pre_params = {
                #"jql": "key = PPFP-933",  # thêm điều kiện search này: https://reqs.frt.vn/browse/PPFP-933
                #"maxResults": 1,          # Chỉ cần 1 kết quả để test
                #"startAt": 0,              # Bắt đầu từ vị trí 0
                #"fields": "key,summary,status",  # Chỉ lấy các trường cơ bản
            #}
            #print("[Jira] Preflight (NEW): search PPFP-933 để kiểm tra nhanh")
#            
            ## Gọi API search để test
            #pre_resp = self._request("GET", "/rest/api/2/search", params=pre_params)
#            
            ## Xử lý kết quả test
            #if pre_resp.status_code == 200:
                ## Nếu thành công: parse JSON và đếm số issue tìm được
                #pre_data = pre_resp.json() or {}
                #pre_issues = pre_data.get("issues", [])
                #pre_count = len(pre_issues)
                #pre_key = pre_issues[0]["key"] if pre_count else ""  # Lấy key của issue đầu tiên (nếu có)
                #print(f"[Jira] Preflight (NEW) OK: {pre_count} issue(s). First: {pre_key}")
            #else:
                ## Nếu thất bại: in ra mã lỗi và một phần response body
                #print(f"[Jira] Preflight (NEW) FAILED: {pre_resp.status_code} - {pre_resp.text[:200]}")
        #except Exception as ex:
            ## Bắt bất kỳ exception nào xảy ra trong quá trình test (network error, JSON parse error, etc.)
            #print(f"[Jira] Preflight (NEW) error: {ex}")
        
        # === PHẦN 2: VÒNG LẶP PHÂN TRANG CHÍNH (Tìm kiếm thực tế với JQL) ===
        # Mục đích: Thực hiện tìm kiếm issues theo JQL được truyền vào, tự động phân trang
        # để lấy tất cả kết quả (vì Jira API có giới hạn số lượng kết quả mỗi lần)
        while True:
            print("[Jira] Bắt đầu tìm kiếm issues theo JQL...")
            print(f"[Jira] JQL: {jql}")
            # Chuẩn bị tham số cho request search chính
            params: Dict[str, Any] = {
                "jql": jql,                    # Câu lệnh JQL để tìm kiếm (do người dùng truyền vào)
                "maxResults": max_results,     # Số lượng kết quả tối đa mỗi trang (mặc định 1000)
                "startAt": page,               # Vị trí bắt đầu của trang hiện tại (để phân trang)
            }
            
            # Thêm danh sách các trường (fields) cần lấy nếu được chỉ định
            # Ví dụ: ["summary", "status", "assignee"] -> "summary,status,assignee"
            if fields:
                params["fields"] = ",".join(fields)
            
            # Thêm danh sách các phần mở rộng (expand) nếu được chỉ định
            # Ví dụ: ["changelog", "renderedFields"] để lấy thêm thông tin chi tiết
            if expand:
                params["expand"] = ",".join(expand)

            # Ghi log khi bắt đầu search (chỉ lần đầu tiên)
            if show_first_url and page == start_at:
                self.logger.info("Search API initialized (URL hidden, see curl in debug logs)")

            # In thông tin trang hiện tại (số trang, vị trí bắt đầu, số lượng kết quả)
            print(f"[Jira] Trang {int(page/max_results)+1} (startAt={page}, maxResults={max_results})")
            
            # Log full API parameters
            params_json = json.dumps(params, ensure_ascii=False, indent=2)
            self.logger.info(f"API parameters: {params_json}")
            print(f"[Jira] API parameters: {params_json}")
            self._write_log_file(f"API parameters: {params_json}")
            
            # Gọi API search với tham số đã chuẩn bị
            resp = self._request("GET", "/rest/api/2/search", params=params)
            
            # Kiểm tra mã trạng thái HTTP
            if resp.status_code != 200:
                # Nếu không phải 200 (OK): lấy chi tiết lỗi và raise exception
                details = resp.text.strip() if isinstance(resp.text, str) else ""
                raise RuntimeError(
                    f"JQL search failed: {resp.status_code} | auth_type={self.auth_type} | verify_ssl={self.session.verify} | body={details[:300]}"
                )

            # === PHẦN 3: XỬ LÝ KẾT QUẢ TỪNG TRANG ===
            # Parse JSON response từ Jira API
            data = resp.json()
            
            # Lấy danh sách issues từ response (mỗi trang có thể có nhiều issues)
            issues = data.get("issues", [])
            
            # Thêm tất cả issues của trang hiện tại vào danh sách tổng hợp (collected)
            collected.extend(issues)

            # Lấy tổng số issues phù hợp với JQL (từ Jira API trả về)
            total = data.get("total", 0)
            
            # In thông tin: số issues thu được ở trang này và tổng số đã thu được / tổng số có
            print(f"[Jira] Thu được {len(issues)} issue (tổng lũy kế: {len(collected)}/{total})")
            
            # === PHẦN 4: ĐIỀU KIỆN DỪNG VÒNG LẶP ===
            # Dừng vòng lặp nếu:
            # - Số issues thu được < max_results: không còn trang nào nữa
            # - Hoặc đã lấy đủ tất cả (page + số issues hiện tại >= tổng số)
            if len(issues) < max_results or page + len(issues) >= total:
                break
            
            # Tăng page lên để lấy trang tiếp theo (di chuyển sang trang sau)
            page += max_results

        # === PHẦN 5: KẾT THÚC ===
        # In thông tin tổng kết và trả về danh sách tất cả issues đã thu thập được
        print(f"[Jira] Hoàn tất tìm kiếm. Tổng số issue: {len(collected)}")
        return collected

    def get_issue(self, issue_key: str, *, expand: Optional[List[str]] = None) -> Dict[str, Any]:
        params = {"expand": ",".join(expand)} if expand else None
        resp = self._request("GET", f"/rest/api/2/issue/{issue_key}", params=params)
        if resp.status_code != 200:
            raise RuntimeError(f"get_issue failed for {issue_key}: {resp.status_code} - {resp.text}")
        return resp.json()

    def get_worklog(self, issue_key: str) -> List[Dict[str, Any]]:
        resp = self._request("GET", f"/rest/api/2/issue/{issue_key}/worklog")
        if resp.status_code != 200:
            # Trả về rỗng thay vì raise để không gián đoạn pipeline
            self.logger.warning(f"get_worklog failed for {issue_key}: {resp.status_code}")
            return []
        data = resp.json()
        return data.get("worklogs", [])

    def get_last_assignee_change(self, issue_key: str) -> Optional[str]:
        """Lấy thời gian (ISO) khi assignee được thay đổi lần cuối, hoặc None nếu không tìm thấy."""
        try:
            issue = self.get_issue(issue_key, expand=["changelog"])
            changelog = issue.get("changelog", {})
            histories = changelog.get("histories", [])
            # Tìm ngược từ mới nhất về cũ nhất
            for history in reversed(histories):
                created = history.get("created", "")
                items = history.get("items", [])
                for item in items:
                    if item.get("field") == "assignee":
                        # Tìm thấy thay đổi assignee
                        return created
            return None
        except Exception as ex:
            self.logger.warning(f"Failed to get changelog for {issue_key}: {ex}")
            return None

    def get_issue_with_worklog(self, issue_key: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        print(f"[Jira] Lấy thông tin issue + worklog: {issue_key}")
        issue = self.get_issue(issue_key)
        worklogs = self.get_worklog(issue_key)
        return issue, worklogs

    # -----------------------------
    # Normalization helpers
    # -----------------------------
    def _safe_get(self, obj: Dict[str, Any], path: List[str], default: Any = "") -> Any:
        cur: Any = obj
        for key in path:
            if not isinstance(cur, dict):
                return default
            cur = cur.get(key, {})
        return cur if cur != {} else default

    def _format_iso(self, iso_str: str) -> str:
        if not iso_str:
            return ""
        try:
            return datetime.fromisoformat(iso_str.replace("Z", "+00:00")).strftime("%d/%m/%Y %H:%M")
        except Exception:
            return iso_str

    def _compute_actual_project(self, project_key: str, component_names: List[str]) -> str:
        # Cố gắng dùng logic có sẵn nếu import được
        try:
            from get_lc_tasks_with_worklog_final import get_actual_project as external_actual_project
            return external_actual_project(project_key, component_names)
        except Exception:
            pass

        # Fallback tối giản (đồng bộ với logic chính ở mức cơ bản)
        if project_key == "PKT":
            return "[Project] Kho Tổng + PIM"
        if project_key == "WAK":
            return "Web App KHLC"
        if project_key == "PPFP":
            return "Payment FPT Pay"
        if project_key == "FSS":
            return "Noti + Loyalty + Core Cust"
        # FC: cố gắng nhận diện theo component phổ biến
        if project_key == "FC":
            if any(c in ["LC Offline Q1", "LC RSA Ecom", "B05. RSA/RSA ECOM", "LCD", "Tuning RSA Ecom"] for c in component_names):
                return "RSA + RSA eCom + Shipment"
            if any(c.startswith("Ecom - ") for c in component_names):
                return "Web App KHLC"
            if any(c in ["PaymentTenacy"] for c in component_names):
                return "Payment FPT Pay"
        return project_key

    def _normalize_worklogs(self, worklogs: List[Dict[str, Any]], project_key: str, project_name: str) -> Tuple[List[Dict[str, Any]], float]:
        result: List[Dict[str, Any]] = []
        total_hours = 0.0
        for wl in worklogs or []:
            try:
                author = self._safe_get(wl, ["author", "displayName"], "")
                time_spent_seconds = wl.get("timeSpentSeconds", 0) or 0
                started = wl.get("started", "")
                comment = wl.get("comment", "")
                hours = float(time_spent_seconds) / 3600.0
                total_hours += hours
                result.append({
                    "author": author,
                    "time_spent": wl.get("timeSpent", ""),
                    "hours_spent": round(hours, 2),
                    "started": self._format_iso(started),
                    "comment": comment,
                    "project_key": project_key,
                    "project_name": project_name,
                })
            except Exception:
                continue
        return result, round(total_hours, 2)

    def build_task_object(
        self,
        issue: Dict[str, Any],
        worklogs: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Chuẩn hoá issue của Jira thành task object thống nhất."""
        key = issue.get("key", "")
        print(f"[Jira] Chuẩn hoá task: {key}")
        fields = issue.get("fields", {}) if isinstance(issue, dict) else {}

        summary = fields.get("summary", "")
        status = self._safe_get(fields, ["status", "name"], "")
        updated = fields.get("updated", "")
        issue_type = self._safe_get(fields, ["issuetype", "name"], "")
        priority = self._safe_get(fields, ["priority", "name"], "")
        project_key = self._safe_get(fields, ["project", "key"], "").upper()
        project_name = self._safe_get(fields, ["project", "name"], "")

        components_raw = fields.get("components", []) or []
        component_names = [c.get("name", "") for c in components_raw if isinstance(c, dict)]
        component_str = ", ".join([c for c in component_names if c]) if component_names else "Không có component"

        # Parent (nếu có)
        parent_key = self._safe_get(fields, ["parent", "key"], "")
        parent_summary = self._safe_get(fields, ["parent", "fields", "summary"], "")
        is_subtask = (issue_type == "Sub-task")

        # Original estimate (giây)
        original_estimate_seconds = fields.get("timeoriginalestimate", 0) or 0
        original_estimate_hours = round((original_estimate_seconds / 3600.0), 2) if original_estimate_seconds else 0.0

        # Worklogs
        if worklogs is None:
            worklogs = self.get_worklog(key)
        norm_worklogs, total_hours = self._normalize_worklogs(worklogs, project_key, project_name)
        print(f"[Jira] → Worklogs: {len(norm_worklogs)}, Tổng giờ: {total_hours}")

        task = {
            "key": key,
            "summary": summary,
            "status": status,
            "updated": self._format_iso(updated),
            "type": issue_type,
            "priority": priority,
            "project": project_key,
            "project_name": project_name,
            "components": component_names,
            "component_str": component_str,
            "actual_project": self._compute_actual_project(project_key, component_names),
            "link": f"{self.jira_url}/browse/{key}" if key else "",
            "worklogs": norm_worklogs,
            "total_hours": total_hours,
            "has_worklog": len(norm_worklogs) > 0,
            "parent_key": parent_key,
            "parent_summary": parent_summary,
            "is_subtask": is_subtask,
            "original_estimate_hours": round(original_estimate_hours, 2),
        }

        # Enrich for reminder rules
        assignee = fields.get("assignee") or {}
        reporter = fields.get("reporter") or {}
        task["assignee_email"] = assignee.get("emailAddress") or ""
        task["reporter_email"] = reporter.get("emailAddress") or ""
        task["task_url"] = task["link"]
        task["description"] = fields.get("description")
        # Status last changed time (ISO)
        last_status_change = fields.get("statuscategorychangedate") or ""
        task["last_status_changed_at"] = last_status_change
        # FixVersions and release dates mapping
        fix_versions = fields.get("fixVersions") or []
        task["fixVersions"] = fix_versions
        fv_dates: Dict[str, str] = {}
        for fv in fix_versions:
            if isinstance(fv, dict):
                name = fv.get("name")
                rel = fv.get("releaseDate")  # ISO date without time
                if name and rel:
                    try:
                        # normalize to ISO with time, use 00:00:00
                        fv_dates[name] = f"{rel}T00:00:00"
                    except Exception:
                        pass
        task["fixVersion_dates"] = fv_dates
        # Flags (best-effort defaults)
        task["is_uat_done"] = False
        task["is_production"] = False
        
        # Last assignee change time (ISO) - lazy load only if needed
        # Note: This is computed on-demand to avoid slowing down normal flows
        task["last_assignee_changed_at"] = None

        return task

    # -----------------------------
    # Convenience for reminder_bot
    # -----------------------------
    def search_recent_tasks(self, minutes: int, cr_scan_days: int = 3) -> List[Dict[str, Any]]:
        """Tìm tasks cập nhật trong X phút gần đây hoặc CR tasks trong vòng X ngày.
        Gộp thành 1 query duy nhất: (tasks updated in minutes) OR (CR tasks updated in cr_scan_days)
        """
        if self.projects:
            project_values = ", ".join(["'{}'".format(p) for p in self.projects])
            proj_clause = f"project in ({project_values})"
        else:
            proj_clause = ""
        
        # Gộp thành 1 query: (tasks updated in minutes) OR (CR tasks updated in cr_scan_days)
        time_clause_1 = f"updated >= -{int(minutes)}m"
        #cr_filter = "(issuetype = 'Code Review' OR summary ~ 'CR' OR labels = 'CR')"
        cr_filter = ""
        time_clause_2 = f"updated >= -{int(cr_scan_days)}d"
        contextquery = 'text ~ "test api hangnt60"'
        # Tạo JQL: project AND time_clause_2 AND contextquery (chỉ lấy time_clause_2)
        if proj_clause:
            jql = f"{proj_clause} AND {time_clause_2} AND {contextquery}"
        else:
            jql = f"{time_clause_2} AND {contextquery}"
        jql += " ORDER BY updated DESC"
        
        fields = [
            "summary",
            "status",
            "updated",
            "issuetype",
            "priority",
            "project",
            "components",
            "timeoriginalestimate",
            "assignee",
            "reporter",
            "description",
            "fixVersions",
            "statuscategorychangedate",
            "labels",
        ]
        print(f"[Jira] Query: Tasks updated in last {minutes} minutes OR CR tasks in last {cr_scan_days} days")
        try:
            issues = self.search_issues(jql, fields=fields, expand=None, max_results=400)
            print(f"[Jira] Found {len(issues)} tasks")
        except Exception as ex:
            self.logger.warning(f"Failed to search tasks: {ex}")
            print(f"[Jira] Query failed: {ex}")
            issues = []
        
        # Chuẩn hoá tất cả tasks
        tasks: List[Dict[str, Any]] = []
        for idx, issue in enumerate(issues, 1):
            print(f"[Jira] Chuẩn hoá task {idx}/{len(issues)}: {issue.get('key')}")
            task = self.build_task_object(issue)
            tasks.append(task)
        print(f"[Jira] Tổng tasks chuẩn hoá: {len(tasks)}")
        return tasks


__all__ = ["JiraClient"]
