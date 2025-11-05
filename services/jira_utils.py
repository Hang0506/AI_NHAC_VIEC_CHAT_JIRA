import os
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple, Union

import requests
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.auth import HTTPBasicAuth

from logger import get_logger

# Timezone Việt Nam
VN_TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")

def get_vn_now() -> datetime:
    """Lấy datetime hiện tại theo giờ Việt Nam."""
    return datetime.now(VN_TIMEZONE)


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
        # Thread-local sessions for parallel requests
        self._thread_local = threading.local()
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
                    self.projects = []
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
        # Use a thread-local session for thread safety when parallelizing
        sess = getattr(self._thread_local, "session", None)
        if sess is None:
            sess = requests.Session()
            # Mirror SSL verify setting
            sess.verify = self.session.verify
            self._thread_local.session = sess

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
        self._write_log_file(f"[{get_vn_now().strftime('%Y-%m-%d %H:%M:%S')}] {method.upper()} {url}")
        self._write_log_file(f"curl: {curl_cmd}")

        resp = sess.request(
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
            #đoạn code lấy thông tin project từ Jira
            #resp = self._request("GET", "/rest/api/2/project")
            #if resp.status_code != 200:
                #self.logger.warning(f"get_all_projects failed: {resp.status_code} - {resp.text[:200]}")
                #return []
            #projects_data = resp.json()
            #project_keys = [p.get("key", "").upper() for p in projects_data if p.get("key")]
            #project_keys = [p for p in project_keys if p]  # Loại bỏ empty strings
            #print(f"[Jira] Lấy được {len(project_keys)} projects: {', '.join(project_keys)}")
            #return project_keys
            return []
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
        use_parallel: bool = False,
        parallel_workers: int = 8,
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
        # First page to discover total and optionally reuse results
        print("[Jira] Bắt đầu tìm kiếm issues theo JQL...")
        print(f"[Jira] JQL: {jql}")
        first_page_start = page  # Lưu lại startAt của trang đầu để tính toán chính xác
        params: Dict[str, Any] = {
            "jql": jql,
            "maxResults": max_results,
            "startAt": first_page_start,
        }
        if fields:
            params["fields"] = ",".join(fields)
        if expand:
            params["expand"] = ",".join(expand)
        if show_first_url:
            self.logger.info("Search API initialized (URL hidden, see curl in debug logs)")
        print(f"[Jira] Trang 1 (startAt={first_page_start}, maxResults={max_results})")
        params_json = json.dumps(params, ensure_ascii=False, indent=2)
        self.logger.info(f"API parameters: {params_json}")
        print(f"[Jira] API parameters: {params_json}")
        self._write_log_file(f"API parameters: {params_json}")

        resp = self._request("GET", "/rest/api/2/search", params=params)
        if resp.status_code != 200:
            details = resp.text.strip() if isinstance(resp.text, str) else ""
            raise RuntimeError(
                f"JQL search failed: {resp.status_code} | auth_type={self.auth_type} | verify_ssl={self.session.verify} | body={details[:300]}"
            )
        data = resp.json()
        issues = data.get("issues", [])
        collected.extend(issues)
        total = data.get("total", 0)
        first_page_count = len(issues)
        print(f"[Jira] Trang đầu: thu được {first_page_count} issue (tổng lũy kế: {len(collected)}/{total})")

        # Nếu đã đủ hoặc không có dữ liệu, return ngay
        if total == 0 or len(collected) >= total:
            print(f"[Jira] Hoàn tất tìm kiếm. Tổng số issue: {len(collected)}")
            return collected

        # If not using parallel, continue sequentially
        if not use_parallel:
            current_start = first_page_start + max_results
            page_num = 2
            while current_start < total:
                print(f"[Jira] Trang {page_num} (startAt={current_start}, maxResults={max_results})")
                params["startAt"] = current_start
                params_json_page = json.dumps(params, ensure_ascii=False, indent=2)
                self.logger.info(f"API parameters (page {page_num}): {params_json_page}")
                print(f"[Jira] API parameters (page {page_num}): {params_json_page}")
                self._write_log_file(f"API parameters (page {page_num}): {params_json_page}")
                resp2 = self._request("GET", "/rest/api/2/search", params=params)
                print(f"[Jira] Response (page {page_num}): status={resp2.status_code}")
                self.logger.info(f"Response (page {page_num}): status={resp2.status_code}")
                if resp2.status_code != 200:
                    details2 = resp2.text.strip() if isinstance(resp2.text, str) else ""
                    print(f"[Jira] Response error (page {page_num}): {details2[:500]}")
                    self.logger.warning(f"Response error (page {page_num}): {details2[:500]}")
                    raise RuntimeError(
                        f"JQL search failed: {resp2.status_code} | body={details2[:300]}"
                    )
                try:
                    data2 = resp2.json()
                    issues2 = data2.get("issues", [])
                    total_from_response = data2.get("total", 0)
                    print(f"[Jira] Response (page {page_num}): total={total_from_response}, issues={len(issues2)}")
                    self.logger.info(f"Response (page {page_num}): total={total_from_response}, issues={len(issues2)}")
                    collected.extend(issues2)
                    print(f"[Jira] Trang {page_num}: thu được {len(issues2)} issue (tổng lũy kế: {len(collected)}/{total})")
                except Exception as ex:
                    print(f"[Jira] Error parsing response (page {page_num}): {ex}")
                    print(f"[Jira] Response text (first 500 chars): {resp2.text[:500] if hasattr(resp2, 'text') else 'N/A'}")
                    self.logger.warning(f"Error parsing response (page {page_num}): {ex}")
                    raise
                current_start += max_results
                page_num += 1
                # Safety check: stop if we've collected enough
                if len(collected) >= total:
                    break
            print(f"[Jira] Hoàn tất tìm kiếm. Tổng số issue: {len(collected)}")
            return collected

        # Parallel fetch remaining pages (chỉ các trang sau trang đầu)
        # Tính toán chính xác các startAt còn lại, bắt đầu từ trang tiếp theo sau trang đầu
        remaining_starts: List[int] = []
        next_start = first_page_start + max_results  # Trang tiếp theo sau trang đầu
        while next_start < total:
            remaining_starts.append(next_start)
            next_start += max_results

        if not remaining_starts:
            print(f"[Jira] Không còn trang nào cần fetch. Hoàn tất tìm kiếm. Tổng số issue: {len(collected)}")
            return collected

        print(f"[Jira] Parallel fetching {len(remaining_starts)} trang còn lại (startAt từ {remaining_starts[0]} đến {remaining_starts[-1]}) với {parallel_workers} workers")

        def fetch_page(start_at_value: int) -> List[Dict[str, Any]]:
            """Fetch một trang với startAt cụ thể."""
            local_params = dict(params)
            local_params["startAt"] = start_at_value
            params_json_parallel = json.dumps(local_params, ensure_ascii=False, indent=2)
            self.logger.info(f"API parameters (parallel startAt={start_at_value}): {params_json_parallel}")
            print(f"[Jira] API parameters (parallel startAt={start_at_value}): {params_json_parallel}")
            self._write_log_file(f"API parameters (parallel startAt={start_at_value}): {params_json_parallel}")
            r = self._request("GET", "/rest/api/2/search", params=local_params)
            if r.status_code != 200:
                raise RuntimeError(f"JQL page fetch failed: startAt={start_at_value} status={r.status_code}")
            d = r.json()
            return d.get("issues", [])

        # Fetch các trang còn lại song song
        with ThreadPoolExecutor(max_workers=max(1, int(parallel_workers))) as executor:
            futures = {executor.submit(fetch_page, s): s for s in remaining_starts}
            completed_count = 0
            for future in as_completed(futures):
                s = futures[future]
                try:
                    issues_page = future.result()
                    collected.extend(issues_page)
                    completed_count += 1
                    print(f"[Jira] (parallel) startAt={s} -> {len(issues_page)} issues (lũy kế: {len(collected)}/{total}, completed: {completed_count}/{len(remaining_starts)})")
                except Exception as ex:
                    self.logger.warning(f"Parallel page fetch failed at startAt={s}: {ex}")
                    print(f"[Jira] ERROR: Failed to fetch page startAt={s}: {ex}")

        print(f"[Jira] Hoàn tất tìm kiếm. Tổng số issue: {len(collected)} (mong đợi: {total})")
        if len(collected) != total:
            self.logger.warning(f"Số lượng issue thu được ({len(collected)}) khác với total từ Jira ({total})")
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
        created_raw = fields.get("created", "")
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
            "created": created_raw,
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
        # Due date (Jira format YYYY-MM-DD)
        task["duedate"] = fields.get("duedate")
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
    def search_recent_tasks(
        self,
        minutes: int,
        cr_scan_days: int = 3,
        excluded_statuses: Optional[List[str]] = None,
        test_emails: Optional[List[str]] = None,
        *,
        parallel: bool = True,
        parallel_workers: int = 8,
        page_size: int = 400,
    ) -> List[Dict[str, Any]]:
        """Tìm tasks cập nhật trong X phút gần đây hoặc CR tasks trong vòng X ngày.
        Gộp thành 1 query duy nhất: (tasks updated in minutes) OR (CR tasks updated in cr_scan_days)
        """
        if self.projects:
            project_values = ", ".join(["'{}'".format(p) for p in self.projects])
            proj_clause = f"project in ({project_values})"
        else:
            proj_clause = ""
        
        cr_filter = ""
        time_clause_2 = f"updated >= -{int(cr_scan_days)}d"
        contextquery = ''
        #contextquery = 'text ~ "Tạo link thanh toán Zalopay bổ sung field"'
        # Allow control of excluded statuses via a single list (overridable by caller)
        status_exclude = ""
        if excluded_statuses:
            excluded_statuses_jql = ", ".join(f'"{s}"' for s in excluded_statuses)
            status_exclude = f"status not in ({excluded_statuses_jql})"
        
        # Thêm điều kiện test emails nếu có
        test_email_filter = ""
        if test_emails and len(test_emails) > 0:
            email_values = ", ".join([f'"{email}"' for email in test_emails])
            test_email_filter = f"(assignee in ({email_values}) OR reporter in ({email_values}))"
        
        # Tạo JQL: project AND time_clause_2 AND status_exclude AND test_email_filter (nếu có)
        jql_parts = []
        if proj_clause:
            jql_parts.append(proj_clause)
        jql_parts.append(time_clause_2)
        if status_exclude:
            jql_parts.append(status_exclude)
        if test_email_filter:
            jql_parts.append(test_email_filter)
        if contextquery:
            jql_parts.append(contextquery)
        
        jql = " AND ".join(jql_parts)
        jql += " ORDER BY updated DESC"
        
        fields = [
            "summary",
            "status",
            "updated",
            "created",
            "issuetype",
            "priority",
            "project",
            "components",
            "timeoriginalestimate",
            "assignee",
            "reporter",
            "description",
            "fixVersions",
            "duedate",
            "statuscategorychangedate",
            "labels",
        ]
        try:
            issues = self.search_issues(
                jql,
                fields=fields,
                expand=None,
                max_results=page_size,
                use_parallel=parallel,
                parallel_workers=parallel_workers,
            )
            print(f"[Jira] Found {len(issues)} tasks")
        except Exception as ex:
            self.logger.warning(f"Failed to search tasks: {ex}")
            print(f"[Jira] Query failed: {ex}")
            issues = []
        
        # Chuẩn hoá tất cả tasks
        tasks: List[Dict[str, Any]] = []
        for idx, issue in enumerate(issues, 1):
            print(f"[Jira]  {idx}/{len(issues)}: {issue.get('key')}")
            task = self.build_task_object(issue)
            tasks.append(task)
        print(f"[Jira] Tổng tasks chuẩn hoá: {len(tasks)}")
        return tasks


__all__ = ["JiraClient"]
