import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import urllib.parse
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import json
import sys
import time
import re
import csv

def get_worklog(issue_key, jira_url, username, password):
    """
    Lấy thông tin log work của một issue
    
    Args:
        issue_key (str): Mã issue
        jira_url (str): URL của Jira
        username (str): Tên đăng nhập Jira
        password (str): Mật khẩu Jira
        
    Returns:
        tuple: (danh sách các log work, thông tin dự án, thông tin parent)
    """
    try:
        # Đầu tiên lấy thông tin chi tiết về issue để có được dự án
        issue_api_url = f"{jira_url}/rest/api/2/issue/{issue_key}"
        
        issue_response = requests.get(
            issue_api_url,
            auth=HTTPBasicAuth(username, password),
            headers={"Accept": "application/json"},
            timeout=30
        )
        
        # Thông tin dự án mặc định
        project_info = {
            "key": "",
            "name": "",
            "id": ""
        }
        
        # Thông tin parent task mặc định
        parent_info = {
            "key": "",
            "summary": "",
            "type": ""
        }
        
        # Kiểm tra response lấy thông tin issue
        if issue_response.status_code == 200:
            issue_data = issue_response.json()
            project = issue_data.get("fields", {}).get("project", {})
            project_info["key"] = project.get("key", "")
            project_info["name"] = project.get("name", "")
            project_info["id"] = project.get("id", "")
            print(f"   📂 Dự án: {project_info['key']} - {project_info['name']}")
            
            # Lấy thông tin parent task nếu có
            parent = issue_data.get("fields", {}).get("parent")
            if parent:
                parent_info["key"] = parent.get("key", "")
                parent_info["summary"] = parent.get("fields", {}).get("summary", "")
                parent_info["type"] = parent.get("fields", {}).get("issuetype", {}).get("name", "")
        else:
            print(f"⚠️ Không thể lấy thông tin dự án cho issue {issue_key}: {issue_response.status_code}")
        
        # Tiếp tục lấy worklog như bình thường
        api_url = f"{jira_url}/rest/api/2/issue/{issue_key}/worklog"
        
        response = requests.get(
            api_url,
            auth=HTTPBasicAuth(username, password),
            headers={"Accept": "application/json"},
            timeout=30
        )
        
        # Kiểm tra response
        if response.status_code != 200:
            print(f"⚠️ Lỗi khi lấy worklog của issue {issue_key}: {response.status_code}")
            return [], project_info, parent_info
            
        # Xử lý dữ liệu
        data = response.json()
        worklogs = data.get("worklogs", [])
        
        result = []
        for worklog in worklogs:
            try:
                author = worklog.get("author", {}).get("displayName", "")
                time_spent = worklog.get("timeSpent", "")
                time_spent_seconds = worklog.get("timeSpentSeconds", 0)
                started = worklog.get("started", "")
                comment = worklog.get("comment", "")
                
                # Chuyển đổi thời gian
                if started:
                    try:
                        started_date = datetime.fromisoformat(started.replace('Z', '+00:00')).strftime('%d/%m/%Y %H:%M')
                    except ValueError as e:
                        print(f"⚠️ Lỗi định dạng thời gian cho worklog của issue {issue_key}: {e}")
                        started_date = started
                else:
                    started_date = ""
                
                # Tính số giờ
                hours_spent = time_spent_seconds / 3600
                
                result.append({
                    "author": author,
                    "time_spent": time_spent,
                    "hours_spent": round(hours_spent, 2),
                    "started": started_date,
                    "comment": comment,
                    "project_key": project_info["key"],
                    "project_name": project_info["name"]
                })
            except Exception as e:
                print(f"⚠️ Lỗi khi xử lý worklog: {str(e)}")
                continue
        
        return result, project_info, parent_info
        
    except Exception as e:
        print(f"❌ Lỗi khi lấy worklog của issue {issue_key}: {str(e)}")
        return [], {"key": "", "name": "", "id": ""}, {"key": "", "summary": "", "type": ""}

def update_story_worklog_from_subtasks(all_tasks):
    """
    Cập nhật trạng thái has_worklog cho story dựa trên subtask của nó
    
    Args:
        all_tasks (list): Danh sách tất cả task
        
    Returns:
        list: Danh sách task đã được cập nhật
    """
    if not all_tasks:
        print("⚠️ Không có task nào để xử lý")
        return all_tasks
        
    print(f"\n🔄 Đang cập nhật trạng thái logwork cho story dựa trên subtask... (Tổng {len(all_tasks)} task)")
    
    # Tạo mapping giữa parent key và các subtask
    parent_to_subtasks = {}
    story_tasks = {}
    subtask_count = 0
    story_count = 0
    
    # Phân loại task với logging chi tiết
    for task in all_tasks:
        task_key = task.get('key', 'UNKNOWN')
        task_type = task.get('type', 'UNKNOWN')
        is_subtask = task.get('is_subtask', False)
        parent_key = task.get('parent_key', '')
        has_worklog = task.get('has_worklog', False)
        
        if is_subtask and parent_key:
            # Đây là subtask
            if parent_key not in parent_to_subtasks:
                parent_to_subtasks[parent_key] = []
            parent_to_subtasks[parent_key].append(task)
            subtask_count += 1
            print(f"   📋 Subtask: {task_key} (parent: {parent_key}) - Logwork: {'✓' if has_worklog else '✗'}")
        elif not is_subtask:
            # Đây là story hoặc task độc lập
            story_tasks[task_key] = task
            story_count += 1
            print(f"   📄 Story/Task: {task_key} ({task_type}) - Logwork: {'✓' if has_worklog else '✗'}")
    
    print(f"   📊 Tổng kết: {story_count} story/task, {subtask_count} subtask")
    print(f"   🔗 Tìm thấy {len(parent_to_subtasks)} story có subtask")
    
    # Debug: Hiển thị mapping
    for parent_key, subtasks in parent_to_subtasks.items():
        subtasks_with_logwork = [st for st in subtasks if st.get('has_worklog', False)]
        print(f"   📋 {parent_key}: {len(subtasks)} subtask ({len(subtasks_with_logwork)} có logwork)")
    
    # Cập nhật trạng thái logwork cho story
    stories_updated = 0
    stories_processed = 0
    
    for story_key, story in story_tasks.items():
        stories_processed += 1
        
        if story_key in parent_to_subtasks:
            # Story này có subtask
            subtasks = parent_to_subtasks[story_key]
            story_has_worklog = story.get('has_worklog', False)
            
            # Kiểm tra xem có subtask nào có logwork không
            subtasks_with_logwork = [st for st in subtasks if st.get('has_worklog', False)]
            
            print(f"   🔍 Kiểm tra story {story_key}:")
            print(f"     - Story có logwork riêng: {'✓' if story_has_worklog else '✗'}")
            print(f"     - Subtask có logwork: {len(subtasks_with_logwork)}/{len(subtasks)}")
            
            # Cập nhật nếu story chưa có logwork nhưng có subtask có logwork
            if subtasks_with_logwork and not story_has_worklog:
                print(f"   ✅ Cập nhật story {story_key}: có {len(subtasks_with_logwork)}/{len(subtasks)} subtask đã logwork")
                
                # Cập nhật trạng thái logwork
                story['has_worklog'] = True
                story['has_child_with_logwork'] = True
                
                # Tính tổng thời gian từ subtask nếu story chưa có worklog riêng
                current_story_hours = story.get('total_hours', 0)
                if current_story_hours == 0:
                    total_subtask_hours = sum(st.get('total_hours', 0) for st in subtasks_with_logwork)
                    story['total_hours'] = round(total_subtask_hours, 2)
                    print(f"     📊 Cập nhật thời gian story: {current_story_hours}h → {story['total_hours']}h")
                
                # Cập nhật time_saved_hours nếu đang là -1 (chưa có logwork)
                current_time_saved = story.get('time_saved_hours', -1)
                if current_time_saved == -1:
                    original_estimate = story.get('original_estimate_hours', 0)
                    if original_estimate > 0:
                        story['time_saved_hours'] = original_estimate - story.get('total_hours', 0)
                        story['time_saved_hours'] = round(story['time_saved_hours'], 2)
                        
                        if story['time_saved_hours'] > 0:
                            saving_percent = (story['time_saved_hours'] / original_estimate) * 100
                            story['time_saved_percent'] = round(saving_percent, 1)
                            print(f"     💰 Tiết kiệm: {story['time_saved_hours']}h ({story['time_saved_percent']}%)")
                        else:
                            story['time_saved_percent'] = 0
                            print(f"     ⚠️ Vượt thời gian: {abs(story['time_saved_hours'])}h")
                    else:
                        story['time_saved_hours'] = 0
                        story['time_saved_percent'] = 0
                        print(f"     ℹ️ Không có estimate, đặt time_saved_hours = 0")
                
                stories_updated += 1
                
                # Hiển thị danh sách subtask có logwork
                for st in subtasks_with_logwork:
                    print(f"     └─ {st.get('key')}: {st.get('total_hours', 0)}h")
            elif story_has_worklog:
                print(f"   ℹ️ Story {story_key} đã có logwork riêng, không cần cập nhật")
            elif not subtasks_with_logwork:
                print(f"   ⚠️ Story {story_key} không có subtask nào có logwork")
    
    print(f"✅ Đã xử lý {stories_processed} story, cập nhật {stories_updated} story dựa trên logwork của subtask")
    
    if stories_updated == 0 and len(parent_to_subtasks) > 0:
        print("⚠️ CẢNH BÁO: Có story có subtask nhưng không story nào được cập nhật!")
        print("   Có thể nguyên nhân:")
        print("   - Tất cả story đã có logwork riêng")
        print("   - Subtask không có logwork")
        print("   - Logic điều kiện có vấn đề")
    
    return all_tasks

def filter_parent_tasks_without_updated_children(all_tasks, filter_enabled=True):
    """
    Lọc bỏ task cha khi tất cả task con không có update
    
    Args:
        all_tasks (list): Danh sách tất cả task
        filter_enabled (bool): Có bật tính năng lọc không
        
    Returns:
        list: Danh sách task đã được lọc
    """
    if not filter_enabled:
        return all_tasks
        
    print(f"\n🔍 Đang kiểm tra task cha không có task con với update...")
    
    # Tạo mapping giữa parent key và các subtask
    parent_to_children = {}
    parent_tasks = {}
    
    # Phân loại task
    for task in all_tasks:
        task_key = task.get('key')
        if task.get('is_subtask') and task.get('parent_key'):
            # Đây là subtask
            parent_key = task.get('parent_key')
            if parent_key not in parent_to_children:
                parent_to_children[parent_key] = []
            parent_to_children[parent_key].append(task)
        elif not task.get('is_subtask'):
            # Đây là task cha hoặc task độc lập
            parent_tasks[task_key] = task
    
    # Tìm task cha cần loại bỏ
    tasks_to_remove = []
    
    for parent_key, parent_task in parent_tasks.items():
        if parent_key in parent_to_children:
            # Task này có các task con
            children = parent_to_children[parent_key]
            
            # Kiểm tra xem có task con nào có update không
            children_with_update = []
            for child in children:
                has_update = (
                    child.get('has_worklog', False) or 
                    child.get('last_update_time') or
                    child.get('update_reasons', [])
                )
                if has_update:
                    children_with_update.append(child)
            
            # Nếu không có task con nào có update, đánh dấu task cha để loại bỏ
            if not children_with_update:
                tasks_to_remove.append(parent_key)
                print(f"   ❌ Task cha {parent_key} sẽ bị loại bỏ vì không có task con nào có update")
                print(f"      └─ Có {len(children)} task con, tất cả đều không có update")
            else:
                print(f"   ✅ Task cha {parent_key} được giữ lại")
                print(f"      └─ Có {len(children_with_update)}/{len(children)} task con có update")
    
    # Lọc bỏ task cha và task con của chúng
    filtered_tasks = []
    removed_count = 0
    
    for task in all_tasks:
        task_key = task.get('key')
        parent_key = task.get('parent_key')
        
        # Nếu là task cha bị đánh dấu loại bỏ
        if task_key in tasks_to_remove:
            removed_count += 1
            continue
            
        # Nếu là task con của task cha bị đánh dấu loại bỏ
        if parent_key and parent_key in tasks_to_remove:
            removed_count += 1
            continue
            
        # Giữ lại task
        filtered_tasks.append(task)
    
    print(f"   📊 Đã loại bỏ {removed_count} task (bao gồm {len(tasks_to_remove)} task cha và task con của chúng)")
    print(f"   📋 Còn lại {len(filtered_tasks)}/{len(all_tasks)} task")
    
    return filtered_tasks

def get_employee_tasks(employee_identifier, start_date, end_date, jira_url, username, password, request_delay=0.1, include_worklog=True, is_email=True, include_reported=False, show_jql=True, time_field="updatedDate", jira_project_filter=None, jira_project_exclude=None, jira_status_exclude=None, ignore_fix_version_sprint_updates=True, assignee_updates_only=False, status_updates_only=False, skill_group=None, filter_parent_without_updated_children=True):
    """
    Lấy danh sách task của một nhân viên từ Jira
    
    Args:
        employee_identifier (str): Email hoặc username của nhân viên
        start_date (str): Ngày bắt đầu (định dạng yyyy-MM-dd)
        end_date (str): Ngày kết thúc (định dạng yyyy-MM-dd)
        jira_url (str): URL của Jira
        username (str): Tên đăng nhập Jira
        password (str): Mật khẩu Jira
        request_delay (float): Thời gian trễ giữa các request (giây)
        include_worklog (bool): Có lấy thông tin log work hay không
        is_email (bool): True nếu employee_identifier là email, False nếu là username
        include_reported (bool): True nếu bao gồm cả task do nhân viên báo cáo hoặc tạo
        show_jql (bool): True nếu muốn hiển thị JQL query
        time_field (str): Trường thời gian sử dụng để lọc (updatedDate, created, resolutiondate)
        jira_project_filter (list): Danh sách mã dự án Jira cần lọc
        jira_project_exclude (list): Danh sách mã dự án Jira cần loại bỏ
        jira_status_exclude (list): Danh sách trạng thái Jira cần loại bỏ
        ignore_fix_version_sprint_updates (bool): Bỏ qua các cập nhật chỉ liên quan đến Fix Version hoặc Sprint
        assignee_updates_only (bool): Chỉ lấy cập nhật quan trọng của người được gán task
        status_updates_only (bool): Chỉ lấy cập nhật thay đổi trạng thái do chính assignee thực hiện
        skill_group (str): Nhóm kỹ năng của nhân viên. Nếu là "Test", chỉ lấy issue có status DONE hoặc COMPLETED
        filter_parent_without_updated_children (bool): Lọc bỏ task cha khi tất cả task con không có update
        
    Returns:
        list: Danh sách các task
    """
    try:
        # Tạo JQL để tìm kiếm task của nhân viên trong khoảng thời gian
        if is_email:
            # Thử tìm kiếm theo email trước
            if include_reported:
                jql_query = f"(assignee = '{employee_identifier}' OR reporter = '{employee_identifier}') AND {time_field} >= '{start_date}' AND {time_field} <= '{end_date}'"
            else:
                jql_query = f"assignee = '{employee_identifier}' AND {time_field} >= '{start_date}' AND {time_field} <= '{end_date}'"
        else:
            # Nếu không phải email, tìm kiếm theo username cũng sử dụng dấu nháy đơn để đồng nhất
            if include_reported:
                jql_query = f"(assignee = '{employee_identifier}' OR reporter = '{employee_identifier}') AND {time_field} >= '{start_date}' AND {time_field} <= '{end_date}'"
            else:
                jql_query = f"assignee = '{employee_identifier}' AND {time_field} >= '{start_date}' AND {time_field} <= '{end_date}'"
        
        # Thêm bộ lọc dự án Jira nếu có
        if jira_project_filter is None or len(jira_project_filter) == 0:
            jira_project_filter = ["FC", "FSS"]  # Giá trị mặc định nếu không có
            print(f"   ℹ️ Sử dụng bộ lọc dự án mặc định: {', '.join(jira_project_filter)}")
            
        # Đảm bảo luôn thêm bộ lọc dự án
        project_clause = " AND project in (" + ", ".join([f"'{p}'" for p in jira_project_filter]) + ")"
        jql_query += project_clause
        
        # Thêm mệnh đề loại bỏ dự án nếu có 
        if jira_project_exclude:
            exclude_clause = " AND project not in (" + ", ".join([f"'{p}'" for p in jira_project_exclude]) + ")"
            jql_query += exclude_clause
            
        # Thêm mệnh đề loại bỏ trạng thái nếu có
        if jira_status_exclude:
            status_exclude_clause = " AND status not in (" + ", ".join([f"'{s}'" for s in jira_status_exclude]) + ")"
            jql_query += status_exclude_clause
            
        # Áp dụng filter đặc biệt cho nhân viên có SKILL_GROUP là "Test"
        if skill_group and skill_group.upper() == "TEST":
            test_status_clause = " AND status in ('DONE', 'COMPLETED')"
            jql_query += test_status_clause
            print(f"   ℹ️ Áp dụng filter đặc biệt cho SKILL_GROUP 'Test': chỉ lấy issue có status DONE hoặc COMPLETED")
            
        encoded_jql = urllib.parse.quote(jql_query)
        
        # Hiển thị JQL query
        if show_jql:
            print(f"   🔍 JQL Query: {jql_query}")
        
        # Gửi request đến Jira API
        max_results = 1000
        start_at = 0
        all_issues = []
        
        while True:
            api_url = f"{jira_url}/rest/api/2/search?jql={encoded_jql}&maxResults={max_results}&startAt={start_at}"
            
            # Hiển thị URL trong lần lặp đầu tiên
            if start_at == 0 and show_jql:
                print(f"   🌐 API URL: {jira_url}/rest/api/2/search?jql=...")
            
            response = requests.get(
                api_url,
                auth=HTTPBasicAuth(username, password),
                headers={"Accept": "application/json"},
                timeout=30
            )
            
            # Kiểm tra response
            if response.status_code != 200:
                if is_email and "Error in the JQL Query" in response.text:
                    # Nếu tìm theo email bị lỗi và đây là lần đầu thử, thử lại với username
                    print(f"   ⚠️ Không tìm thấy task với email, thử tìm với username...")
                    return get_employee_tasks(employee_identifier.split('@')[0], start_date, end_date, 
                                           jira_url, username, password, request_delay, include_worklog, False, 
                                           include_reported, show_jql, time_field, jira_project_filter, jira_project_exclude, jira_status_exclude, ignore_fix_version_sprint_updates, assignee_updates_only, status_updates_only, skill_group, filter_parent_without_updated_children)
                else:
                    print(f"❌ Lỗi khi lấy dữ liệu từ Jira: {response.status_code} - {response.text}")
                    return []
                
            # Xử lý dữ liệu
            data = response.json()
            issues = data.get("issues", [])
            
            # Nếu không tìm thấy issues và đang tìm theo email, thử chuyển sang tìm theo username
            if not issues and is_email:
                print(f"   ⚠️ Không tìm thấy task với email, thử tìm với username...")
                return get_employee_tasks(employee_identifier.split('@')[0], start_date, end_date, 
                                       jira_url, username, password, request_delay, include_worklog, False, 
                                       include_reported, show_jql, time_field, jira_project_filter, jira_project_exclude, jira_status_exclude, ignore_fix_version_sprint_updates, assignee_updates_only, status_updates_only, skill_group, filter_parent_without_updated_children)
                
            all_issues.extend(issues)
            
            # Kiểm tra phân trang
            if len(issues) < max_results or start_at + len(issues) >= data.get("total", 0):
                break
            
            # Tăng chỉ số bắt đầu cho trang tiếp theo
            start_at += max_results
            
            # Thêm độ trễ giữa các request để giảm tải cho server
            if request_delay > 0:
                time.sleep(request_delay)
        
        # Thống kê số lượng và trạng thái các issue trả về từ API
        if all_issues:
            # Thống kê theo dự án
            api_projects = {}
            # Thống kê theo status
            status_counts = {}
            
            for issue in all_issues:
                # Thống kê theo dự án
                project_key = issue.get("fields", {}).get("project", {}).get("key", "Unknown")
                if project_key not in api_projects:
                    api_projects[project_key] = 0
                api_projects[project_key] += 1
                
                # Thống kê theo status
                status = issue.get("fields", {}).get("status", {}).get("name", "Unknown")
                if status not in status_counts:
                    status_counts[status] = 0
                status_counts[status] += 1
            
            print(f"   ℹ️ Tổng số issue tìm thấy: {len(all_issues)}")
            print(f"   ℹ️ Dự án trả về từ API: {', '.join([f'{k}({v})' for k,v in api_projects.items()])}")
            print(f"   ℹ️ Các trạng thái của issue:")
            for status, count in sorted(status_counts.items(), key=lambda x: x[1], reverse=True):
                print(f"      - {status}: {count} issues ({count/len(all_issues)*100:.1f}%)")
            
            # Kiểm tra xem có dự án nào không nằm trong bộ lọc không
            if jira_project_filter:
                unexpected_projects = [p for p in api_projects.keys() if p not in jira_project_filter]
                if unexpected_projects:
                    print(f"   ⚠️ Phát hiện dự án không nằm trong bộ lọc: {', '.join(unexpected_projects)}")
        
        # Xử lý và trả về kết quả
        result = []
        filtered_issues = []
        
        # Lọc bỏ các task có component là "Ecom - Pending"
        issues_before_filter = len(all_issues)
        all_issues = [issue for issue in all_issues if "Ecom - Pending" not in [component.get("name", "") for component in issue.get("fields", {}).get("components", [])] and issue.get("fields", {}).get("issuetype", {}).get("name", "") != "Epic"]
        issues_filtered = issues_before_filter - len(all_issues)
        if issues_filtered > 0:
            print(f"   ⚠️ Đã loại bỏ {issues_filtered} task có component \"Ecom - Pending\"")
        
        # Nếu chọn bỏ qua cập nhật Fix Version/Sprint/RemoteIssueLink, lọc thêm dựa trên thời gian cập nhật thực
        if ignore_fix_version_sprint_updates and time_field == "updatedDate":
            print(f"   ℹ️ Đang kiểm tra thời gian cập nhật thực cho {len(all_issues)} task...")
            start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_date_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)  # Cuối ngày end_date
            
            for issue in all_issues:
                try:
                    key = issue.get("key", "")
                    
                    # Lấy thông tin người được gán task (assignee) trước khi gọi get_update_reason
                    assignee = issue.get("fields", {}).get("assignee", {})
                    current_assignee_name = assignee.get("displayName", "") if assignee else "Unassigned"
                    
                    # Lấy changelog chi tiết
                    update_info = get_update_reason(key, jira_url, username, password, current_assignee_name, assignee_updates_only, status_updates_only)
                    
                    if update_info["last_update_time"]:
                        try:
                            # Chuyển đổi thời gian cập nhật quan trọng thành datetime
                            update_time = datetime.strptime(update_info["last_update_time"], "%d/%m/%Y %H:%M")
                            
                            # Kiểm tra xem thời gian cập nhật có nằm trong khoảng cần lọc không
                            if start_date_dt <= update_time <= end_date_dt:
                                filtered_issues.append(issue)
                                print(f"   ✅ Task {key} có cập nhật quan trọng vào {update_info['last_update_time']} - Trong khoảng thời gian cần lọc")
                            else:
                                print(f"   ❌ Task {key} có cập nhật quan trọng vào {update_info['last_update_time']} - Ngoài khoảng thời gian cần lọc")
                        except Exception as e:
                            print(f"   ⚠️ Lỗi khi phân tích thời gian cập nhật của task {key}: {str(e)}")
                            # Nếu có lỗi thì vẫn giữ lại task
                            filtered_issues.append(issue)
                    else:
                        # Nếu không có thông tin cập nhật, giữ lại task
                        filtered_issues.append(issue)
                        print(f"   ℹ️ Task {key} không có thông tin cập nhật")
                except Exception as e:
                    print(f"   ⚠️ Lỗi khi kiểm tra lịch sử cập nhật của task {issue.get('key', '')}: {str(e)}")
                    # Vẫn giữ lại task nếu có lỗi
                    filtered_issues.append(issue)
            
            print(f"   ℹ️ Đã lọc {len(filtered_issues)}/{len(all_issues)} task dựa trên thời gian cập nhật thực")
            all_issues = filtered_issues
        
        # Xử lý và trả về kết quả
        result = []
        for issue in all_issues:
            try:
                key = issue.get("key", "")
                summary = issue.get("fields", {}).get("summary", "")
                status = issue.get("fields", {}).get("status", {}).get("name", "")
                updated = issue.get("fields", {}).get("updated", "")
                issue_type = issue.get("fields", {}).get("issuetype", {}).get("name", "")
                priority = issue.get("fields", {}).get("priority", {}).get("name", "")
                project = issue.get("fields", {}).get("project", {}).get("key", "").upper()
                
                # Lấy thông tin người được gán task (assignee)
                assignee = issue.get("fields", {}).get("assignee", {})
                assignee_name = assignee.get("displayName", "") if assignee else "Unassigned"
                assignee_email = assignee.get("emailAddress", "") if assignee else ""
                
                # Nếu có bộ lọc dự án, kiểm tra xem dự án của task có nằm trong bộ lọc không
                if jira_project_filter and project not in jira_project_filter:
                    print(f"   ⚠️ Bỏ qua task {key} của dự án {project} (không nằm trong bộ lọc)")
                    continue
                
                # Lấy thông tin components của task
                components = issue.get("fields", {}).get("components", [])
                component_names = [component.get("name", "") for component in components]
                component_str = ", ".join(component_names) if component_names else "Không có component"
                
                # Xác định dự án thực tế
                actual_project = get_actual_project(project, component_names)
                
                                # DEBUG: Theo dõi việc gán actual_project cho PKT và WAK
                # if project == "PKT":
                #     print(f"🔍 DEBUG: Task {key} từ PKT được gán actual_project = '{actual_project}'")
                #     print(f"           Components: {component_names}")
                #     if actual_project == "PKT":
                #         print(f"🚨 LỖI: Task {key} từ PKT KHÔNG được chuyển đổi! Kiểm tra hàm get_actual_project()")
                
                # if project == "WAK":
                #     print(f"🔍 DEBUG: Task {key} từ WAK được gán actual_project = '{actual_project}'")
                #     print(f"           Components: {component_names}")
                #     if actual_project == "WAK":
                #         print(f"🚨 LỖI: Task {key} từ WAK KHÔNG được chuyển đổi! Kiểm tra hàm get_actual_project()")
                
                # Filter: Chỉ giữ lại tasks với logic mới (loại bỏ tasks với components legacy)
                if project == "FC" and actual_project == project:
                    # Nếu task từ FC project mà không được phân loại thành business project nào
                    # thì đây là task với components legacy, bỏ qua
                    legacy_components = []
                    
                    # Kiểm tra các components legacy đã bị loại bỏ (chỉ còn Kho Tổng)
                    legacy_kho_tong = ["IMS-WMS", "IMS-POMS", "B17.PIM"]
                    
                    for comp in component_names:
                        if comp in legacy_kho_tong:
                            legacy_components.append(comp)
                    
                    if legacy_components:
                        print(f"   🚫 Bỏ qua task {key} với components legacy: {', '.join(legacy_components)}")
                        continue
                    
                    # Nếu không có components nào hoặc components không match logic mới thì cũng bỏ qua
                    if not component_names or actual_project == "FC":
                        print(f"   🚫 Bỏ qua task {key} từ FC không thuộc business project nào")
                        continue
                
                # Xử lý custom field an toàn
                customfield_10000 = issue.get("fields", {}).get("customfield_10000", "")
                if isinstance(customfield_10000, dict) and "value" in customfield_10000:
                    skill_group = customfield_10000.get("value", "")
                else:
                    skill_group = ""
                    
                # Lấy project name an toàn
                project_obj = issue.get("fields", {}).get("project", {})
                if isinstance(project_obj, dict) and "name" in project_obj:
                    project_name = project_obj.get("name", "")
                else:
                    project_name = ""
                
                # Lấy thông tin log work
                worklogs = []
                total_hours = 0
                project_info = {}
                parent_info = {}
                
                # Khởi tạo các biến tính toán tiết kiệm thời gian
                # Giá trị mặc định cho time_saved_hours là -1 (không có log work)
                time_saved_hours = -1
                time_saved_percent = 0
                is_completed = False
                
                # Lấy thông tin ước tính thời gian (Original Estimate)
                original_estimate_seconds = issue.get("fields", {}).get("timeoriginalestimate", 0) or 0
                original_estimate_hours = original_estimate_seconds / 3600
                
                if include_worklog:
                    # Kiểm tra loại task
                    is_subtask = issue_type == "Sub-task"
                    
                    if is_subtask:
                        print(f"   🔄 Đang lấy worklog cho sub-task {key} [{issue_type} - {status}]...")
                    else:
                        print(f"   🔄 Đang lấy worklog cho issue {key} [{issue_type} - {status}]...")
                    
                    # Hiển thị thông tin ước tính (nếu có)
                    if original_estimate_seconds > 0:
                        print(f"   ⏱️ Thời gian ước tính (không AI): {original_estimate_hours:.2f}h")
                    
                    worklogs, project_info, parent_info = get_worklog(key, jira_url, username, password)
                    
                    # Hiển thị thông tin task cha nếu đây là sub-task
                    if parent_info and parent_info.get("key"):
                        print(f"   📌 Sub-task của: {parent_info.get('key')} - {parent_info.get('summary')} [{parent_info.get('type')}]")
                    
                    # Tính tổng số giờ log work
                    if worklogs:
                        total_hours = sum(worklog.get("hours_spent", 0) for worklog in worklogs)
                        
                        if original_estimate_seconds > 0:
                            # Đã có log work và có estimate, mặc định đặt time_saved_hours = 0 (có log work nhưng không tiết kiệm)
                            time_saved_hours = 0
                            
                            # Chỉ tính tiết kiệm cho task đã hoàn thành và không đang triển khai (IMPLEMENTING)
                            is_completed = "IMPLEMENTING" not in status.upper()
                            if is_completed and original_estimate_hours > 0:
                                # Tính toán thời gian tiết kiệm thực tế
                                saved_hours, saving_ratio = calculate_saved_time(original_estimate_hours, total_hours)
                                time_saved_hours = saved_hours
                                time_saved_percent = saving_ratio
                            
                            # Hiển thị thông tin
                            if is_completed:
                                if time_saved_hours > 0:
                                    print(f"   💰 Tiết kiệm được: {time_saved_hours:.2f}h ({time_saved_percent:.1f}%)")
                                elif time_saved_hours == 0:
                                    print(f"   ⚙️ Sử dụng đúng thời gian ước tính")
                                else:
                                    print(f"   ⚠️ Vượt thời gian: {abs(time_saved_hours):.2f}h")
                            else:
                                print(f"   ℹ️ Task chưa hoàn thành, không tính tiết kiệm")
                        else:
                            # Có log work nhưng không có estimate
                            time_saved_hours = -2  # Đánh dấu đặc biệt cho trường hợp này
                            time_saved_percent = 0
                            is_completed = "IMPLEMENTING" not in status.upper()
                            print(f"   ⏱️ Đã log work {total_hours:.2f}h nhưng không có estimate")
                    else:
                        # Không có log work, giá trị time_saved_hours vẫn là -1
                        total_hours = 0
                        time_saved_hours = -1
                        time_saved_percent = 0
                        is_completed = False
                        print(f"   ⚠️ Task chưa có log work nào")
                    
                    # Thêm độ trễ giữa các request để giảm tải cho server
                    if request_delay > 0:
                        time.sleep(request_delay)
                
                # Chuyển đổi thời gian cập nhật và lấy lý do cập nhật cho TẤT CẢ các task, không chỉ cập nhật hôm nay
                updated_dt = datetime.fromisoformat(updated.replace('Z', '+00:00'))
                updated_date = updated_dt.strftime('%d/%m/%Y %H:%M')

                # Lấy thông tin cập nhật cho tất cả các task
                update_info = get_update_reason(key, jira_url, username, password, assignee_name, assignee_updates_only, status_updates_only)
                if update_info["last_updater"]:
                    last_updater_name = update_info['last_updater']['name']
                    main_reason = update_info.get('main_update_reason', 'Không xác định')
                    update_category = update_info.get('update_category', 'unknown')
                    
                    # Hiển thị lý do chính và thông tin cập nhật
                    print(f"   🎯 Lý do: {main_reason}")
                    print(f"   👤 Cập nhật cuối: {update_info['last_update_time']} bởi {last_updater_name}")
                    
                    # Kiểm tra và hiển thị cảnh báo nếu người cập nhật cuối cùng khác với người được gán
                    if assignee_name and last_updater_name and assignee_name != last_updater_name:
                        print(f"   ⚠️ CHÚ Ý: Người cập nhật cuối ({last_updater_name}) khác với người được gán task ({assignee_name})")
                    
                    # Hiển thị chi tiết thay đổi (bỏ qua dòng đầu tiên vì đã hiển thị lý do chính)
                    detail_reasons = update_info["reasons"][1:4] if len(update_info["reasons"]) > 1 else []
                    for reason in detail_reasons:
                        print(f"     {reason}")
                    if len(update_info["reasons"]) > 4:
                        print(f"     ... và {len(update_info['reasons']) - 4} thay đổi khác")
                
                # Ghi đè project name từ dữ liệu mới nhất nếu có
                if project_info and project_info.get("name"):
                    project_name = project_info.get("name", project_name)
                
                result.append({
                    "key": key,
                    "summary": summary,
                    "status": status,
                    "updated": updated_date,
                    "type": issue_type,
                    "priority": priority,
                    "project": project,
                    "project_name": project_name,
                    "components": component_names,
                    "component_str": component_str,
                    "actual_project": actual_project,  # Thêm trường dự án thực tế
                    "link": f"{jira_url}/browse/{key}",
                    "worklogs": worklogs,
                    "total_hours": round(total_hours, 2),
                    "has_worklog": len(worklogs) > 0,
                    "parent_key": parent_info.get("key", ""),
                    "parent_summary": parent_info.get("summary", ""),
                    "is_subtask": issue_type == "Sub-task",
                    "original_estimate_hours": round(original_estimate_hours, 2),
                    "time_saved_hours": round(time_saved_hours, 2),
                    "time_saved_percent": round(time_saved_percent, 1),
                    "is_completed": is_completed,
                    "has_estimate": original_estimate_seconds > 0,  # Thêm trường đánh dấu có estimate hay không
                    "update_reasons": update_info["reasons"],
                    "last_updater": update_info.get("last_updater", {}),
                    "last_update_time": update_info.get("last_update_time", ""),
                    "main_update_reason": update_info.get("main_update_reason", "Không xác định"),
                    "update_category": update_info.get("update_category", "unknown"),
                    "assignee_name": assignee_name,
                    "assignee_email": assignee_email,
                    "is_different_updater": assignee_name and last_updater_name and assignee_name != last_updater_name
                })
            except Exception as e:
                print(f"⚠️ Lỗi khi xử lý issue {issue.get('key', 'Không xác định')}: {str(e)}")
                # Vẫn thêm vào danh sách kết quả nhưng với các giá trị mặc định
                try:
                    # Khởi tạo các biến local cần thiết với giá trị mặc định
                    key = issue.get("key", "")
                    summary = issue.get("fields", {}).get("summary", "")
                    status = issue.get("fields", {}).get("status", {}).get("name", "") if "fields" in issue else ""
                    updated = issue.get("fields", {}).get("updated", "") if "fields" in issue else ""
                    issue_type = issue.get("fields", {}).get("issuetype", {}).get("name", "") if "fields" in issue else ""
                    priority = issue.get("fields", {}).get("priority", {}).get("name", "") if "fields" in issue else ""
                    project = issue.get("fields", {}).get("project", {}).get("key", "").upper() if "fields" in issue else ""
                    project_name = issue.get("fields", {}).get("project", {}).get("name", "") if "fields" in issue else ""
                    
                    # Lấy thông tin người được gán task (assignee)
                    assignee = issue.get("fields", {}).get("assignee", {}) if "fields" in issue else {}
                    assignee_name = assignee.get("displayName", "") if assignee else "Unassigned"
                    assignee_email = assignee.get("emailAddress", "") if assignee else ""
                    
                    # Khởi tạo các giá trị tính toán
                    original_estimate_hours = 0
                    time_saved_hours = -1  # Không có logwork
                    time_saved_percent = 0
                    is_completed = False
                    last_updater_name = ""
                    
                    # Chuyển đổi thời gian cập nhật nếu có
                    updated_date = ""
                    if updated:
                        try:
                            updated_dt = datetime.fromisoformat(updated.replace('Z', '+00:00'))
                            updated_date = updated_dt.strftime('%d/%m/%Y %H:%M')
                        except Exception:
                            pass
                    
                    # Thêm vào với thông tin cơ bản và các giá trị tính toán mặc định
                    result.append({
                        "key": key,
                        "summary": summary,
                        "status": status,
                        "updated": updated_date,
                        "type": issue_type,
                        "priority": priority,
                        "project": project,
                        "project_name": project_name,
                        "components": [],
                        "component_str": "Không có component",
                        "link": f"{jira_url}/browse/{key}",
                        "worklogs": [],
                        "total_hours": 0,
                        "has_worklog": False,
                        "parent_key": "",
                        "parent_summary": "",
                        "is_subtask": issue_type == "Sub-task",
                        "original_estimate_hours": original_estimate_hours,
                        "time_saved_hours": time_saved_hours,
                        "time_saved_percent": time_saved_percent,
                        "is_completed": is_completed,
                        "update_reasons": [],
                        "last_updater": {},
                        "last_update_time": "",
                        "main_update_reason": "Lỗi xử lý",
                        "update_category": "error",
                        "assignee_name": assignee_name,
                        "assignee_email": assignee_email,
                        "is_different_updater": assignee_name and last_updater_name and assignee_name != last_updater_name,
                        "actual_project": get_actual_project(project, [])
                    })
                    print(f"   ℹ️ Issue {key} đã được thêm với thông tin cơ bản mặc dù bị lỗi")
                except Exception as inner_e:
                    print(f"   ❌ Không thể thêm issue {issue.get('key', '')} do lỗi nghiêm trọng: {str(inner_e)}")
                continue
        
        return result
        
    except Exception as e:
        print(f"❌ Lỗi khi lấy tasks của nhân viên {employee_identifier}: {str(e)}")
        return []

def load_jira_config():
    """Tải thông tin cấu hình Jira từ file .env"""
    try:
        load_dotenv()
        
        jira_url = os.getenv("JIRA_URL")
        username = os.getenv("JIRA_USERNAME")
        password = os.getenv("JIRA_PASSWORD")
        
        if not all([jira_url, username, password]):
            print("❌ Thiếu thông tin cấu hình Jira. Vui lòng kiểm tra file .env")
            print("File .env cần có các biến: JIRA_URL, JIRA_USERNAME, JIRA_PASSWORD")
            return None, None, None
            
        return jira_url, username, password
        
    except Exception as e:
        print(f"❌ Lỗi khi đọc cấu hình Jira: {str(e)}")
        return None, None, None

def format_date(date_str):
    """Chuyển đổi định dạng ngày từ d/m/Y sang Y-m-d"""
    try:
        date_obj = datetime.strptime(date_str, "%d/%m/%Y")
        return date_obj.strftime("%Y-%m-%d")
    except Exception:
        print(f"❌ Lỗi định dạng ngày: {date_str}. Vui lòng sử dụng định dạng DD/MM/YYYY")
        return None

def format_time_duration(seconds):
    """Định dạng thời gian chờ theo giây thành chuỗi dễ đọc"""
    if seconds < 60:
        return f"{seconds} giây"
    elif seconds < 3600:
        minutes = seconds // 60
        secs = seconds % 60
        if secs == 0:
            return f"{minutes} phút"
        else:
            return f"{minutes} phút {secs} giây"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        if minutes == 0 and secs == 0:
            return f"{hours} giờ"
        elif secs == 0:
            return f"{hours} giờ {minutes} phút"
        else:
            return f"{hours} giờ {minutes} phút {secs} giây"
        


def main():
    print("=== LẤY DANH SÁCH TASK VÀ LOG WORK CỦA NHÂN VIÊN LC TỪ JIRA ===")
    
    # Tùy chọn hiển thị JQL
    show_jql_input = input("Bạn có muốn hiển thị JQL query không? (y/n, mặc định: n): ") or "n"
    show_jql = show_jql_input.lower() == "y"
    
    # Tải thông tin cấu hình Jira
    jira_url, username, password = load_jira_config()
    if not all([jira_url, username, password]):
        return
    
    # Đọc file Excel chứa danh sách nhân viên
    excel_file = input("Nhập đường dẫn đến file Excel chứa danh sách nhân viên LC (mặc định: resource/projects_employees.xlsx): ") or "resource/projects_employees.xlsx"
    
    # Kiểm tra file tồn tại
    if not os.path.exists(excel_file):
        print(f"❌ Không tìm thấy file: {excel_file}")
        return
    
    # Chọn sheet từ file Excel
    try:
        excel_info = pd.ExcelFile(excel_file)
        sheet_names = excel_info.sheet_names
        print(f"File Excel có {len(sheet_names)} sheet: {', '.join(sheet_names)}")
        
        # Nếu có nhiều sheet, cho phép người dùng chọn
        if len(sheet_names) > 1:
            sheet_input = input(f"Nhập tên sheet cần đọc (Enter để đọc sheet đầu tiên '{sheet_names[0]}'): ") or sheet_names[0]
            if sheet_input not in sheet_names:
                print(f"⚠️ Không tìm thấy sheet '{sheet_input}', sử dụng sheet đầu tiên '{sheet_names[0]}'")
                sheet_name = sheet_names[0]
            else:
                sheet_name = sheet_input
        else:
            sheet_name = sheet_names[0]
            print(f"Sử dụng sheet: {sheet_name}")
        
        # Đọc dữ liệu từ sheet
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        print(f"✅ Đã đọc thành công file Excel với {len(df)} bản ghi")
        
        # Kiểm tra và chuyển đổi tên cột nếu cần
        column_mapping = {}
        if 'EMAIL' not in df.columns:
            # Tìm cột có thể chứa email
            email_cols = [col for col in df.columns if 'EMAIL' in col.upper() or 'MAIL' in col.upper()]
            if email_cols:
                email_col = email_cols[0]
                column_mapping[email_col] = 'EMAIL'
                print(f"Đã tìm thấy cột email: {email_col}")
            else:
                print("❌ Không tìm thấy cột chứa địa chỉ email trong file Excel")
                return
        
        # Tìm cột họ tên
        if 'NAME' not in df.columns:
            name_cols = [col for col in df.columns if any(keyword in col.upper() for keyword in ['NAME', 'HỌ TÊN', 'HỌTÊN', 'FULLNAME'])]
            if name_cols:
                name_col = name_cols[0]
                column_mapping[name_col] = 'NAME'
                print(f"Đã tìm thấy cột tên: {name_col}")
        
        # Tìm cột SKILL_GROUP
        if 'SKILL_GROUP' not in df.columns:
            skill_cols = [col for col in df.columns if col.upper() == 'SKILL_GROUP' or 'SKILL' in col.upper()]
            if skill_cols:
                skill_col = skill_cols[0]
                column_mapping[skill_col] = 'SKILL_GROUP'
                print(f"Đã tìm thấy cột kỹ năng: {skill_col}")
        
        # Tìm cột PROJECTNAME
        if 'PROJECTNAME' not in df.columns:
            project_cols = [col for col in df.columns if 'PROJECT' in col.upper()]
            if project_cols:
                project_col = project_cols[0]
                column_mapping[project_col] = 'PROJECTNAME'
                print(f"Đã tìm thấy cột dự án: {project_col}")
        
        # Rename các cột
        if column_mapping:
            df = df.rename(columns=column_mapping)
        
        # Kiểm tra cột EMAIL
        if 'EMAIL' not in df.columns:
            print("❌ Thiếu cột EMAIL trong file Excel")
            return
    
    except Exception as e:
        print(f"❌ Lỗi khi đọc file Excel: {str(e)}")
        return
    
    # Nhập khoảng thời gian
    today = datetime.now().strftime("%d/%m/%Y")
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%Y")
    start_date_str = input(f"Nhập ngày bắt đầu (định dạng DD/MM/YYYY, mặc định: 11/06/2025): ") or "11/06/2025"
    end_date_str = input(f"Nhập ngày kết thúc (định dạng DD/MM/YYYY, mặc định: ngày mai - {tomorrow}): ") or tomorrow
    
    # Lọc theo trạng thái task
    excluded_statuses = ["ANALYZING", "APPROVED BY TMO", "PRODUCT BACKLOG", "CANCELLED", "10. Cancelled", "OPEN", "Pending", "1. Confirm", "IMPLEMENTING"]
    status_filter_input = input(f"Nhập các trạng thái cần lọc (phân cách bởi dấu phẩy, để trống để lấy tất cả trừ: {', '.join(excluded_statuses)}): ")

    if status_filter_input:
        status_filter = [status.strip() for status in status_filter_input.split(",")]
        exclude_default = False
        jira_status_exclude = None  # Không loại bỏ status nếu người dùng chỉ định rõ status cần lấy
    else:
        status_filter = []
        exclude_default = True
        jira_status_exclude = excluded_statuses
        print(f"⚠️ Mặc định sẽ loại bỏ các task có trạng thái: {', '.join(excluded_statuses)}")
    
    # Lọc theo dự án trong Jira API (lọc từ nguồn)
    jira_project_filter_input = input("Nhập các mã dự án trên Jira cần lọc (phân cách bởi dấu phẩy, để trống để sử dụng mặc định FC,FSS,PKT,WAK,PPFP): ") or "FC,FSS,PKT,WAK,PPFP"
    
    # Tùy chọn chỉ lấy cập nhật của người được gán task
    assignee_updates_only = input("Chỉ lấy cập nhật quan trọng của người được gán task? (y/n, mặc định: y): ").lower() != 'n'
    if assignee_updates_only:
        print("⚠️ Chỉ hiển thị cập nhật do người được gán task thực hiện")
    
    # Tùy chọn chỉ lấy cập nhật thay đổi trạng thái
    status_updates_only = input("Chỉ lấy cập nhật quan trọng liên quan đến thay đổi trạng thái? (y/n, mặc định: n): ").lower() == 'y'
    if status_updates_only:
        print("⚠️ Chỉ hiển thị cập nhật thay đổi trạng thái do chính người được gán task thực hiện")
        print("⚠️ Bỏ qua cập nhật status do PM/Lead thực hiện và tất cả loại cập nhật khác")
    else:
        print("⚠️ Hiển thị tất cả các loại cập nhật quan trọng (status, assignee, comment, time, v.v.)")
    
    if jira_project_filter_input:
        jira_project_filter = [project.strip().upper() for project in jira_project_filter_input.split(",") if project.strip()]
        if not jira_project_filter:
            print("⚠️ Không có dự án nào được chỉ định để lọc, sẽ sử dụng mặc định: FC,FSS,PKT,WAK,PPFP")
            jira_project_filter = ["FC", "FSS", "PKT", "WAK", "PPFP"]
        print(f"🔍 Lọc theo các mã dự án Jira API: {', '.join(jira_project_filter)}")
    
    # Thêm các dự án cần loại bỏ
    jira_project_exclude_input = input("Nhập các mã dự án trên Jira cần loại bỏ (phân cách bởi dấu phẩy, để trống để không loại bỏ. Mặc định: TADS): ") or "TADS"
    jira_project_exclude = [project.strip().upper() for project in jira_project_exclude_input.split(",") if project.strip()]
    if jira_project_exclude:
        print(f"🚫 Loại bỏ hoàn toàn các mã dự án Jira API: {', '.join(jira_project_exclude)}")
        print(f"⚠️ LƯU Ý: Tất cả task liên quan đến {', '.join(jira_project_exclude)} sẽ bị loại bỏ khỏi kết quả và thống kê!")
    
    # Loại bỏ phần lọc dự án thứ hai
    project_filter = []
    
    # Lọc theo loại issue
    type_filter_input = input("Nhập các loại issue cần lọc (phân cách bởi dấu phẩy, để trống để lấy tất cả): ")
    type_filter = [type.strip() for type in type_filter_input.split(",")] if type_filter_input else []
    
    # Tùy chọn trường thời gian để lọc
    time_field_options = ["updatedDate", "created", "resolutiondate"]
    time_field_input = input(f"Chọn trường thời gian để lọc ({', '.join(time_field_options)}, mặc định: updatedDate): ") or "updatedDate"
    time_field = time_field_input.strip()
    if time_field not in time_field_options:
        print(f"⚠️ Trường thời gian không hợp lệ, sử dụng mặc định: updatedDate")
        time_field = "updatedDate"
    
    # Tùy chọn tìm kiếm task do nhân viên báo cáo
    include_reported_input = input("Bạn có muốn tìm cả các task do nhân viên báo cáo không? (y/n, mặc định: n): ") or "n"
    include_reported = include_reported_input.lower() == "y"
    
    # Tùy chọn bỏ qua cập nhật chỉ liên quan đến Fix Version hoặc Sprint
    ignore_fix_version_sprint_input = input("Bạn có muốn bỏ qua các cập nhật chỉ liên quan đến Fix Version/Sprint/RemoteIssueLink/Components? (y/n, mặc định: y): ") or "y"
    ignore_fix_version_sprint = ignore_fix_version_sprint_input.lower() == "y"
    if ignore_fix_version_sprint:
        print("⚠️ Sẽ bỏ qua các cập nhật chỉ liên quan đến Fix Version/Sprint/RemoteIssueLink/Components và tìm cập nhật có ý nghĩa")
    else:
        print("⚠️ Sẽ tính tất cả các loại cập nhật (kể cả Fix Version/Sprint/RemoteIssueLink/Components)")
    
    # Tùy chọn lọc bỏ task cha khi task con không có update
    filter_parent_without_updated_children_input = input("Bạn có muốn loại bỏ task cha khi tất cả task con không có update? (y/n, mặc định: y): ") or "y"
    filter_parent_without_updated_children = filter_parent_without_updated_children_input.lower() == "y"
    if filter_parent_without_updated_children:
        print("✅ Sẽ loại bỏ task cha khi tất cả task con không có update")
    else:
        print("⚠️ Sẽ giữ lại tất cả task cha bất kể task con có update hay không")
    
    # Tùy chọn thời gian chờ giữa các request
    request_delay_input = input("Nhập thời gian trễ giữa các request (giây, mặc định: 0.1): ") or "0.1"
    try:
        request_delay = float(request_delay_input)
    except ValueError:
        print("⚠️ Giá trị không hợp lệ, sử dụng giá trị mặc định: 0.1 giây")
        request_delay = 0.1
    
    # Xác nhận các điều kiện lọc
    if status_filter:
        print(f"\n🔍 Chỉ lấy các task có trạng thái: {', '.join(status_filter)}")
    elif exclude_default:
        print(f"\n🔍 Lấy tất cả các trạng thái task ngoại trừ: {', '.join(excluded_statuses)}")
    else:
        print("\n🔍 Lấy tất cả các trạng thái task")
        
    if project_filter:
        print(f"🔍 Chỉ lấy các task thuộc dự án: {', '.join(project_filter)}")
    else:
        print("🔍 Lấy task của tất cả các dự án")
        
    if type_filter:
        print(f"🔍 Chỉ lấy các task có loại: {', '.join(type_filter)}")
    else:
        print("🔍 Lấy tất cả các loại task")
    
    # Chuyển đổi định dạng ngày
    start_date = format_date(start_date_str)
    end_date = format_date(end_date_str)
    
    if not all([start_date, end_date]):
        return
    
    print(f"\n🔍 Tìm kiếm task từ {start_date_str} đến {end_date_str}")
    print(f"🔍 Sử dụng trường thời gian: {time_field}")
    if include_reported:
        print("🔍 Tìm kiếm cả task do nhân viên báo cáo/tạo")
    else:
        print("🔍 Chỉ tìm kiếm task được gán cho nhân viên")
    print(f"⏱️ Thời gian chờ giữa các request API: {request_delay} giây")
    
    try:
        # Đọc file Excel
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        print(f"✅ Đã đọc thành công file Excel với {len(df)} bản ghi")
        
        # Kiểm tra cột EMAIL
        if 'EMAIL' not in df.columns:
            print("❌ Thiếu cột EMAIL trong file Excel")
            return
            
        # Lưu lại số lượng nhân viên ban đầu
        original_df = df.copy()
        original_count = len(df)
        
        # Kiểm tra xem có cột PROJECTNAME không và cho phép lọc
        if 'PROJECTNAME' in df.columns:
            # Hiển thị thống kê dự án
            project_counts = df['PROJECTNAME'].value_counts()
            print(f"\n📊 File Excel có {len(project_counts)} dự án khác nhau")
            print("Top 10 dự án có nhiều nhân viên nhất:")
            for idx, (project, count) in enumerate(project_counts.head(10).items(), 1):
                print(f"  {idx}. {project}: {count} nhân viên")
                
            # Kiểm tra các dự án có chứa FH
            fh_projects = [proj for proj in project_counts.index if isinstance(proj, str) and "FH" in proj.upper()]
            if fh_projects:
                print(f"\n🔍 Tìm thấy {len(fh_projects)} dự án liên quan đến FH:")
                for idx, proj in enumerate(fh_projects, 1):
                    print(f"  {idx}. {proj}: {project_counts[proj]} nhân viên")
            
            # Tùy chọn lọc theo dự án
            filter_project = input(f"Bạn có muốn lọc theo dự án cụ thể không? (y/n, mặc định: n): ") or "n"
            
            if filter_project.lower() == "y":
                project_filter = input("Nhập tên dự án cần lọc (phân cách bởi dấu phẩy nếu có nhiều dự án): ")
                specified_projects = [p.strip() for p in project_filter.split(',') if p.strip()]
                
                if specified_projects:
                    # Kiểm tra xem các dự án chỉ định có tồn tại không
                    existing_projects = []
                    not_found_projects = []
                    
                    for sp in specified_projects:
                        # Tìm kiếm chính xác (case sensitive)
                        exact_matches = [p for p in project_counts.index if isinstance(p, str) and p.strip() == sp.strip()]
                        
                        if exact_matches:
                            existing_projects.extend(exact_matches)
                        else:
                            # Nếu không tìm thấy, kiểm tra không phân biệt hoa thường
                            case_insensitive_matches = [p for p in project_counts.index if isinstance(p, str) and p.strip().upper() == sp.strip().upper()]
                            if case_insensitive_matches:
                                existing_projects.extend(case_insensitive_matches)
                            else:
                                not_found_projects.append(sp)
                    
                    if not_found_projects:
                        print(f"\n⚠️ Không tìm thấy {len(not_found_projects)} dự án:")
                        for i, p in enumerate(not_found_projects, 1):
                            print(f"  {i}. {p}")
                        
                        # Hỏi người dùng có muốn tìm một phần tên không
                        partial_search = input("Bạn có muốn tìm kiếm dự án chứa các tên trên không? (y/n, mặc định: n): ") or "n"
                        if partial_search.lower() == "y":
                            for sp in not_found_projects:
                                partial_matches = [p for p in project_counts.index if isinstance(p, str) and sp.strip().upper() in p.strip().upper()]
                                if partial_matches:
                                    print(f"\n🔍 Tìm thấy {len(partial_matches)} dự án chứa '{sp}':")
                                    for i, match in enumerate(partial_matches, 1):
                                        print(f"  {i}. {match}: {project_counts[match]} nhân viên")
                                    
                                    add_partial = input(f"Thêm các dự án này vào danh sách lọc? (y/n, mặc định: y): ") or "y"
                                    if add_partial.lower() == "y":
                                        existing_projects.extend(partial_matches)
                    
                    if existing_projects:
                        print(f"\n✅ Tìm thấy {len(existing_projects)} dự án phù hợp:")
                        for idx, proj in enumerate(existing_projects, 1):
                            print(f"  {idx}. {proj}: {project_counts[proj]} nhân viên")
                        
                        # Lọc nhân viên theo dự án
                        df_before_filter = df.copy()
                        df = df[df['PROJECTNAME'].isin(existing_projects)]
                        filtered_count = len(df_before_filter) - len(df)
                        
                        print(f"\n✅ Đã lọc được {len(df)} nhân viên thuộc {len(existing_projects)} dự án chỉ định")
                        print(f"   Đã loại bỏ {filtered_count} nhân viên không thuộc dự án chỉ định")
                    else:
                        print(f"\n⚠️ Không tìm thấy dự án nào phù hợp với yêu cầu")
        
        # Hiển thị thông tin về các email trùng lặp
        duplicated_emails = df[df.duplicated(subset=['EMAIL'], keep='first')]['EMAIL'].tolist()
        if duplicated_emails:
            print(f"\n⚠️ Phát hiện {len(duplicated_emails)} email trùng lặp:")
            for idx, email in enumerate(duplicated_emails, 1):
                duplicates = df[df['EMAIL'] == email]
                names = duplicates['NAME'].tolist() if 'NAME' in df.columns else ["Không có tên"] * len(duplicates)
                print(f"  {idx}. {email} - {len(duplicates)} lần xuất hiện - Tên: {', '.join(names)}")
            
        # Loại bỏ các email trùng lặp
        df_before_dedup = df.copy()
        df = df.drop_duplicates(subset=['EMAIL'])
        removed_by_duplication = len(df_before_dedup) - len(df)
        print(f"ℹ️ Đã loại bỏ {removed_by_duplication} bản ghi trùng lặp email, còn lại {len(df)} bản ghi")
        
        # Hiển thị danh sách bị loại do trùng lặp
        if removed_by_duplication > 0:
            print("\n📋 DANH SÁCH NHÂN VIÊN BỊ LOẠI BỎ DO TRÙNG LẶP EMAIL:")
            duplicate_df = df_before_dedup[df_before_dedup.duplicated(subset=['EMAIL'], keep='first')]
            for idx, row in duplicate_df.iterrows():
                name = row.get('NAME', 'Không có tên')
                email = row.get('EMAIL', '')
                skill_group = row.get('SKILL_GROUP', 'Không xác định')
                project_name = row.get('PROJECTNAME', 'Không xác định')
                print(f"  {idx+1}. {name} ({email}) - SKILL: {skill_group}, PROJECT: {project_name}")
        
        # Loại trừ một số SKILL_GROUP không mong muốn
        if 'SKILL_GROUP' in df.columns:
            # Danh sách mặc định các SKILL_GROUP không mong muốn
            default_excluded_skills = ['AMS', 'IT', 'EA', 'Databrick', 'AI', 'ISMS']
            
            # Cho phép người dùng tùy chỉnh danh sách
            custom_skills_input = input(f"Nhập các SKILL_GROUP cần loại bỏ (phân cách bởi dấu phẩy, Enter để sử dụng mặc định: {', '.join(default_excluded_skills)}): ")
            
            # Sử dụng danh sách tùy chỉnh hoặc mặc định
            if custom_skills_input.strip():
                excluded_skills = [skill.strip() for skill in custom_skills_input.split(',')]
                print(f"Sử dụng danh sách SKILL_GROUP do người dùng cung cấp: {', '.join(excluded_skills)}")
            else:
                excluded_skills = default_excluded_skills
                print(f"Sử dụng danh sách SKILL_GROUP mặc định: {', '.join(excluded_skills)}")
            
            before_skill_filter = len(df)
            
            # Lưu danh sách nhân viên bị loại bỏ do SKILL_GROUP
            excluded_employees_by_skill = df[df['SKILL_GROUP'].isin(excluded_skills)].copy()
            
            # Lọc bỏ nhân viên có SKILL_GROUP không mong muốn
            df = df[~df['SKILL_GROUP'].isin(excluded_skills)]
            after_skill_filter = len(df)
            removed_by_skill = before_skill_filter - after_skill_filter
            
            print(f"\nℹ️ Đã loại bỏ {removed_by_skill} nhân viên thuộc các SKILL_GROUP không mong muốn: {', '.join(excluded_skills)}")
            
            # Hiển thị danh sách bị loại do SKILL_GROUP
            if removed_by_skill > 0:
                print("\n📋 DANH SÁCH NHÂN VIÊN BỊ LOẠI BỎ DO THUỘC SKILL_GROUP KHÔNG MONG MUỐN:")
                skill_counts = excluded_employees_by_skill['SKILL_GROUP'].value_counts()
                print(f"Phân bố theo SKILL_GROUP:")
                for skill, count in skill_counts.items():
                    print(f"  - {skill}: {count} nhân viên")
                
                print("\nDanh sách chi tiết:")
                for idx, row in excluded_employees_by_skill.iterrows():
                    name = row.get('NAME', 'Không có tên')
                    email = row.get('EMAIL', '')
                    skill_group = row.get('SKILL_GROUP', 'Không xác định')
                    project_name = row.get('PROJECTNAME', 'Không xác định')
                    print(f"  {idx+1}. {name} ({email}) - SKILL: {skill_group}, PROJECT: {project_name}")
            
            print(f"ℹ️ Còn lại {len(df)} nhân viên sau khi lọc theo SKILL_GROUP")
        
        # Loại trừ các email cụ thể nếu người dùng yêu cầu
        # Danh sách email cần loại bỏ mặc định
        default_exclude_emails = [
            "ToanLBK@fpt.com",
            "LongNV61@fpt.com",
            "TrungTM7@fpt.com",
            "VietHQ3@fpt.com",
            "PhuocND6@fpt.com",
            "ThanhNX7@fpt.com",
            "MinhDH11@fpt.com",
            "SinhNV@fpt.com",
            "TuyetTT16@fpt.com"
        ]
        
        exclude_emails_input = input(f"Nhập các email cần loại bỏ khỏi việc kiểm tra task (phân cách bởi dấu phẩy, Enter để sử dụng danh sách mặc định): ")
        if exclude_emails_input.strip():
            exclude_emails = [email.strip().lower() for email in exclude_emails_input.split(',') if email.strip()]
        else:
            exclude_emails = [email.lower() for email in default_exclude_emails]
            print(f"Sử dụng danh sách email loại trừ mặc định: {', '.join(default_exclude_emails)}")
            
        # Kiểm tra xem các email có trong danh sách không
        emails_in_df = set(df['EMAIL'].str.lower())
        found_emails = [email for email in exclude_emails if email in emails_in_df]
        not_found_emails = [email for email in exclude_emails if email not in emails_in_df]
        
        if found_emails:
            print(f"\nℹ️ Tìm thấy {len(found_emails)} email trong danh sách cần loại bỏ:")
            for email in found_emails:
                print(f"  - {email}")
            
            # Lưu danh sách nhân viên bị loại bỏ
            before_email_filter = len(df)
            excluded_employees_by_email = df[df['EMAIL'].str.lower().isin(found_emails)].copy()
            
            # Lọc bỏ những email không mong muốn
            df = df[~df['EMAIL'].str.lower().isin(found_emails)]
            
            after_email_filter = len(df)
            removed_by_email = before_email_filter - after_email_filter
            print(f"\nℹ️ Đã loại bỏ {removed_by_email} nhân viên dựa theo email")
            
            # Hiển thị danh sách bị loại theo email
            if removed_by_email > 0:
                print("\n📋 DANH SÁCH NHÂN VIÊN BỊ LOẠI BỎ THEO EMAIL:")
                for idx, row in excluded_employees_by_email.iterrows():
                    name = row.get('NAME', 'Không có tên')
                    email = row.get('EMAIL', '')
                    skill_group = row.get('SKILL_GROUP', 'Không xác định') if 'SKILL_GROUP' in row else 'Không xác định'
                    project_name = row.get('PROJECTNAME', 'Không xác định') if 'PROJECTNAME' in row else 'Không xác định'
                    print(f"  {idx+1}. {name} ({email}) - SKILL: {skill_group}, PROJECT: {project_name}")
        
        if not_found_emails:
            print(f"\n⚠️ Không tìm thấy {len(not_found_emails)} email trong danh sách nhân viên:")
            for email in not_found_emails:
                print(f"  - {email}")
        
        print(f"ℹ️ Còn lại {len(df)} nhân viên sau khi lọc theo email")
        
        # Hiển thị tổng số nhân viên bị loại bỏ
        total_removed = original_count - len(df)
        if total_removed > 0:
            print(f"\n🔍 TỔNG KẾT VIỆC LỌC NHÂN VIÊN:")
            print(f"  - Số nhân viên ban đầu: {original_count}")
            print(f"  - Số nhân viên bị loại do trùng lặp email: {removed_by_duplication}")
            if 'SKILL_GROUP' in df.columns:
                print(f"  - Số nhân viên bị loại do SKILL_GROUP không mong muốn: {removed_by_skill}")
            
            # Hiển thị số nhân viên bị loại do email nếu có
            if exclude_emails_input.strip():
                print(f"  - Số nhân viên bị loại do nằm trong danh sách email loại trừ: {removed_by_email}")
                
            print(f"  - Tổng số nhân viên bị loại: {total_removed} ({total_removed/original_count*100:.1f}%)")
            print(f"  - Số nhân viên còn lại: {len(df)}")
            
        # Giới hạn chỉ 10 nhân viên đầu tiên
        max_employees = 150
        if len(df) > max_employees:
            df = df.head(max_employees)
            print(f"ℹ️ Chỉ xử lý {max_employees} nhân viên đầu tiên từ danh sách")
        else:
            print(f"ℹ️ Xử lý tất cả {len(df)} nhân viên trong danh sách")
        
        # Tạo thư mục kết quả
        result_dir = "data/tasks"
        os.makedirs(result_dir, exist_ok=True)
        
        # Tạo file kết quả tổng hợp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if status_filter:
            status_suffix = "_" + "_".join(status_filter).replace(" ", "")
        elif exclude_default:
            status_suffix = "_exclude_" + "_".join([s.replace(" ", "") for s in excluded_statuses])
        else:
            status_suffix = ""
        project_suffix = "_" + "_".join(project_filter).replace(" ", "") if project_filter else ""
        type_suffix = "_" + "_".join(type_filter).replace(" ", "") if type_filter else ""
        summary_file = f"{result_dir}/lc_tasks_worklog{status_suffix}{project_suffix}{type_suffix}_summary_{timestamp}.csv"
        report_file = f"{result_dir}/lc_tasks_worklog{status_suffix}{project_suffix}{type_suffix}_report_{timestamp}.txt"
        log_file = f"{result_dir}/lc_tasks_worklog{status_suffix}{project_suffix}{type_suffix}_log_{timestamp}.txt"
        worklog_file = f"{result_dir}/lc_tasks_worklog{status_suffix}{project_suffix}{type_suffix}_hours_{timestamp}.csv"
        
        # Ghi log thời gian bắt đầu
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write(f"=== LOG THỜI GIAN LẤY TASK VÀ WORKLOG TỪ JIRA ===\n\n")
            f.write(f"Thời gian bắt đầu: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
            f.write(f"Khoảng thời gian lấy task: {start_date_str} - {end_date_str}\n")
            f.write(f"Trường thời gian sử dụng: {time_field}\n")
            if include_reported:
                f.write(f"Tìm kiếm cả task do nhân viên báo cáo/tạo\n")
            else:
                f.write(f"Chỉ tìm kiếm task được gán cho nhân viên\n")
            if jira_project_filter:
                f.write(f"Lọc theo mã dự án Jira: {', '.join(jira_project_filter)}\n")
            if jira_project_exclude:
                f.write(f"Loại bỏ mã dự án Jira: {', '.join(jira_project_exclude)}\n")
            if jira_status_exclude:
                f.write(f"Loại bỏ các trạng thái: {', '.join(jira_status_exclude)}\n")
            
            # Ghi log danh sách email bị loại trừ
            if exclude_emails_input.strip() and found_emails:
                f.write(f"Loại bỏ {len(found_emails)} email: {', '.join(found_emails)}\n")
                
            f.write(f"Số nhân viên ban đầu: {len(pd.read_excel(excel_file, sheet_name=sheet_name))}\n")
            f.write(f"Số nhân viên sau khi loại bỏ trùng lặp: {len(pd.read_excel(excel_file, sheet_name=sheet_name).drop_duplicates(subset=['EMAIL']))}\n")
            if 'SKILL_GROUP' in df.columns:
                csv_df = pd.read_excel(excel_file, sheet_name=sheet_name)
                csv_df = csv_df.drop_duplicates(subset=['EMAIL'])
                filtered_df = csv_df[~csv_df['SKILL_GROUP'].isin(excluded_skills)]
                f.write(f"Số nhân viên sau khi lọc SKILL_GROUP: {len(filtered_df)}\n")
            
            # Ghi log số nhân viên sau khi lọc email
            if exclude_emails_input.strip() and found_emails:
                f.write(f"Số nhân viên sau khi lọc email: {len(df)}\n")
                
            f.write(f"Số nhân viên được xử lý: {min(len(df), max_employees)}\n")
            f.write(f"Thời gian chờ giữa các request API: {request_delay} giây\n\n")
        
        # Tổng số task của tất cả nhân viên
        all_tasks = []
        all_worklogs = []
        employee_task_counts = {}
        employee_worklog_hours = {}
        employee_detailed_stats = {}  # Dictionary mới để lưu thống kê chi tiết
        project_task_counts = {}
        project_name_task_counts = {}
        skill_group_task_counts = {}
        type_task_counts = {}
        status_task_counts = {}
        
        # Số nhân viên đã xử lý
        processed_count = 0
        
        # Lặp qua từng nhân viên
        for idx, row in df.iterrows():
            name = row.get('NAME', 'Không có tên')
            email = row.get('EMAIL', '')
            skill_group = row.get('SKILL_GROUP', '')
            project_name = row.get('PROJECTNAME', '')
            
            # Thời gian bắt đầu xử lý nhân viên này
            employee_start_time = datetime.now()
            
            # Ghi log
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(f"[{employee_start_time.strftime('%d/%m/%Y %H:%M:%S')}] Bắt đầu lấy task cho nhân viên: {name} ({email}) - SKILL: {skill_group}, PROJECT: {project_name}\n")
            
            if not email:
                print(f"⚠️ Nhân viên {name} không có email, bỏ qua")
                continue
                
            # Tăng số nhân viên đã xử lý
            processed_count += 1
                
            print(f"\n👤 ({processed_count}/{len(df)}) Đang lấy tasks và worklogs của {name} ({email}) - SKILL: {skill_group}, PROJECT: {project_name}...")
            
            # Lấy danh sách task
            tasks = get_employee_tasks(email, start_date, end_date, jira_url, username, password, 
                                      request_delay=request_delay, include_worklog=True, is_email=True, 
                                      include_reported=include_reported, show_jql=show_jql, 
                                      time_field=time_field, jira_project_filter=jira_project_filter,
                                      jira_project_exclude=jira_project_exclude,
                                      jira_status_exclude=jira_status_exclude,
                                      ignore_fix_version_sprint_updates=ignore_fix_version_sprint,
                                      assignee_updates_only=assignee_updates_only,
                                      status_updates_only=status_updates_only,
                                      skill_group=skill_group,
                                      filter_parent_without_updated_children=filter_parent_without_updated_children)
            
            # Cập nhật trạng thái logwork cho story dựa trên subtask
            tasks = update_story_worklog_from_subtasks(tasks)
            
            # Kiểm tra lại một lần nữa để loại bỏ task từ dự án đã loại trừ 
            if jira_project_exclude:
                tasks_before = len(tasks)
                tasks = [task for task in tasks if task.get('project', '').upper() not in [p.upper() for p in jira_project_exclude]]
                if len(tasks) < tasks_before:
                    print(f"   ⚠️ Phát hiện và loại bỏ thêm {tasks_before - len(tasks)} task từ dự án bị loại trừ ({', '.join(jira_project_exclude)})")
            
            # Kiểm tra lại một lần nữa để loại bỏ task có trạng thái đã loại trừ
            if jira_status_exclude:
                tasks_before = len(tasks)
                tasks = [task for task in tasks if task.get('status', '').upper() not in [s.upper() for s in jira_status_exclude]]
                if len(tasks) < tasks_before:
                    print(f"   ⚠️ Phát hiện và loại bỏ thêm {tasks_before - len(tasks)} task có trạng thái bị loại trừ ({', '.join(jira_status_exclude)})")
            
            # Thông báo về số lượng task tìm thấy ban đầu
            print(f"   ℹ️ Tìm thấy {len(tasks)} task trước khi lọc")
            
            # Lưu số task ban đầu để theo dõi quá trình lọc
            original_task_count = len(tasks)
            tasks_before_filter = tasks.copy()
            
            # Lọc task theo trạng thái nếu có yêu cầu
            if status_filter:
                task_count_before = len(tasks)
                tasks = [task for task in tasks if task.get('status', '').upper() in [s.upper() for s in status_filter]]
                filtered_count = task_count_before - len(tasks)
                print(f"   ℹ️ Lọc theo trạng thái đã chọn: {task_count_before} → {len(tasks)} task (loại bỏ {filtered_count} task)")
            # Nếu chúng ta loại bỏ status mặc định, luôn lọc lại một lần nữa để chắc chắn
            elif exclude_default:
                task_count_before = len(tasks)
                tasks = [task for task in tasks if task.get('status', '').upper() not in [s.upper() for s in excluded_statuses]]
                filtered_count = task_count_before - len(tasks)
                print(f"   ℹ️ Loại bỏ các trạng thái mặc định: {task_count_before} → {len(tasks)} task (loại bỏ {filtered_count} task)")

            # Lọc task theo loại nếu có yêu cầu
            if type_filter:
                task_count_before = len(tasks)
                tasks = [task for task in tasks if task.get('type', '') in type_filter]
                filtered_count = task_count_before - len(tasks)
                print(f"   ℹ️ Lọc theo loại: {task_count_before} → {len(tasks)} task (loại bỏ {filtered_count} task)")

            # Thông báo khi không còn task nào sau khi lọc
            if len(tasks) == 0 and original_task_count > 0:
                print(f"   ⚠️ Tất cả {original_task_count} task đã bị loại bỏ sau khi áp dụng các bộ lọc")
                
                # Hiện thông tin về task trước khi lọc để debug
                print(f"\n   📊 Thông tin về các task trước khi lọc:")
                for idx, task in enumerate(tasks_before_filter[:5], 1):  # Chỉ hiển thị 5 task đầu tiên
                    print(f"     {idx}. {task.get('key')} - Dự án: {task.get('project')} - Trạng thái: {task.get('status')} - Loại: {task.get('type')}")
                
                if len(tasks_before_filter) > 5:
                    print(f"     ... và {len(tasks_before_filter) - 5} task khác")
                
                # Hiển thị bảng thống kê dự án
                project_stats = {}
                for task in tasks_before_filter:
                    project = task.get('project', 'Không rõ')
                    if project not in project_stats:
                        project_stats[project] = 0
                    project_stats[project] += 1
                
                print(f"\n   📊 Phân bố dự án trước khi lọc:")
                for project, count in sorted(project_stats.items(), key=lambda x: x[1], reverse=True):
                    print(f"     - {project}: {count} task")
            
            # Thời gian kết thúc xử lý nhân viên này
            employee_end_time = datetime.now()
            processing_time = (employee_end_time - employee_start_time).total_seconds()
            
            # Tính tổng số giờ log work
            total_worklog_hours = sum(task.get("total_hours", 0) for task in tasks)
            employee_worklog_hours[name] = total_worklog_hours
            
            # Thống kê chi tiết theo yêu cầu
            total_tasks = len(tasks)
            tasks_without_logwork = sum(1 for task in tasks if task.get("time_saved_hours", 0) == -1)
            tasks_with_logwork = total_tasks - tasks_without_logwork
            tasks_with_logwork_no_saving = sum(1 for task in tasks if task.get("time_saved_hours", 0) == 0)
            tasks_with_saving = sum(1 for task in tasks if task.get("time_saved_hours", 0) > 0)
            tasks_exceeding_time = sum(1 for task in tasks if task.get("time_saved_hours", 0) < 0 and task.get("time_saved_hours", 0) != -1)
            
            print(f"   ✅ Tìm thấy {total_tasks} task, tổng {total_worklog_hours:.2f} giờ worklog (thời gian xử lý: {processing_time:.1f} giây)")
            print(f"   📊 Chi tiết: {tasks_without_logwork} chưa log work, {tasks_with_logwork} đã log work ({tasks_with_saving} tiết kiệm, {tasks_exceeding_time} vượt thời gian, {tasks_with_logwork_no_saving} đúng dự tính)")
            
            # Ghi log
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(f"[{employee_end_time.strftime('%d/%m/%Y %H:%M:%S')}] Hoàn thành, tìm thấy {len(tasks)} task, {total_worklog_hours:.2f} giờ worklog, thời gian xử lý: {processing_time:.1f} giây\n")
            
            # Cập nhật thống kê
            employee_task_counts[name] = len(tasks)
            
            # Thêm thống kê chi tiết về log work
            if name not in employee_detailed_stats:
                employee_detailed_stats[name] = {
                    "total_tasks": 0,
                    "tasks_without_logwork": 0,
                    "tasks_with_logwork": 0,
                    "tasks_with_logwork_no_saving": 0,
                    "tasks_with_saving": 0,
                    "tasks_exceeding_time": 0,
                    "total_hours": 0,
                    "total_saved_hours": 0
                }
            
            employee_detailed_stats[name]["total_tasks"] = total_tasks
            employee_detailed_stats[name]["tasks_without_logwork"] = tasks_without_logwork
            employee_detailed_stats[name]["tasks_with_logwork"] = tasks_with_logwork
            employee_detailed_stats[name]["tasks_with_logwork_no_saving"] = tasks_with_logwork_no_saving
            employee_detailed_stats[name]["tasks_with_saving"] = tasks_with_saving
            employee_detailed_stats[name]["tasks_exceeding_time"] = tasks_exceeding_time
            employee_detailed_stats[name]["total_hours"] = total_worklog_hours
            employee_detailed_stats[name]["total_saved_hours"] = sum(max(0, task.get("time_saved_hours", 0)) for task in tasks if task.get("time_saved_hours", 0) != -1)
            
            # Lưu tasks vào file cho nhân viên
            if tasks:
                # Tạo danh sách worklog
                for task in tasks:
                    for worklog in task.get('worklogs', []):
                        all_worklogs.append({
                            "employee_name": name,
                            "employee_email": email,
                            "issue_key": task.get("key"),
                            "issue_summary": task.get("summary"),
                            "issue_status": task.get("status"),
                            "project": task.get("project"),
                            "author": worklog.get("author"),
                            "time_spent": worklog.get("time_spent"),
                            "hours_spent": worklog.get("hours_spent"),
                            "started": worklog.get("started"),
                            "comment": worklog.get("comment")
                        })
                
                # Thêm thông tin nhân viên vào tasks
                for task in tasks:
                    task['employee_name'] = name
                    task['employee_email'] = email
                    task['skill_group'] = skill_group
                    task['project_name'] = project_name
                
                # Trước khi thêm vào all_tasks
                for task in tasks:
                    # Kiểm tra lại một lần nữa để đảm bảo không có task từ dự án bị loại trừ
                    if jira_project_exclude and task.get('project', '').upper() in [p.upper() for p in jira_project_exclude]:
                        continue
                    
                    # Kiểm tra lại một lần nữa để đảm bảo không có task có trạng thái bị loại trừ
                    if jira_status_exclude and task.get('status', '').upper() in [s.upper() for s in jira_status_exclude]:
                        continue
                    
                    # Thêm vào danh sách tất cả tasks
                    all_tasks.append(task)
                
                # Cập nhật thống kê theo dự án và trạng thái
                for task in tasks:
                    project = task.get('project', '')
                    status = task.get('status', '')
                    issue_type = task.get('type', '')
                    
                    # Cập nhật thống kê theo dự án
                    if project in project_task_counts:
                        project_task_counts[project] += 1
                    else:
                        project_task_counts[project] = 1
                    
                    # Cập nhật thống kê theo tên dự án
                    if project_name in project_name_task_counts:
                        project_name_task_counts[project_name] += 1
                    else:
                        project_name_task_counts[project_name] = 1
                    
                    # Cập nhật thống kê theo nhóm kỹ năng
                    if skill_group in skill_group_task_counts:
                        skill_group_task_counts[skill_group] += 1
                    else:
                        skill_group_task_counts[skill_group] = 1
                        
                    # Cập nhật thống kê theo trạng thái
                    if status in status_task_counts:
                        status_task_counts[status] += 1
                    else:
                        status_task_counts[status] = 1
                        
                    # Cập nhật thống kê theo loại issue
                    if issue_type in type_task_counts:
                        type_task_counts[issue_type] += 1
                    else:
                        type_task_counts[issue_type] = 1
                # Cập nhật trạng thái logwork cho task cha trước khi tạo báo cáo
                    parent_to_children = {}
                    for task in tasks:
                        if task.get('is_subtask') and task.get('parent_key'):
                            parent_key = task.get('parent_key')
                            if parent_key not in parent_to_children:
                                parent_to_children[parent_key] = []
                            parent_to_children[parent_key].append(task)

                    for task in tasks:
                        task_key = task.get('key')
                        if not task.get('is_subtask') and task_key in parent_to_children:
                            if not task.get('has_worklog'):  # Nếu task cha chưa có logwork
                                children_with_logwork = [child for child in parent_to_children[task_key] if child.get('has_worklog', False)]
                                if children_with_logwork:  # Nếu có ít nhất một task con có logwork
                                    # Đánh dấu task cha là có logwork
                                    task['has_worklog'] = True
                                    task['has_child_with_logwork'] = True  # Thêm trường để đánh dấu
                                    
                                    # Cập nhật time_saved_hours nếu đang là -1 (không có logwork)
                                    if task.get('time_saved_hours', -1) == -1:
                                        # Tính tổng thời gian thực tế từ các task con
                                        children_total_hours = sum(child.get('total_hours', 0) for child in children_with_logwork)
                                        
                                        # Cập nhật thời gian thực tế cho task cha
                                        if task.get('total_hours', 0) == 0:  # Chỉ cập nhật nếu task cha chưa có giá trị
                                            task['total_hours'] = children_total_hours
                                        
                                        # Nếu task cha có estimate, tính time_saved_hours
                                        if task.get('original_estimate_hours', 0) > 0:
                                            task['time_saved_hours'] = task.get('original_estimate_hours', 0) - task.get('total_hours', 0)
                                        else:
                                            # Nếu không có estimate, đặt thành 0 (không tiết kiệm)
                                            task['time_saved_hours'] = 0
                # Lưu tasks của nhân viên này vào file riêng
                employee_file = f"{result_dir}/{email.split('@')[0]}_{timestamp}.csv"
                
                # Tạo báo cáo chi tiết về task của nhân viên
                employee_report_file = f"{result_dir}/{email.split('@')[0]}_{timestamp}_report.txt"
                create_employee_detailed_report(name, email, tasks, employee_report_file)
                
                # Lọc các trường quan trọng để lưu vào CSV
                employee_tasks_simplified = []
                for task in tasks:
                    task_simplified = {
                        "key": task.get("key"),
                        "summary": task.get("summary"),
                        "status": task.get("status"),
                        "type": task.get("type"),
                        "project": task.get("project"),
                        "updated": task.get("updated"),
                        "total_hours": task.get("total_hours"),
                        "has_worklog": task.get("has_worklog"),
                        "component_str": task.get("component_str", "Không có component"),
                        "actual_project": task.get("actual_project", task.get("project"))
                    }
                    employee_tasks_simplified.append(task_simplified)
                
                employee_df = pd.DataFrame(employee_tasks_simplified)
                employee_df.to_csv(employee_file, index=False, encoding='utf-8')
                print(f"   📄 Đã lưu tasks vào file: {employee_file}")
            
            # Nếu không phải nhân viên cuối cùng, không cần chờ nữa
            if idx < len(df) - 1:
                # Ghi log sau khi xử lý xong
                with open(log_file, 'a', encoding='utf-8') as f:
                    f.write(f"[{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}] Tiếp tục xử lý nhân viên tiếp theo\n\n")
                    
                # Thông báo tiếp tục và thêm dấu phân cách
                print("\n" + "-" * 60)
                print("Tiếp tục xử lý nhân viên tiếp theo...")
                print("-" * 60 + "\n")
        
        # Lưu danh sách worklog
        if all_worklogs:
            worklog_df = pd.DataFrame(all_worklogs)
            worklog_df.to_csv(worklog_file, index=False, encoding='utf-8')
            print(f"\n📊 Đã tạo file tổng hợp worklog: {worklog_file}")
        
        # Tạo file tổng hợp các task
        if all_tasks:
            # QUAN TRỌNG: Cập nhật lại trạng thái worklog cho story dựa trên subtask
            # sau khi đã tổng hợp tất cả task từ các nhân viên
            print(f"\n🔄 Cập nhật toàn cục trạng thái logwork cho story từ subtask...")
            all_tasks = update_story_worklog_from_subtasks(all_tasks)
            
            # Lọc bỏ task cha khi task con không có update
            all_tasks = filter_parent_tasks_without_updated_children(all_tasks, filter_parent_without_updated_children)
            
            # Sắp xếp lại các task để nhóm các sub-task với task cha
            task_hierarchy = {}
            standalone_tasks = []
            
            # Phân loại task và sub-task
            for task in all_tasks:
                if task.get("is_subtask") and task.get("parent_key"):
                    parent_key = task.get("parent_key")
                    if parent_key not in task_hierarchy:
                        task_hierarchy[parent_key] = []
                    task_hierarchy[parent_key].append(task)
                elif not task.get("is_subtask"):
                    standalone_tasks.append(task)
            
            # Lọc các trường quan trọng để lưu vào CSV
            all_tasks_simplified = []
            
            # Trước tiên xử lý các task độc lập và các task cha
            for task in standalone_tasks:
                # Lấy thông tin người cập nhật cuối
                last_updater = task.get("last_updater", {})
                last_updater_name = last_updater.get("name", "") if last_updater else ""
                last_updater_email = last_updater.get("email", "") if last_updater else ""
                last_update_time = task.get("last_update_time", "")
                
                # Ghép các lý do cập nhật thành một chuỗi (giới hạn ở 3 lý do đầu tiên)
                update_reasons = task.get("update_reasons", [])
                update_reason_text = "; ".join(update_reasons[:3])
                if len(update_reasons) > 3:
                    update_reason_text += f"; ...và {len(update_reasons) - 3} thay đổi khác"
                
                task_simplified = {
                    "employee_name": task.get("employee_name"),
                    "employee_email": task.get("employee_email"),
                    "key": task.get("key"),
                    "summary": task.get("summary"),
                    "status": task.get("status"),
                    "type": task.get("type"),
                    "project": task.get("project"),
                    "updated": task.get("updated"),
                    "total_hours": task.get("total_hours"),
                    "has_worklog": task.get("has_worklog"),
                    "link": task.get("link"),
                    "hierarchy": "PARENT" if task.get("key") in task_hierarchy else "TASK",
                    "last_updater": last_updater_name,
                    "last_updater_email": last_updater_email,
                    "last_update_time": last_update_time,
                    "update_reason": update_reason_text,
                    "component_str": task.get("component_str", "Không có component")
                }
                all_tasks_simplified.append(task_simplified)
                
                # Thêm các sub-task nếu có
                subtasks = task_hierarchy.get(task.get("key"), [])
                for subtask in subtasks:
                    subtask_simplified = {
                        "employee_name": subtask.get("employee_name"),
                        "employee_email": subtask.get("employee_email"),
                        "key": subtask.get("key"),
                        "summary": f"└─ {subtask.get('summary')}",  # Thêm tiền tố cho sub-task
                        "status": subtask.get("status"),
                        "type": subtask.get("type"),
                        "project": subtask.get("project"),
                        "updated": subtask.get("updated"),
                        "total_hours": subtask.get("total_hours"),
                        "has_worklog": subtask.get("has_worklog"),
                        "link": subtask.get("link"),
                        "hierarchy": "SUBTASK",
                        "parent_key": subtask.get("parent_key"),
                        "component_str": subtask.get("component_str", "Không có component")
                    }
                    all_tasks_simplified.append(subtask_simplified)
            
            # Tìm các sub-task mà task cha không thuộc cùng nhân viên
            orphan_subtasks = []
            for task in all_tasks:
                if task.get("is_subtask") and task.get("parent_key"):
                    parent_key = task.get("parent_key")
                    # Kiểm tra xem task cha có trong danh sách standalone_tasks không
                    if not any(st.get("key") == parent_key for st in standalone_tasks):
                        orphan_subtasks.append(task)
            
            # Thêm các orphan sub-tasks vào list
            for subtask in orphan_subtasks:
                subtask_simplified = {
                    "employee_name": subtask.get("employee_name"),
                    "employee_email": subtask.get("employee_email"),
                    "key": subtask.get("key"),
                    "summary": f"└─ {subtask.get('summary')} (Orphan)",
                    "status": subtask.get("status"),
                    "type": subtask.get("type"),
                    "project": subtask.get("project"),
                    "updated": subtask.get("updated"),
                    "total_hours": subtask.get("total_hours"),
                    "has_worklog": subtask.get("has_worklog"),
                    "link": subtask.get("link"),
                    "hierarchy": "ORPHAN_SUBTASK",
                    "parent_key": subtask.get("parent_key"),
                    "component_str": subtask.get("component_str", "Không có component")
                }
                all_tasks_simplified.append(subtask_simplified)
            
            # Tạo báo cáo thống kê chi tiết theo dự án
            project_stats_file = f"{result_dir}/lc_tasks_worklog{status_suffix}{project_suffix}{type_suffix}_project_stats_{timestamp}.csv"
            
            # Tính toán thống kê theo dự án
            project_stats = {}
            for task in all_tasks:
                project = task.get("project", "")
                
                # Kiểm tra lại xem dự án có bị loại trừ không
                if jira_project_exclude and project.upper() in [p.upper() for p in jira_project_exclude]:
                    print(f"   ⚠️ Phát hiện task {task.get('key')} thuộc dự án bị loại trừ: {project}, bỏ qua khỏi thống kê")
                    continue
                    
                employee_name = task.get("employee_name", "")
                employee_email = task.get("employee_email", "")
                has_worklog = task.get("has_worklog", False)
                total_hours = task.get("total_hours", 0)
                original_estimate = task.get("original_estimate_hours", 0)
                time_saved = task.get("time_saved_hours", 0)
                
                if project not in project_stats:
                    project_stats[project] = {
                        "total_issues": 0,
                        "issues_with_worklog": 0,
                        "original_estimate_hours": 0,
                        "total_hours": 0,
                        "time_saved_hours": 0,
                        "employees": set(),
                        "employee_emails": set()
                    }
                
                project_stats[project]["total_issues"] += 1
                if has_worklog:
                    project_stats[project]["issues_with_worklog"] += 1
                
                project_stats[project]["original_estimate_hours"] += original_estimate
                project_stats[project]["total_hours"] += total_hours
                project_stats[project]["time_saved_hours"] += time_saved if task.get("is_completed", False) else 0
                
                if employee_name:
                    project_stats[project]["employees"].add(employee_name)
                if employee_email:
                    project_stats[project]["employee_emails"].add(employee_email)
            
            # Chuẩn bị dữ liệu dự án cho CSV
            project_stats_data = []
            for project, stats in project_stats.items():
                # Tính phần trăm tiết kiệm thời gian
                if stats["original_estimate_hours"] > 0:
                    saving_percentage = (stats["time_saved_hours"] / stats["original_estimate_hours"]) * 100
                else:
                    saving_percentage = 0
                
                # Tính phần trăm issue có log work
                if stats["total_issues"] > 0:
                    worklog_percentage = (stats["issues_with_worklog"] / stats["total_issues"]) * 100
                else:
                    worklog_percentage = 0
                
                project_stats_data.append({
                    "project": project,
                    "employee_count": len(stats["employees"]),
                    "total_issues": stats["total_issues"],
                    "issues_with_worklog": stats["issues_with_worklog"],
                    "worklog_percentage": round(worklog_percentage, 1),
                    "original_estimate_hours": round(stats["original_estimate_hours"], 2),
                    "total_hours": round(stats["total_hours"], 2),
                    "time_saved_hours": round(stats["time_saved_hours"], 2),
                    "saving_percentage": round(saving_percentage, 1)
                })
            
            # Sắp xếp theo thời gian tiết kiệm
            project_stats_data = sorted(project_stats_data, key=lambda x: x["time_saved_hours"], reverse=True)
            
            # Lưu vào CSV
            project_stats_df = pd.DataFrame(project_stats_data)
            project_stats_df.to_csv(project_stats_file, index=False, encoding='utf-8')
            print(f"📊 Đã tạo file thống kê theo dự án: {project_stats_file}")
            
            # Tính toán thống kê theo dự án thực tế (sử dụng hàm get_actual_project)
            actual_project_stats = {}
            for task in all_tasks:
                project = task.get("project", "")
                
                # Kiểm tra lại xem dự án có bị loại trừ không
                if jira_project_exclude and project.upper() in [p.upper() for p in jira_project_exclude]:
                    continue
                
                # Xác định dự án thực tế dựa vào project Jira và components 
                components = task.get("components", [])
                actual_project = get_actual_project(project, components)
                
                employee_name = task.get("employee_name", "")
                employee_email = task.get("employee_email", "")
                has_worklog = task.get("has_worklog", False)
                total_hours = task.get("total_hours", 0)
                original_estimate = task.get("original_estimate_hours", 0)
                time_saved = task.get("time_saved_hours", 0)
                
                if actual_project not in actual_project_stats:
                    actual_project_stats[actual_project] = {
                        "total_issues": 0,
                        "issues_with_worklog": 0,
                        "original_estimate_hours": 0,
                        "total_hours": 0,
                        "time_saved_hours": 0,
                        "employees": set(),
                        "employee_emails": set(),
                        "jira_projects": set()
                    }
                
                actual_project_stats[actual_project]["total_issues"] += 1
                if has_worklog:
                    actual_project_stats[actual_project]["issues_with_worklog"] += 1
                
                actual_project_stats[actual_project]["original_estimate_hours"] += original_estimate
                actual_project_stats[actual_project]["total_hours"] += total_hours
                actual_project_stats[actual_project]["time_saved_hours"] += time_saved if task.get("is_completed", False) else 0
                
                if employee_name:
                    actual_project_stats[actual_project]["employees"].add(employee_name)
                if employee_email:
                    actual_project_stats[actual_project]["employee_emails"].add(employee_email)
                
                # Thêm thông tin về project Jira gốc
                actual_project_stats[actual_project]["jira_projects"].add(project)
            
            # Chuẩn bị dữ liệu dự án thực tế cho CSV
            actual_project_stats_data = []
            for actual_project, stats in actual_project_stats.items():
                # Tính phần trăm tiết kiệm thời gian
                if stats["original_estimate_hours"] > 0:
                    saving_percentage = (stats["time_saved_hours"] / stats["original_estimate_hours"]) * 100
                else:
                    saving_percentage = 0
                
                # Tính phần trăm issue có log work
                if stats["total_issues"] > 0:
                    worklog_percentage = (stats["issues_with_worklog"] / stats["total_issues"]) * 100
                else:
                    worklog_percentage = 0
                
                actual_project_stats_data.append({
                    "actual_project": actual_project,
                    "jira_projects": ", ".join(stats["jira_projects"]),
                    "employee_count": len(stats["employees"]),
                    "total_issues": stats["total_issues"],
                    "issues_with_worklog": stats["issues_with_worklog"],
                    "worklog_percentage": round(worklog_percentage, 1),
                    "original_estimate_hours": round(stats["original_estimate_hours"], 2),
                    "total_hours": round(stats["total_hours"], 2),
                    "time_saved_hours": round(stats["time_saved_hours"], 2),
                    "saving_percentage": round(saving_percentage, 1)
                })
            
            # Sắp xếp theo thời gian tiết kiệm
            actual_project_stats_data = sorted(actual_project_stats_data, key=lambda x: x["time_saved_hours"], reverse=True)
            
            # Lưu vào CSV
            actual_project_stats_file = f"{result_dir}/lc_tasks_worklog{status_suffix}{project_suffix}{type_suffix}_actual_project_stats_{timestamp}.csv"
            actual_project_stats_df = pd.DataFrame(actual_project_stats_data)
            actual_project_stats_df.to_csv(actual_project_stats_file, index=False, encoding='utf-8')
            print(f"📊 Đã tạo file thống kê theo dự án thực tế: {actual_project_stats_file}")
            
            # Thống kê theo component
            component_stats = {}
            for task in all_tasks:
                # Kiểm tra lại xem dự án có bị loại trừ không
                project = task.get("project", "")
                if jira_project_exclude and project.upper() in [p.upper() for p in jira_project_exclude]:
                    continue
                
                # Lấy danh sách components của task
                components = task.get("components", [])
                
                # Nếu không có component, đặt vào nhóm "Không có component"
                if not components:
                    components = ["Không có component"]
                
                for component in components:
                    if component not in component_stats:
                        component_stats[component] = {
                            "total_issues": 0,
                            "issues_with_worklog": 0,
                            "original_estimate_hours": 0,
                            "total_hours": 0,
                            "time_saved_hours": 0,
                            "projects": set()
                        }
                    
                    has_worklog = task.get("has_worklog", False)
                    total_hours = task.get("total_hours", 0)
                    original_estimate = task.get("original_estimate_hours", 0)
                    time_saved = task.get("time_saved_hours", 0)
                    
                    component_stats[component]["total_issues"] += 1
                    if has_worklog:
                        component_stats[component]["issues_with_worklog"] += 1
                    
                    component_stats[component]["original_estimate_hours"] += original_estimate
                    component_stats[component]["total_hours"] += total_hours
                    component_stats[component]["time_saved_hours"] += time_saved if task.get("is_completed", False) else 0
                    component_stats[component]["projects"].add(project)
            
            # Chuẩn bị dữ liệu component cho CSV
            component_stats_data = []
            for component, stats in component_stats.items():
                # Tính phần trăm tiết kiệm thời gian
                if stats["original_estimate_hours"] > 0:
                    saving_percentage = (stats["time_saved_hours"] / stats["original_estimate_hours"]) * 100
                else:
                    saving_percentage = 0
                
                # Tính phần trăm issue có log work
                if stats["total_issues"] > 0:
                    worklog_percentage = (stats["issues_with_worklog"] / stats["total_issues"]) * 100
                else:
                    worklog_percentage = 0
                
                component_stats_data.append({
                    "component": component,
                    "project_count": len(stats["projects"]),
                    "projects": ", ".join(stats["projects"]),
                    "total_issues": stats["total_issues"],
                    "issues_with_worklog": stats["issues_with_worklog"],
                    "worklog_percentage": round(worklog_percentage, 1),
                    "original_estimate_hours": round(stats["original_estimate_hours"], 2),
                    "total_hours": round(stats["total_hours"], 2),
                    "time_saved_hours": round(stats["time_saved_hours"], 2),
                    "saving_percentage": round(saving_percentage, 1)
                })
            
            # Sắp xếp theo thời gian tiết kiệm
            component_stats_data = sorted(component_stats_data, key=lambda x: x["time_saved_hours"], reverse=True)
            
            # Lưu vào CSV
            component_stats_file = f"{result_dir}/lc_tasks_worklog{status_suffix}{project_suffix}{type_suffix}_component_stats_{timestamp}.csv"
            component_stats_df = pd.DataFrame(component_stats_data)
            component_stats_df.to_csv(component_stats_file, index=False, encoding='utf-8')
            print(f"📊 Đã tạo file thống kê theo component: {component_stats_file}")
            
            # Hiển thị bảng thống kê theo dự án
            # print("\n📊 THỐNG KÊ CHI TIẾT THEO DỰ ÁN:")
            # print(f"{'Dự án':<12}{'Nhân viên':<10}{'Issues':<8}{'Có log':<8}{'%Log':<7}{'Ước tính':<10}{'Thực tế':<10}{'Tiết kiệm':<10}{'%Tiết kiệm':<10}")
            # print("-" * 80)
            
            for stats in project_stats_data[:10]:  # Hiển thị top 10
                project = stats["project"]
                employee_count = stats["employee_count"]
                total_issues = stats["total_issues"]
                issues_with_worklog = stats["issues_with_worklog"]
                worklog_percentage = stats["worklog_percentage"]
                original_estimate = stats["original_estimate_hours"]
                total_hours = stats["total_hours"]
                time_saved = stats["time_saved_hours"]
                saving_percentage = stats["saving_percentage"]
                
                # Định dạng phần trăm
                worklog_percent_display = f"{worklog_percentage:.1f}%"
                
                # Định dạng thời gian tiết kiệm
                if time_saved > 0:
                    time_saved_display = f"{time_saved:.2f}h"
                    saving_percent_display = f"{saving_percentage:.1f}%"
                else:
                    time_saved_display = f"-{abs(time_saved):.2f}h"
                    saving_percent_display = f"-{abs(saving_percentage):.1f}%"
                
                print(f"{project:<12}{employee_count:<10}{total_issues:<8}{issues_with_worklog:<8}{worklog_percent_display:<7}{original_estimate:<10.2f}{total_hours:<10.2f}{time_saved_display:<10}{saving_percent_display:<10}")
            
            if len(project_stats_data) > 10:
                print(f"... và {len(project_stats_data) - 10} dự án khác (xem chi tiết trong file CSV)")
                
            # Hiển thị bảng thống kê theo dự án thực tế
            print("\n📊 THỐNG KÊ CHI TIẾT THEO DỰ ÁN THỰC TẾ:")
            print(f"{'Dự án thực tế':<20}{'Nhân viên':<10}{'Issues':<8}{'Có log':<8}{'%Log':<7}{'Ước tính':<10}{'Thực tế':<10}{'Tiết kiệm':<10}{'%Tiết kiệm':<10}")
            print("-" * 90)
            
            for stats in actual_project_stats_data[:10]:  # Hiển thị top 10
                project = stats["actual_project"][:18] + ".." if len(stats["actual_project"]) > 20 else stats["actual_project"]
                employee_count = stats["employee_count"]
                total_issues = stats["total_issues"]
                issues_with_worklog = stats["issues_with_worklog"]
                worklog_percentage = stats["worklog_percentage"]
                original_estimate = stats["original_estimate_hours"]
                total_hours = stats["total_hours"]
                time_saved = stats["time_saved_hours"]
                saving_percentage = stats["saving_percentage"]
                
                # Định dạng phần trăm
                worklog_percent_display = f"{worklog_percentage:.1f}%"
                
                # Định dạng thời gian tiết kiệm
                if time_saved > 0:
                    time_saved_display = f"{time_saved:.2f}h"
                    saving_percent_display = f"{saving_percentage:.1f}%"
                else:
                    time_saved_display = f"-{abs(time_saved):.2f}h"
                    saving_percent_display = f"-{abs(saving_percentage):.1f}%"
                
                print(f"{project:<20}{employee_count:<10}{total_issues:<8}{issues_with_worklog:<8}{worklog_percent_display:<7}{original_estimate:<10.2f}{total_hours:<10.2f}{time_saved_display:<10}{saving_percent_display:<10}")
            
            if len(actual_project_stats_data) > 10:
                print(f"... và {len(actual_project_stats_data) - 10} dự án thực tế khác (xem chi tiết trong file CSV)")
            
            # Hiển thị bảng thống kê theo component
            print("\n📊 THỐNG KÊ CHI TIẾT THEO COMPONENT:")
            print(f"{'Component':<22}{'Dự án':<10}{'Issues':<8}{'Có log':<8}{'%Log':<7}{'Ước tính':<10}{'Thực tế':<10}{'Tiết kiệm':<10}{'%Tiết kiệm':<10}")
            print("-" * 95)
            
            for stats in component_stats_data[:10]:  # Hiển thị top 10
                component = stats["component"][:20] + ".." if len(stats["component"]) > 22 else stats["component"]
                project_count = stats["project_count"]
                total_issues = stats["total_issues"]
                issues_with_worklog = stats["issues_with_worklog"]
                worklog_percentage = stats["worklog_percentage"]
                original_estimate = stats["original_estimate_hours"]
                total_hours = stats["total_hours"]
                time_saved = stats["time_saved_hours"]
                saving_percentage = stats["saving_percentage"]
                
                # Định dạng phần trăm
                worklog_percent_display = f"{worklog_percentage:.1f}%"
                
                # Định dạng thời gian tiết kiệm
                if time_saved > 0:
                    time_saved_display = f"{time_saved:.2f}h"
                    saving_percent_display = f"{saving_percentage:.1f}%"
                else:
                    time_saved_display = f"-{abs(time_saved):.2f}h"
                    saving_percent_display = f"-{abs(saving_percentage):.1f}%"
                
                print(f"{component:<22}{project_count:<10}{total_issues:<8}{issues_with_worklog:<8}{worklog_percent_display:<7}{original_estimate:<10.2f}{total_hours:<10.2f}{time_saved_display:<10}{saving_percent_display:<10}")
            
            if len(component_stats_data) > 10:
                print(f"... và {len(component_stats_data) - 10} component khác (xem chi tiết trong file CSV)")
                
            # Lưu báo cáo phân cấp vào file text để dễ đọc
            hierarchy_report_file = f"{result_dir}/lc_tasks_worklog{status_suffix}{project_suffix}{type_suffix}_hierarchy_{timestamp}.txt"
            with open(hierarchy_report_file, 'w', encoding='utf-8') as f:
                f.write("=== BÁO CÁO CÂY PHÂN CẤP TASK VÀ SUB-TASK ===\n\n")
                f.write(f"Thời gian tạo báo cáo: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
                f.write(f"Khoảng thời gian: {start_date_str} - {end_date_str}\n\n")
                
                # Tính tổng thời gian tiết kiệm
                total_original_estimate = sum(task.get("original_estimate_hours", 0) for task in all_tasks)
                total_time_spent = sum(task.get("total_hours", 0) for task in all_tasks)
                total_time_saved = sum(task.get("time_saved_hours", 0) for task in all_tasks if task.get("is_completed", False))
                
                if total_original_estimate > 0:
                    saving_percentage = (total_time_saved / total_original_estimate) * 100
                    f.write(f"Tổng thời gian ước tính (không AI): {total_original_estimate:.2f}h\n")
                    f.write(f"Tổng thời gian log work (với AI): {total_time_spent:.2f}h\n")
                    if total_time_saved > 0:
                        f.write(f"Tổng thời gian tiết kiệm: {total_time_saved:.2f}h ({saving_percentage:.1f}%)\n\n")
                    else:
                        f.write(f"Tổng thời gian chênh lệch: -{abs(total_time_saved):.2f}h\n\n")
                
                f.write("PHÂN CẤP TASK VÀ SUB-TASK:\n")
                f.write("="*100 + "\n")
                
                # Nhóm theo nhân viên
                employees = {}
                for task in standalone_tasks:
                    employee_name = task.get("employee_name", "Không xác định")
                    if employee_name not in employees:
                        employees[employee_name] = []
                    employees[employee_name].append(task)
                
                # Viết báo cáo theo từng nhân viên
                for employee_name, tasks in employees.items():
                    # Tính tổng thời gian cho nhân viên này
                    employee_tasks = [t for t in all_tasks if t.get("employee_name") == employee_name]
                    employee_estimate = sum(t.get("original_estimate_hours", 0) for t in employee_tasks)
                    employee_time_spent = sum(t.get("total_hours", 0) for t in employee_tasks)
                    employee_time_saved = sum(t.get("time_saved_hours", 0) for t in employee_tasks)
                    
                    # f.write(f"\n👤 NHÂN VIÊN: {employee_name}\n")
                    
                    if employee_estimate > 0:
                        saving_percentage = (employee_time_saved / employee_estimate) * 100 if employee_estimate > 0 else 0
                        if employee_time_saved > 0:
                            f.write(f"   Thời gian ước tính (không AI): {employee_estimate:.2f}h | Thời gian sử dụng AI: {employee_time_spent:.2f}h | Tiết kiệm: {employee_time_saved:.2f}h ({saving_percentage:.1f}%)\n")
                        else:
                            f.write(f"   Thời gian ước tính (không AI): {employee_estimate:.2f}h | Thời gian sử dụng AI: {employee_time_spent:.2f}h | Chênh lệch: -{abs(employee_time_saved):.2f}h\n")
                        
                    f.write("-"*100 + "\n")
                    
                    for task in tasks:
                        task_key = task.get("key", "")
                        has_subtasks = task_key in task_hierarchy
                        
                        # Lấy thông tin ước tính và thời gian
                        original_estimate = task.get("original_estimate_hours", 0)
                        time_spent = task.get("total_hours", 0)
                        time_saved = task.get("time_saved_hours", 0)
                        
                        # Hiển thị thông tin thời gian
                        time_info = ""
                        if original_estimate > 0:
                            saving_percent = (time_saved / original_estimate) * 100 if original_estimate > 0 else 0
                            if time_saved > 0:
                                time_info = f" | Ước tính: {original_estimate:.2f}h, Thực tế: {time_spent:.2f}h, Tiết kiệm: {time_saved:.2f}h ({saving_percent:.1f}%)"
                            else:
                                time_info = f" | Ước tính: {original_estimate:.2f}h, Thực tế: {time_spent:.2f}h, Chênh lệch: -{abs(time_saved):.2f}h"
                        elif time_spent > 0:
                            time_info = f" | Không có ước tính, Thực tế: {time_spent:.2f}h"
                        
                        task_icon = "📁" if has_subtasks else "📄"
                        f.write(f"{task_icon} {task_key}: {task.get('summary', '')} [{task.get('type', '')} - {task.get('status', '')}]{time_info}\n")
                        f.write(f"   🔖 Component: {task.get('component_str', 'Không có component')}\n")
                        f.write(f"   📌 Dự án thực tế: {task.get('actual_project', task.get('project', ''))}\n")
                        
                        # Hiển thị các sub-task
                        if has_subtasks:
                            subtasks = task_hierarchy.get(task_key, [])
                            for i, subtask in enumerate(subtasks):
                                is_last = i == len(subtasks) - 1
                                prefix = "└─" if is_last else "├─"
                                
                                # Lấy thông tin ước tính và thời gian cho sub-task
                                st_original_estimate = subtask.get("original_estimate_hours", 0)
                                st_time_spent = subtask.get("total_hours", 0)
                                st_time_saved = subtask.get("time_saved_hours", 0)
                                
                                # Hiển thị thông tin thời gian cho sub-task
                                st_time_info = ""
                                if st_original_estimate > 0:
                                    st_saving_percent = (st_time_saved / st_original_estimate) * 100 if st_original_estimate > 0 else 0
                                    if st_time_saved > 0:
                                        st_time_info = f" | Ước tính: {st_original_estimate:.2f}h, Thực tế: {st_time_spent:.2f}h, Tiết kiệm: {st_time_saved:.2f}h ({st_saving_percent:.1f}%)"
                                    else:
                                        st_time_info = f" | Ước tính: {st_original_estimate:.2f}h, Thực tế: {st_time_spent:.2f}h, Chênh lệch: -{abs(st_time_saved):.2f}h"
                                elif st_time_spent > 0:
                                    st_time_info = f" | Không có ước tính, Thực tế: {st_time_spent:.2f}h"
                                
                                f.write(f"    {prefix} {subtask.get('key', '')}: {subtask.get('summary', '')} [{subtask.get('type', '')} - {subtask.get('status', '')}]{st_time_info}\n")
                                f.write(f"        🔖 Component: {subtask.get('component_str', 'Không có component')}\n")
                                f.write(f"        📌 Dự án thực tế: {subtask.get('actual_project', subtask.get('project', ''))}\n")
                        
                        f.write("\n")
                    
                    # Hiển thị các sub-task mồ côi
                    if orphan_subtasks:
                        f.write("\n⚠️ SUB-TASKS CÓ TASK CHA KHÔNG THUỘC CÙNG NHÂN VIÊN:\n")
                        f.write("-"*100 + "\n")
                        
                        # Nhóm theo nhân viên
                        orphan_by_employee = {}
                        for subtask in orphan_subtasks:
                            employee_name = subtask.get("employee_name", "Không xác định")
                            if employee_name not in orphan_by_employee:
                                orphan_by_employee[employee_name] = []
                            orphan_by_employee[employee_name].append(subtask)
                        
                        for employee_name, subtasks in orphan_by_employee.items():
                            # f.write(f"\n👤 NHÂN VIÊN: {employee_name}\n")
                            
                            for subtask in subtasks:
                                parent_key = subtask.get("parent_key", "")
                                parent_summary = subtask.get("parent_summary", "")
                                
                                # Lấy thông tin ước tính và thời gian
                                st_original_estimate = subtask.get("original_estimate_hours", 0)
                                st_time_spent = subtask.get("total_hours", 0)
                                st_time_saved = subtask.get("time_saved_hours", 0)
                                
                                # Hiển thị thông tin thời gian
                                st_time_info = ""
                                if st_original_estimate > 0:
                                    st_saving_percent = (st_time_saved / st_original_estimate) * 100 if st_original_estimate > 0 else 0
                                    if st_time_saved > 0:
                                        st_time_info = f" | Ước tính: {st_original_estimate:.2f}h, Thực tế: {st_time_spent:.2f}h, Tiết kiệm: {st_time_saved:.2f}h ({st_saving_percent:.1f}%)"
                                    else:
                                        st_time_info = f" | Ước tính: {st_original_estimate:.2f}h, Thực tế: {st_time_spent:.2f}h, Chênh lệch: -{abs(st_time_saved):.2f}h"
                                elif st_time_spent > 0:
                                    st_time_info = f" | Không có ước tính, Thực tế: {st_time_spent:.2f}h"
                                
                                f.write(f"    └─ {subtask.get('key', '')}: {subtask.get('summary', '')} [{subtask.get('type', '')} - {subtask.get('status', '')}]{st_time_info}\n")
                                f.write(f"       ↑ Task cha: {parent_key} - {parent_summary}\n")
                                f.write(f"        🔖 Component: {subtask.get('component_str', 'Không có component')}\n")
                                f.write(f"        📌 Dự án thực tế: {subtask.get('actual_project', subtask.get('project', ''))}\n\n")
            
            # Ghi CSV file như bình thường
            all_tasks_df = pd.DataFrame(all_tasks_simplified)
            all_tasks_df.to_csv(summary_file, index=False, encoding='utf-8')
            print(f"\n📊 Đã tạo file tổng hợp tất cả task: {summary_file}")
            print(f"📊 Đã tạo báo cáo phân cấp task: {hierarchy_report_file}")
            
            # Tạo báo cáo thống kê
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write("=== BÁO CÁO THỐNG KÊ TASK VÀ WORKLOG CỦA NHÂN VIÊN LC ===\n\n")
                f.write(f"Thời gian tạo báo cáo: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
                f.write(f"Khoảng thời gian: {start_date_str} - {end_date_str}\n")
                f.write(f"Trường thời gian sử dụng: {time_field}\n")
                if include_reported:
                    f.write(f"Tìm kiếm cả task do nhân viên báo cáo/tạo\n")
                else:
                    f.write(f"Chỉ tìm kiếm task được gán cho nhân viên\n")
                if status_filter:
                    f.write(f"Lọc theo trạng thái: {', '.join(status_filter)}\n")
                elif exclude_default:
                    f.write(f"Loại bỏ các trạng thái mặc định: {', '.join(excluded_statuses)}\n")
                if project_filter:
                    f.write(f"Lọc theo dự án: {', '.join(project_filter)}\n")
                if type_filter:
                    f.write(f"Lọc theo loại issue: {', '.join(type_filter)}\n")
                if 'SKILL_GROUP' in df.columns:
                    excluded_skills = ['AMS', 'IT', 'EA', 'Databrick', 'AI', 'ISMS']
                    f.write(f"Loại bỏ nhân viên thuộc SKILL_GROUP: {', '.join(excluded_skills)}\n")
                f.write("\n")
                
                # Tính tổng thời gian tiết kiệm
                total_original_estimate = sum(task.get("original_estimate_hours", 0) for task in all_tasks)
                total_time_spent = sum(task.get("total_hours", 0) for task in all_tasks)
                total_time_saved = sum(task.get("time_saved_hours", 0) for task in all_tasks if task.get("is_completed", False))
                
                if total_original_estimate > 0:
                    saving_percentage = (total_time_saved / total_original_estimate) * 100
                else:
                    saving_percentage = 0
                
                f.write("THÔNG TIN TỔNG QUAN:\n")
                f.write(f"• Tổng số nhân viên đã xử lý: {len(df)}\n")
                f.write(f"• Tổng số nhân viên có task: {len([count for count in employee_task_counts.values() if count > 0])}\n")
                f.write(f"• Tổng số task: {len(all_tasks)}\n")
                f.write(f"• Tổng thời gian ước tính (không AI): {total_original_estimate:.2f} giờ\n")
                f.write(f"• Tổng thời gian log work (sử dụng AI): {total_time_spent:.2f} giờ\n")
                
                if total_time_saved > 0:
                    f.write(f"• Tổng thời gian tiết kiệm: {total_time_saved:.2f} giờ ({saving_percentage:.1f}%)\n\n")
                else:
                    f.write(f"• Tổng thời gian chênh lệch: -{abs(total_time_saved):.2f} giờ\n\n")
                
                f.write("DANH SÁCH NHÂN VIÊN ĐÃ XỬ LÝ:\n")
                for idx, row in df.iterrows():
                    name = row.get('NAME', 'Không có tên')
                    email = row.get('EMAIL', '')
                    task_count = employee_task_counts.get(name, 0)
                    worklog_hours = employee_worklog_hours.get(name, 0)
                    f.write(f"• {name} ({email}): {task_count} task, {worklog_hours:.2f} giờ log work\n")
                f.write("\n")
                
                f.write("THỐNG KÊ THEO NHÂN VIÊN:\n")
                f.write(f"{'STT':<5}{'Tên nhân viên':<30}{'Email':<30}{'Số task':<10}{'Task có worklog':<15}{'Giờ log work':<15}{'Ước tính không AI':<20}{'Tiết kiệm':<15}{'Phần trăm':<10}\n")
                f.write("="*145 + "\n")
                
                # Tính số task có worklog và ước tính cho mỗi nhân viên
                employee_worklog_tasks = {}
                employee_estimates = {}
                employee_time_saved = {}
                
                for task in all_tasks:
                    name = task.get("employee_name", "")
                    has_worklog = task.get("has_worklog", False)
                    original_estimate = task.get("original_estimate_hours", 0)
                    time_saved = task.get("time_saved_hours", 0)
                    
                    if name not in employee_worklog_tasks:
                        employee_worklog_tasks[name] = 0
                        employee_estimates[name] = 0
                        employee_time_saved[name] = 0
                    
                    if has_worklog:
                        employee_worklog_tasks[name] += 1
                    
                    employee_estimates[name] += original_estimate
                    employee_time_saved[name] += time_saved
                
                # Sắp xếp theo thời gian tiết kiệm từ cao đến thấp
                sorted_employees = sorted(employee_time_saved.items(), key=lambda x: x[1], reverse=True)
                
                for idx, (name, time_saved) in enumerate(sorted_employees, 1):
                    if name not in employee_task_counts or employee_task_counts[name] == 0:
                        continue
                        
                    email = next((row.get('EMAIL', '') for idx, row in df.iterrows() if row.get('NAME', '') == name), '')
                    task_count = employee_task_counts.get(name, 0)
                    worklog_task_count = employee_worklog_tasks.get(name, 0)
                    worklog_hours = employee_worklog_hours.get(name, 0)
                    estimate_hours = employee_estimates.get(name, 0)
                    
                    if estimate_hours > 0:
                        saving_percent = (time_saved / estimate_hours) * 100
                        saving_percent_display = f"{saving_percent:.1f}%"
                    else:
                        saving_percent_display = "N/A"
                    
                    if time_saved > 0:
                        time_saved_display = f"{time_saved:.2f}h"
                    else:
                        time_saved_display = f"-{abs(time_saved):.2f}h"
                        
                    f.write(f"{idx:<5}{name[:28]:<30}{email[:28]:<30}{task_count:<10}{worklog_task_count:<15}{worklog_hours:<15.2f}{estimate_hours:<20.2f}{time_saved_display:<15}{saving_percent_display:<10}\n")
                
                f.write("\n")
                
                f.write("THỐNG KÊ THEO DỰ ÁN:\n")
                f.write(f"{'STT':<5}{'Mã dự án':<15}{'Số task':<10}{'Ước tính không AI':<20}{'Thời gian sử dụng AI':<25}{'Tiết kiệm':<15}{'Phần trăm':<10}\n")
                f.write("-"*100 + "\n")
                
                # Tính thống kê theo dự án
                project_stats = {}
                for task in all_tasks:
                    project = task.get("project", "")
                    original_estimate = task.get("original_estimate_hours", 0)
                    time_spent = task.get("total_hours", 0)
                    time_saved = task.get("time_saved_hours", 0)
                    
                    if project not in project_stats:
                        project_stats[project] = {
                            "count": 0,
                            "estimate": 0,
                            "time_spent": 0,
                            "time_saved": 0
                        }
                    
                    project_stats[project]["count"] += 1
                    project_stats[project]["estimate"] += original_estimate
                    project_stats[project]["time_spent"] += time_spent
                    project_stats[project]["time_saved"] += time_saved
                
                # Sắp xếp theo thời gian tiết kiệm
                sorted_projects = sorted(project_stats.items(), key=lambda x: x[1]["time_saved"], reverse=True)
                
                for idx, (project, stats) in enumerate(sorted_projects, 1):
                    count = stats["count"]
                    estimate = stats["estimate"]
                    time_spent = stats["time_spent"]
                    time_saved = stats["time_saved"]
                    
                    if estimate > 0:
                        saving_percent = (time_saved / estimate) * 100
                        saving_percent_display = f"{saving_percent:.1f}%"
                    else:
                        saving_percent_display = "N/A"
                    
                    if time_saved > 0:
                        time_saved_display = f"{time_saved:.2f}h"
                    else:
                        time_saved_display = f"-{abs(time_saved):.2f}h"
                        
                    f.write(f"{idx:<5}{project:<15}{count:<10}{estimate:<20.2f}{time_spent:<25.2f}{time_saved_display:<15}{saving_percent_display:<10}\n")
                
                f.write("\n")
                
                # Các thống kê khác giữ nguyên
                f.write("THỐNG KÊ THEO TRẠNG THÁI:\n")
                for status, count in sorted(status_task_counts.items(), key=lambda x: x[1], reverse=True):
                    f.write(f"• {status}: {count} task ({count/len(all_tasks)*100:.1f}%)\n")
                f.write("\n")
                
                f.write("THỐNG KÊ THEO TÊN DỰ ÁN:\n")
                for project_name, count in sorted(project_name_task_counts.items(), key=lambda x: x[1], reverse=True):
                    if project_name:  # Chỉ hiển thị nếu có tên dự án
                        f.write(f"• {project_name}: {count} task ({count/len(all_tasks)*100:.1f}%)\n")
                    
                    f.write("THỐNG KÊ THEO NHÓM KỸ NĂNG:\n")
                    for skill, count in sorted(skill_group_task_counts.items(), key=lambda x: x[1], reverse=True):
                        if skill:  # Chỉ hiển thị nếu có nhóm kỹ năng
                            f.write(f"• {skill}: {count} task ({count/len(all_tasks)*100:.1f}%)\n")
                        
                        f.write("THỐNG KÊ THEO LOẠI ISSUE:\n")
                        for issue_type, count in sorted(type_task_counts.items(), key=lambda x: x[1], reverse=True):
                            f.write(f"• {issue_type}: {count} task ({count/len(all_tasks)*100:.1f}%)\n")
                
                print(f"📝 Đã tạo báo cáo thống kê: {report_file}")
                
                # Hiển thị thống kê tổng quan
                print("\n📊 THỐNG KÊ TỔNG QUAN:")
                print(f"• Tổng số nhân viên đã xử lý: {len(df)}")
                print(f"• Tổng số nhân viên có task: {len([count for count in employee_task_counts.values() if count > 0])}")
                print(f"• Tổng số task: {len(all_tasks)}")
                print(f"• Tổng số task đã hoàn thành: {len([task for task in all_tasks if task.get('is_completed', False)])}")
                print(f"• Tổng thời gian ước tính (không AI): {total_original_estimate:.2f} giờ")
                print(f"• Tổng thời gian log work (sử dụng AI): {total_time_spent:.2f} giờ")
                print(f"• Tổng thời gian tiết kiệm (chỉ tính task hoàn thành): {total_time_saved:.2f} giờ ({saving_percentage:.1f}%)")
                
                # Thu thập số liệu thống kê tổng hợp
                total_tasks = len(all_tasks)
                total_worklog_entries = sum(len(task.get('worklogs', [])) for task in all_tasks)
                tasks_with_worklog_count = sum(1 for task in all_tasks if task.get('worklogs', []))
                
                # Tổng hợp thống kê toàn dự án
                print(f"\n\n📊 THỐNG KÊ TỔNG HỢP TOÀN DỰ ÁN:")
                print(f"  - Tổng số nhân viên: {len(df)}")
                print(f"  - Tổng số task: {total_tasks}")
                print(f"  - Tổng số task có worklog: {tasks_with_worklog_count} ({tasks_with_worklog_count/total_tasks*100:.1f}% nếu có task)")
                print(f"  - Tổng số bản ghi worklog: {total_worklog_entries}")
                print(f"  - Tổng số giờ worklog: {sum(employee_worklog_hours.values()):.2f} giờ")

                # Thống kê chi tiết theo dự án với định dạng bảng
                if all_tasks:
                    # print("\n📊 THỐNG KÊ CHI TIẾT THEO DỰ ÁN:")
                    
                    # Thu thập dữ liệu theo dự án
                    project_stats = {}
                    for task in all_tasks:
                        project = task.get('project', 'Unknown')
                        employee = task.get('employee_name', 'Unknown')
                        has_worklog = bool(task.get('worklogs', []))
                        total_hours = task.get('total_hours', 0)
                        estimated_hours = task.get('original_estimate_hours', 0) or 0
                        
                        if project not in project_stats:
                            project_stats[project] = {
                                'employees': set(),
                                'issues': 0,
                                'issues_with_log': 0,
                                'estimated_hours': 0,
                                'actual_hours': 0,
                            }
                        
                        project_stats[project]['employees'].add(employee)
                        project_stats[project]['issues'] += 1
                        if has_worklog:
                            project_stats[project]['issues_with_log'] += 1
                        project_stats[project]['estimated_hours'] += estimated_hours
                        project_stats[project]['actual_hours'] += total_hours
                    
                    # Tính toán các giá trị phái sinh
                    for project, stats in project_stats.items():
                        stats['log_percentage'] = (stats['issues_with_log'] / stats['issues'] * 100) if stats['issues'] > 0 else 0
                        stats['time_saved'] = stats['estimated_hours'] - stats['actual_hours'] if stats['estimated_hours'] > 0 else 0
                        stats['saving_percentage'] = (stats['time_saved'] / stats['estimated_hours'] * 100) if stats['estimated_hours'] > 0 else 0
                    
                    # In tiêu đề bảng
                    header = "| {:<30} | {:>8} | {:>8} | {:>8} | {:>6} | {:>8} | {:>8} | {:>10} | {:>10} |".format(
                        "Dự án", "Nhân viên", "Issues", "Có log", "%Log", "Ước tính", "Thực tế", "Tiết kiệm", "%Tiết kiệm"
                    )
                    separator = "|-{:-<30}-|-{:->8}-|-{:->8}-|-{:->8}-|-{:->6}-|-{:->8}-|-{:->8}-|-{:->10}-|-{:->10}-|".format(
                        "", "", "", "", "", "", "", "", ""
                    )
                    
                    # print(separator)
                    # print(header)
                    # print(separator)
                    
                    # In dữ liệu từng dự án
                    sorted_projects = sorted(project_stats.items(), key=lambda x: len(x[1]['employees']), reverse=True)
                    for project, stats in sorted_projects:
                        row = "| {:<30} | {:>8} | {:>8} | {:>8} | {:>6.1f} | {:>8.1f} | {:>8.1f} | {:>10.1f} | {:>10.1f} |".format(
                            project[:30],
                            len(stats['employees']),
                            stats['issues'],
                            stats['issues_with_log'],
                            stats['log_percentage'],
                            stats['estimated_hours'],
                            stats['actual_hours'],
                            stats['time_saved'],
                            stats['saving_percentage']
                        )
                        # print(row)
                    
                    print(separator)
                    
                    # In tổng cộng
                    total_employees = len(set().union(*[stats['employees'] for stats in project_stats.values()]))
                    total_issues = sum(stats['issues'] for stats in project_stats.values())
                    total_issues_with_log = sum(stats['issues_with_log'] for stats in project_stats.values())
                    total_log_percentage = (total_issues_with_log / total_issues * 100) if total_issues > 0 else 0
                    total_estimated = sum(stats['estimated_hours'] for stats in project_stats.values())
                    total_actual = sum(stats['actual_hours'] for stats in project_stats.values())
                    total_saved = total_estimated - total_actual
                    total_saving_percentage = (total_saved / total_estimated * 100) if total_estimated > 0 else 0
                    
                    total_row = "| {:<30} | {:>8} | {:>8} | {:>8} | {:>6.1f} | {:>8.1f} | {:>8.1f} | {:>10.1f} | {:>10.1f} |".format(
                        "TỔNG CỘNG",
                        total_employees,
                        total_issues,
                        total_issues_with_log,
                        total_log_percentage,
                        total_estimated,
                        total_actual,
                        total_saved,
                        total_saving_percentage
                    )
                    # print(total_row)
                    print(separator)
                    
                    # THÊM THỐNG KÊ TỔNG HỢP THEO NHÂN VIÊN
                    # print("\n\n📊 THỐNG KÊ TỔNG HỢP THEO NHÂN VIÊN:")
                    
                    # Thu thập dữ liệu thống kê nhân viên
                    employee_summary = {}
                    
                    for task in all_tasks:
                        employee = task.get('employee_name', 'Unknown')
                        email = task.get('employee_email', '')
                        has_worklog = bool(task.get('worklogs', []))
                        total_hours = task.get('total_hours', 0)
                        estimated_hours = task.get('original_estimate_hours', 0) or 0
                        
                        if employee not in employee_summary:
                            employee_summary[employee] = {
                                'email': email,
                                'issues': 0,
                                'issues_with_log': 0,
                                'estimated_hours': 0,
                                'actual_hours': 0,
                                'projects': set()
                            }
                        
                        employee_summary[employee]['issues'] += 1
                        if has_worklog:
                            employee_summary[employee]['issues_with_log'] += 1
                        employee_summary[employee]['estimated_hours'] += estimated_hours
                        employee_summary[employee]['actual_hours'] += total_hours
                        employee_summary[employee]['projects'].add(task.get('project', 'Unknown'))
                    
                    # Tính toán các giá trị phái sinh
                    for employee, stats in employee_summary.items():
                        stats['log_percentage'] = (stats['issues_with_log'] / stats['issues'] * 100) if stats['issues'] > 0 else 0
                        stats['time_saved'] = stats['estimated_hours'] - stats['actual_hours'] if stats['estimated_hours'] > 0 else 0
                        stats['saving_percentage'] = (stats['time_saved'] / stats['estimated_hours'] * 100) if stats['estimated_hours'] > 0 else 0
                    
                    # In bảng thống kê tổng hợp nhân viên
                    # header = "| {:<25} | {:<30} | {:>6} | {:>8} | {:>6} | {:>8} | {:>8} | {:>10} | {:>10} | {:>6} |".format(
                    #     "Nhân viên", "Email", "Dự án", "Issues", "%Log", "Ước tính", "Thực tế", "Tiết kiệm", "%Tiết kiệm", "Hiệu suất"
                    # )
                    # separator = "|-{:-<25}-|-{:-<30}-|-{:->6}-|-{:->8}-|-{:->6}-|-{:->8}-|-{:->8}-|-{:->10}-|-{:->10}-|-{:->6}-|".format(
                    #     "", "", "", "", "", "", "", "", "", ""
                    # )
                    
                    # print(separator)
                    # print(header)
                    # print(separator)
                    
                    # Sắp xếp nhân viên theo số lượng issue từ cao đến thấp
                    sorted_employees = sorted(employee_summary.items(), key=lambda x: x[1]['issues'], reverse=True)
                    
                    for employee, stats in sorted_employees:
                        # Tính điểm hiệu suất: dựa trên tỷ lệ tiết kiệm thời gian và tỷ lệ task có log
                        performance = 0
                        if stats['estimated_hours'] > 0:
                            # Điểm thưởng khi tiết kiệm được thời gian
                            saving_factor = stats['saving_percentage'] / 100 if stats['saving_percentage'] > 0 else 0
                            # Điểm thưởng khi có tỷ lệ log cao
                            log_factor = stats['log_percentage'] / 100
                            
                            # Hiệu suất từ 0-100
                            performance = min(100, (saving_factor * 0.7 + log_factor * 0.3) * 100)
                        
                        # row = "| {:<25} | {:<30} | {:>6} | {:>8} | {:>6.1f} | {:>8.1f} | {:>8.1f} | {:>10.1f} | {:>10.1f} | {:>6.1f} |".format(
                        #     employee[:25],
                        #     stats['email'][:30],
                        #     len(stats['projects']),
                        #     stats['issues'],
                        #     stats['log_percentage'],
                        #     stats['estimated_hours'],
                        #     stats['actual_hours'],
                        #     stats['time_saved'],
                        #     stats['saving_percentage'],
                        #     performance
                        # )
                        # print(row)
                    
                    print(separator)
                    
                    # Tính tổng cộng
                    total_employees = len(employee_summary)
                    total_projects = len(set().union(*[stats['projects'] for stats in employee_summary.values()]))
                    total_issues = sum(stats['issues'] for stats in employee_summary.values())
                    total_issues_with_log = sum(stats['issues_with_log'] for stats in employee_summary.values())
                    total_log_percentage = (total_issues_with_log / total_issues * 100) if total_issues > 0 else 0
                    total_estimated = sum(stats['estimated_hours'] for stats in employee_summary.values())
                    total_actual = sum(stats['actual_hours'] for stats in employee_summary.values())
                    total_saved = total_estimated - total_actual
                    total_saving_percentage = (total_saved / total_estimated * 100) if total_estimated > 0 else 0
                    
                    # Tính hiệu suất trung bình
                    avg_performance = sum(
                        min(100, ((stats['saving_percentage'] / 100 if stats['saving_percentage'] > 0 else 0) * 0.7 + 
                        (stats['log_percentage'] / 100) * 0.3) * 100) 
                        for stats in employee_summary.values() if stats['estimated_hours'] > 0
                    ) / len([stats for stats in employee_summary.values() if stats['estimated_hours'] > 0]) if employee_summary else 0
                    
                    # total_row = "| {:<25} | {:<30} | {:>6} | {:>8} | {:>6.1f} | {:>8.1f} | {:>8.1f} | {:>10.1f} | {:>10.1f} | {:>6.1f} |".format(
                    #     f"TỔNG CỘNG ({total_employees})",
                    #     "",
                    #     total_projects,
                    #     total_issues,
                    #     total_log_percentage,
                    #     total_estimated,
                    #     total_actual,
                    #     total_saved,
                    #     total_saving_percentage,
                    #     avg_performance
                    # )
                    # print(total_row)
                    # print(separator)
                    
                    # Thống kê dự án theo từng nhân viên
                    # print("\n\n📊 THỐNG KÊ DỰ ÁN THEO TỪNG NHÂN VIÊN:")
                    
                    # Tổ chức dữ liệu theo nhân viên và dự án
                    employee_project_stats = {}
                    
                    for task in all_tasks:
                        employee = task.get('employee_name', 'Unknown')
                        email = task.get('employee_email', '')
                        project = task.get('project', 'Unknown')
                        has_worklog = bool(task.get('worklogs', []))
                        total_hours = task.get('total_hours', 0)
                        estimated_hours = task.get('original_estimate_hours', 0) or 0
                        
                        # Tạo key nhân viên nếu chưa có
                        if employee not in employee_project_stats:
                            employee_project_stats[employee] = {
                                'email': email,
                                'projects': {}
                            }
                        
                        # Tạo key dự án cho nhân viên nếu chưa có
                        if project not in employee_project_stats[employee]['projects']:
                            employee_project_stats[employee]['projects'][project] = {
                                'issues': 0,
                                'issues_with_log': 0,
                                'estimated_hours': 0,
                                'actual_hours': 0,
                            }
                        
                        # Cập nhật thống kê
                        employee_project_stats[employee]['projects'][project]['issues'] += 1
                        if has_worklog:
                            employee_project_stats[employee]['projects'][project]['issues_with_log'] += 1
                        employee_project_stats[employee]['projects'][project]['estimated_hours'] += estimated_hours
                        employee_project_stats[employee]['projects'][project]['actual_hours'] += total_hours
                    
                    # Tính toán các giá trị phái sinh cho từng dự án của nhân viên
                    for employee, data in employee_project_stats.items():
                        for project, stats in data['projects'].items():
                            stats['log_percentage'] = (stats['issues_with_log'] / stats['issues'] * 100) if stats['issues'] > 0 else 0
                            stats['time_saved'] = stats['estimated_hours'] - stats['actual_hours'] if stats['estimated_hours'] > 0 else 0
                            stats['saving_percentage'] = (stats['time_saved'] / stats['estimated_hours'] * 100) if stats['estimated_hours'] > 0 else 0
                    
                    # In thống kê cho từng nhân viên
                    sorted_employees = sorted(employee_project_stats.items(), key=lambda x: x[0])
                    
                    for employee, data in sorted_employees:
                        email = data['email']
                        # print(f"\n👤 NHÂN VIÊN: {employee} ({email})")
                        
                        # In tiêu đề bảng
                        # header = "| {:<30} | {:>8} | {:>8} | {:>6} | {:>8} | {:>8} | {:>10} | {:>10} |".format(
                        #     "Dự án", "Issues", "Có log", "%Log", "Ước tính", "Thực tế", "Tiết kiệm", "%Tiết kiệm"
                        # )
                        # separator = "|-{:-<30}-|-{:->8}-|-{:->8}-|-{:->6}-|-{:->8}-|-{:->8}-|-{:->10}-|-{:->10}-|".format(
                        #     "", "", "", "", "", "", "", ""
                        # )
                        
                        # print(separator)
                        # print(header)
                        # print(separator)
                        
                        # In dữ liệu từng dự án của nhân viên
                        sorted_projects = sorted(data['projects'].items(), key=lambda x: x[1]['issues'], reverse=True)
                        
                        for project, stats in sorted_projects:
                            # row = "| {:<30} | {:>8} | {:>8} | {:>6.1f} | {:>8.1f} | {:>8.1f} | {:>10.1f} | {:>10.1f} |".format(
                            #     project[:30],
                            #     stats['issues'],
                            #     stats['issues_with_log'],
                            #     stats['log_percentage'],
                            #     stats['estimated_hours'],
                            #     stats['actual_hours'],
                            #     stats['time_saved'],
                            #     stats['saving_percentage']
                            # )
                            # print(row)
                            pass
                        
                        # print(separator)
                        
                        # In tổng cộng cho nhân viên
                        total_issues = sum(stats['issues'] for stats in data['projects'].values())
                        total_issues_with_log = sum(stats['issues_with_log'] for stats in data['projects'].values())
                        total_log_percentage = (total_issues_with_log / total_issues * 100) if total_issues > 0 else 0
                        total_estimated = sum(stats['estimated_hours'] for stats in data['projects'].values())
                        total_actual = sum(stats['actual_hours'] for stats in data['projects'].values())
                        total_saved = total_estimated - total_actual
                        total_saving_percentage = (total_saved / total_estimated * 100) if total_estimated > 0 else 0
                        
                        # total_row = "| {:<30} | {:>8} | {:>8} | {:>6.1f} | {:>8.1f} | {:>8.1f} | {:>10.1f} | {:>10.1f} |".format(
                        #     "TỔNG CỘNG",
                        #     total_issues,
                        #     total_issues_with_log,
                        #     total_log_percentage,
                        #     total_estimated,
                        #     total_actual,
                        #     total_saved,
                        #     total_saving_percentage
                        # )
                        # print(total_row)
                        # print(separator)
                else:
                    print("\n⚠️ Không tìm thấy task nào trong khoảng thời gian này")
        
        # Ghi log thời gian kết thúc
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"\n[{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}] Hoàn thành toàn bộ quá trình\n")
            f.write(f"Tổng số nhân viên đã xử lý: {len(df)}\n")
            f.write(f"Tổng số nhân viên có task: {len([count for count in employee_task_counts.values() if count > 0])}\n")
            f.write(f"Tổng số task: {len(all_tasks)}\n")
            f.write(f"Tổng số giờ log work: {sum(employee_worklog_hours.values()):.2f} giờ\n")
            
        print(f"\n📋 Đã ghi log quá trình xử lý: {log_file}")
        print(f"\n✅ Hoàn thành quá trình xử lý!")
        
        # Thống kê top 5 nhân viên có nhiều task nhưng không logwork
        # print("\n\n📊 TOP 10 NHÂN VIÊN CÓ NHIỀU TASK KHÔNG LOGWORK:")
        
        # Tính tỷ lệ task không logwork
        for name, stats in employee_detailed_stats.items():
            if stats["total_tasks"] > 0:
                stats["no_logwork_ratio"] = stats["tasks_without_logwork"] / stats["total_tasks"] * 100
            else:
                stats["no_logwork_ratio"] = 0
        
        # Sắp xếp theo số lượng task không logwork
        sorted_by_no_logwork = sorted(
            [item for item in employee_detailed_stats.items() if item[1]["total_tasks"] >= 3],  # Chỉ xét nhân viên có ít nhất 3 task
            key=lambda x: x[1]["tasks_without_logwork"], 
            reverse=True
        )
        
        # In tiêu đề
        print(f"{'Tên nhân viên':<30}{'Tổng task':<15}{'Không logwork':<15}{'Tỷ lệ không logwork':<20}")
        print("-" * 80)
        
        # In 10 nhân viên đầu tiên
        for employee_name, stats in sorted_by_no_logwork[:10]:
            print(f"{employee_name[:28]:<30}{stats['total_tasks']:<15}{stats['tasks_without_logwork']:<15}{stats['no_logwork_ratio']:.1f}%")
        
        # Sắp xếp theo tỷ lệ không logwork (cho nhân viên có ít nhất 3 task)
        # print("\n\n📊 TOP 10 NHÂN VIÊN CÓ TỶ LỆ TASK KHÔNG LOGWORK CAO NHẤT:")
        sorted_by_ratio = sorted(
            [item for item in employee_detailed_stats.items() if item[1]["total_tasks"] >= 3],  # Chỉ xét nhân viên có ít nhất 3 task
            key=lambda x: x[1]["no_logwork_ratio"], 
            reverse=True
        )
        
        # In tiêu đề
        print(f"{'Tên nhân viên':<30}{'Tổng task':<15}{'Không logwork':<15}{'Tỷ lệ không logwork':<20}")
        print("-" * 80)
        
        # In 10 nhân viên đầu tiên
        for employee_name, stats in sorted_by_ratio[:10]:
            print(f"{employee_name[:28]:<30}{stats['total_tasks']:<15}{stats['tasks_without_logwork']:<15}{stats['no_logwork_ratio']:.1f}%")
        
        # Thống kê theo component
        component_task_counts = {}
        
        for task in all_tasks:
            if task.get('components'):
                for component in task.get('components'):
                    if component in component_task_counts:
                        component_task_counts[component] += 1
                    else:
                        component_task_counts[component] = 1
            else:
                if "Không có component" in component_task_counts:
                    component_task_counts["Không có component"] += 1
                else:
                    component_task_counts["Không có component"] = 1
        
        # Tạo báo cáo thống kê
        with open(report_file, 'w', encoding='utf-8') as f:
            # ... existing code ...
            
            # Thống kê theo component
            f.write("THỐNG KÊ THEO COMPONENT:\n")
            for component, count in sorted(component_task_counts.items(), key=lambda x: x[1], reverse=True):
                f.write(f"• {component}: {count} task ({count/len(all_tasks)*100:.1f}%)\n")
            f.write("\n")
        
        # Tạo báo cáo theo dự án thực tế
        print("\n\n📊 TẠO BÁO CÁO THEO DỰ ÁN THỰC TẾ:")
        
        # Lấy danh sách các dự án thực tế
        actual_projects = {}
        
        for task in all_tasks:
            actual_project = task.get('actual_project', task.get('project', 'Unknown'))
            if actual_project not in actual_projects:
                actual_projects[actual_project] = 0
            actual_projects[actual_project] += 1
        
        # In danh sách dự án thực tế và số lượng task
        print(f"\n📊 TÌM THẤY {len(actual_projects)} DỰ ÁN THỰC TẾ:")
        print("=" * 80)
        for project, count in sorted(actual_projects.items(), key=lambda x: x[1], reverse=True):
            print(f"🔹 {project}: {count} task ({count/len(all_tasks)*100:.1f}%)")
        print("=" * 80)
        
        # THÊM DEBUG: Hiển thị task keys của từng project
        # print(f"\n🔍 CHI TIẾT TASK KEYS THEO DỰ ÁN:")
        print("=" * 80)
        for project_name in sorted(actual_projects.keys()):
            project_tasks = [task for task in all_tasks if task.get('actual_project', task.get('project', 'Unknown')) == project_name]
            task_keys = [task.get('key', 'Unknown') for task in project_tasks[:5]]  # Chỉ hiển thị 5 task đầu
            remaining = len(project_tasks) - 5
            if remaining > 0:
                task_keys.append(f"... và {remaining} task khác")
            print(f"🔸 {project_name} ({len(project_tasks)} task):")
            print(f"   📋 Tasks: {', '.join(task_keys)}")
        print("=" * 80)
        
        # KIỂM TRA: Debug tại sao PKT không được gộp chung
        if "PKT" in actual_projects:
            # print(f"\n🔍 DEBUG: PKT xuất hiện với {actual_projects['PKT']} task như dự án riêng biệt!")
            print("❌ VẤN ĐỀ: PKT phải được gộp vào '[Project] Kho Tổng + PIM' chứ không phải tách riêng")
            print("🔧 Kiểm tra logic get_actual_project() có hoạt động đúng không...")
            
            # Tạm thời loại bỏ PKT để không tạo báo cáo sai
            del actual_projects["PKT"]
            print("⚠️ Tạm thời loại bỏ PKT khỏi danh sách, cần sửa logic get_actual_project()")
            
        if "IMS" in actual_projects:
            # print(f"\n🔍 DEBUG: IMS xuất hiện với {actual_projects['IMS']} task - đã bị loại bỏ logic tạo riêng")
            del actual_projects["IMS"]
            print("✅ Đã loại bỏ IMS khỏi danh sách tạo báo cáo riêng")
        
        # Tạo thư mục cho báo cáo dự án
        project_reports_dir = os.path.join(os.path.dirname(report_file), "project_reports")
        os.makedirs(project_reports_dir, exist_ok=True)
        
        # Tạo báo cáo cho từng dự án
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        success_count = 0
        
        
        for project_name, count in actual_projects.items():
            # KIỂM TRA CUỐI CÙNG: Không tạo báo cáo riêng cho IMS
            if project_name == "IMS":
                print(f"🚫 CẢNH BÁO: Phát hiện IMS trong danh sách tạo báo cáo - BỎ QUA!")
                continue
                
            # Bỏ qua các dự án có ít hơn 2 task
            if count < 2:
                print(f"⚠️ Bỏ qua dự án {project_name} vì chỉ có {count} task")
                continue
                
            # Tạo tên file báo cáo
            safe_project_name = re.sub(r'[^a-zA-Z0-9_]', '_', project_name)
            project_report_file = os.path.join(project_reports_dir, f"{safe_project_name}_{timestamp}.txt")
            
            # Tạo báo cáo dự án
            if create_project_report(project_name, all_tasks, employee_detailed_stats, project_report_file):
                success_count += 1
        
        print(f"✅ Đã tạo {success_count}/{len(actual_projects)} báo cáo dự án trong thư mục: {project_reports_dir}")
            
        # Tạo báo cáo tổng hợp cho tất cả các dự án
        summary_report_file = os.path.join(project_reports_dir, f"all_projects_summary_{timestamp}.txt")
        summary_csv_file = os.path.join(project_reports_dir, f"all_projects_summary_{timestamp}.csv")
        
        project_stats_for_comparison = create_projects_summary_report(all_tasks, summary_report_file, summary_csv_file)
        
        # Kiểm tra tính nhất quán giữa báo cáo tổng hợp và báo cáo chi tiết dự án
        if project_stats_for_comparison:
            # Lấy tất cả các file trong thư mục, ngoại trừ file báo cáo tổng hợp
            project_report_files = [os.path.join(project_reports_dir, f) for f in os.listdir(project_reports_dir) 
                                    if not f.startswith("all_projects_summary")]
            
            # Kiểm tra tính nhất quán
            is_consistent = check_consistency(project_stats_for_comparison, project_report_files)
            
            if not is_consistent:
                print("\n⚠️ Cần kiểm tra lại báo cáo chi tiết dự án và tổng hợp!")
            else:
                print("\n✅ Tất cả báo cáo đều nhất quán!")
        
        # Đồng bộ hóa báo cáo tổng hợp và báo cáo chi tiết dự án
       #synchronize_reports(all_tasks, project_reports_dir, timestamp)
        
    except Exception as e:
        print(f"❌ Lỗi khi xử lý: {str(e)}")
        import traceback
        traceback.print_exc()

def get_update_reason(issue_key, jira_url, username, password, assignee_name=None, assignee_updates_only=False, status_updates_only=False):
    """
    Lấy lý do cập nhật (changelog) cho issue và thông tin người cập nhật cuối cùng
    
    Args:
        issue_key (str): Mã issue cần lấy thông tin
        jira_url (str): URL của Jira
        username (str): Tên đăng nhập Jira
        password (str): Mật khẩu Jira
        assignee_name (str, optional): Tên của người được gán task
        assignee_updates_only (bool, optional): True nếu chỉ lấy cập nhật từ người được gán
        status_updates_only (bool, optional): True nếu chỉ lấy cập nhật thay đổi trạng thái do chính assignee thực hiện
        
    Returns:
        dict: Thông tin lý do cập nhật và người cập nhật cuối cùng
    """
    # Khởi tạo biến kết quả ngay từ đầu
    reasons = []
    last_updater = None
    last_update_time = None
    last_update_time_formatted = ""
    main_update_reason = "Không xác định"  # Lý do chính
    update_category = "unknown"  # Loại cập nhật
    
    # Các trường cập nhật cần bỏ qua
    ignore_update_fields = ["fixVersions", "Fix Version", "Sprint", "RemoteIssueLink", "components", "Fix Version"]
    
    try:
        url = f"{jira_url}/rest/api/2/issue/{issue_key}?expand=changelog"
        response = requests.get(
            url,
            auth=HTTPBasicAuth(username, password),
            headers={"Accept": "application/json"},
            timeout=30
        )
        
        if response.status_code != 200:
            return {
                "reasons": [], 
                "last_updater": None, 
                "last_update_time": "",
                "main_update_reason": "Lỗi kết nối",
                "update_category": "error"
            }
        
        data = response.json()
        histories = data.get("changelog", {}).get("histories", [])
        
        if not histories:
            return {
                "reasons": ["Không có lịch sử cập nhật cho issue này"], 
                "last_updater": None, 
                "last_update_time": "",
                "main_update_reason": "Không có lịch sử cập nhật",
                "update_category": "no_history"
            }
        
        # Sắp xếp lịch sử theo thời gian từ mới đến cũ
        sorted_histories = sorted(histories, key=lambda x: x.get("created", ""), reverse=True)
        
        index = 0
        found_significant_update = False
        
        # Tìm cập nhật có ý nghĩa (không phải chỉ là thay đổi Fix Version hoặc Sprint)
        while index < len(sorted_histories) and not found_significant_update:
            history = sorted_histories[index]
            items = history.get("items", [])
            
            # Nếu chỉ lấy cập nhật của người được gán task
            if assignee_updates_only and assignee_name:
                updater_name = history.get("author", {}).get("displayName", "")
                if updater_name != assignee_name:
                    index += 1
                    continue
            
            # Nếu chỉ lấy cập nhật thay đổi trạng thái
            if status_updates_only:
                has_status_change = False
                for item in items:
                    if item.get("field", "").lower() == "status":
                        has_status_change = True
                        break
                
                # Nếu không có thay đổi status, bỏ qua cập nhật này
                if not has_status_change:
                    index += 1
                    continue
                
                # Kiểm tra xem người cập nhật status có phải là assignee không
                updater_name = history.get("author", {}).get("displayName", "")
                if assignee_name and updater_name != assignee_name:
                    # Người cập nhật status không phải assignee, bỏ qua
                    index += 1
                    continue
                else:
                    found_significant_update = True
            else:
                # Kiểm tra xem lịch sử cập nhật này có chứa các trường quan trọng không
                only_ignorable_fields = True
                for item in items:
                    if item.get("field") not in ignore_update_fields:
                        only_ignorable_fields = False
                        break
                
                # Nếu không chỉ chứa các trường cần bỏ qua, đánh dấu là đã tìm thấy cập nhật có ý nghĩa
                if not only_ignorable_fields or len(items) == 0:
                    found_significant_update = True
                else:
                    index += 1  # Chuyển sang cập nhật tiếp theo
        
        # Lấy thông tin cập nhật quan trọng nhất
        if index < len(sorted_histories):
            significant_history = sorted_histories[index]
            last_update_time = significant_history.get("created", "")
            last_updater_info = significant_history.get("author", {})
            last_updater = {
                "name": last_updater_info.get("displayName", ""),
                "email": last_updater_info.get("emailAddress", ""),
                "key": last_updater_info.get("key", "")
            }
            
            # Format thời gian
            if last_update_time:
                try:
                    last_update_time_dt = datetime.fromisoformat(last_update_time.replace('Z', '+00:00'))
                    last_update_time_formatted = last_update_time_dt.strftime('%d/%m/%Y %H:%M')
                except ValueError as e:
                    print(f"⚠️ Lỗi định dạng thời gian cho issue {issue_key}: {e}")
                    last_update_time_formatted = last_update_time
            else:
                last_update_time_formatted = ""
            
            # Phân tích và phân loại loại cập nhật chính
            update_category, main_update_reason = _categorize_update(significant_history.get("items", []))
            
            # Thêm thông tin tóm tắt về lý do chính
            reasons.append(f"🎯 Lý do ghi nhận task: {main_update_reason}")
            reasons.append(f"🔄 Cập nhật quan trọng: {last_update_time_formatted} bởi {last_updater['name']} ({last_updater['email']})")
            
            # Thêm chi tiết những thay đổi trong lần cập nhật này
            for item in significant_history.get("items", []):
                field = item.get("field")
                from_str = item.get("fromString", "")
                to_str = item.get("toString", "")
                field_emoji = _get_field_emoji(field)
                reasons.append(f"{field_emoji} Thay đổi: {field} từ '{from_str}' sang '{to_str}'")
            
            # Nếu đã bỏ qua một số cập nhật trước đó, thông báo cho người dùng
            if index > 0:
                skipped_updates = sorted_histories[:index]
                reasons.append(f"ℹ️ Đã bỏ qua {len(skipped_updates)} cập nhật chỉ liên quan đến Fix Version, Sprint, RemoteIssueLink hoặc Components")
                
                # Thêm thông tin về các cập nhật đã bỏ qua
                _add_skipped_updates_info(reasons, skipped_updates)
        
        # Thêm lịch sử các lần cập nhật trước đó
        _add_previous_updates_info(reasons, sorted_histories[index+1:])
        
        return {
            "reasons": reasons,
            "last_updater": last_updater,
            "last_update_time": last_update_time_formatted,
            "main_update_reason": main_update_reason,
            "update_category": update_category
        }
        
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Lỗi kết nối khi lấy lý do cập nhật cho issue {issue_key}: {e}")
        return {
            "reasons": [f"Lỗi kết nối: {str(e)}"], 
            "last_updater": None, 
            "last_update_time": "",
            "main_update_reason": "Lỗi kết nối",
            "update_category": "error"
        }
    except ValueError as e:
        print(f"⚠️ Lỗi xử lý dữ liệu JSON cho issue {issue_key}: {e}")
        return {
            "reasons": [f"Lỗi dữ liệu: {str(e)}"], 
            "last_updater": None, 
            "last_update_time": "",
            "main_update_reason": "Lỗi dữ liệu",
            "update_category": "error"
        }
    except Exception as e:
        print(f"⚠️ Lỗi không xác định khi lấy lý do cập nhật cho issue {issue_key}: {e}")
        return {
            "reasons": [f"Lỗi không xác định: {str(e)}"], 
            "last_updater": None, 
            "last_update_time": "",
            "main_update_reason": "Lỗi không xác định",
            "update_category": "error"
        }

def _categorize_update(items):
    """
    Phân loại và xác định lý do chính của cập nhật
    
    Args:
        items (list): Danh sách các thay đổi trong lần cập nhật
        
    Returns:
        tuple: (category, main_reason) - Loại cập nhật và lý do chính
    """
    if not items:
        return "comment", "Thêm comment hoặc cập nhật khác"
    
    # Ưu tiên các loại thay đổi theo mức độ quan trọng
    priority_fields = {
        "status": ("status_change", "Thay đổi trạng thái"),
        "assignee": ("assignee_change", "Thay đổi người được gán"),
        "resolution": ("resolution_change", "Thay đổi resolution"),
        "priority": ("priority_change", "Thay đổi mức độ ưu tiên"),
        "summary": ("summary_change", "Thay đổi tiêu đề"),
        "description": ("description_change", "Cập nhật mô tả"),
        "comment": ("comment", "Thêm comment"),
        "attachment": ("attachment", "Thêm/xóa file đính kèm"),
        "link": ("link_change", "Thay đổi liên kết"),
        "labels": ("labels_change", "Thay đổi labels"),
        "timespent": ("time_logging", "Ghi nhận thời gian làm việc"),
        "timeestimate": ("estimate_change", "Thay đổi ước tính thời gian"),
        "duedate": ("duedate_change", "Thay đổi deadline")
    }
    
    # Tìm thay đổi quan trọng nhất
    for item in items:
        field = item.get("field", "").lower()
        from_str = item.get("fromString", "")
        to_str = item.get("toString", "")
        
        # Kiểm tra từng loại thay đổi theo thứ tự ưu tiên
        for priority_field, (category, base_reason) in priority_fields.items():
            if priority_field in field:
                # Tùy chỉnh lý do dựa trên loại thay đổi cụ thể
                if priority_field == "status":
                    return category, f"Thay đổi trạng thái từ '{from_str}' sang '{to_str}'"
                elif priority_field == "assignee":
                    if not from_str:
                        return category, f"Gán task cho '{to_str}'"
                    elif not to_str:
                        return category, f"Bỏ gán task (trước đó: '{from_str}')"
                    else:
                        return category, f"Chuyển gán từ '{from_str}' sang '{to_str}'"
                elif priority_field == "resolution":
                    if to_str:
                        return category, f"Đặt resolution: '{to_str}'"
                    else:
                        return category, f"Xóa resolution (trước đó: '{from_str}')"
                elif priority_field == "timespent":
                    return category, f"Ghi nhận thời gian làm việc: {to_str}"
                else:
                    return category, f"{base_reason}: '{from_str}' → '{to_str}'"
    
    # Nếu không match với các trường ưu tiên, tạo lý do từ thay đổi đầu tiên
    first_item = items[0]
    field = first_item.get("field", "")
    from_str = first_item.get("fromString", "")
    to_str = first_item.get("toString", "")
    
    return "other", f"Thay đổi {field}: '{from_str}' → '{to_str}'"

def _get_field_emoji(field):
    """
    Lấy emoji phù hợp cho từng loại trường
    
    Args:
        field (str): Tên trường
        
    Returns:
        str: Emoji tương ứng
    """
    field_lower = field.lower()
    
    emoji_map = {
        "status": "🔄",
        "assignee": "👤", 
        "resolution": "✅",
        "priority": "⚡",
        "summary": "📝",
        "description": "📋",
        "comment": "💬",
        "attachment": "📎",
        "link": "🔗",
        "labels": "🏷️",
        "timespent": "⏱️",
        "timeestimate": "⏰",
        "duedate": "📅",
        "components": "🧩",
        "fixversions": "🔖",
        "sprint": "🏃"
    }
    
    for key, emoji in emoji_map.items():
        if key in field_lower:
            return emoji
    
    return "📝"  # Default emoji

def _add_skipped_updates_info(reasons, skipped_updates):
    """
    Thêm thông tin về các cập nhật đã bỏ qua
    
    Args:
        reasons (list): Danh sách lý do để thêm vào
        skipped_updates (list): Danh sách các cập nhật đã bỏ qua
    """
    for history in skipped_updates:
        created = history.get("created", "")
        author = history.get("author", {}).get("displayName", "")
        
        created_date = ""
        if created:
            try:
                created_date = datetime.fromisoformat(created.replace('Z', '+00:00')).strftime('%d/%m/%Y %H:%M')
            except ValueError as e:
                print(f"⚠️ Lỗi định dạng thời gian trong lịch sử cập nhật: {e}")
                created_date = created
        
        for item in history.get("items", []):
            field = item.get("field")
            from_str = item.get("fromString", "")
            to_str = item.get("toString", "")
            reasons.append(f"⏭️ {created_date}: {author} thay đổi {field} từ '{from_str}' sang '{to_str}'")

def _add_previous_updates_info(reasons, previous_histories):
    """
    Thêm thông tin về các cập nhật trước đó
    
    Args:
        reasons (list): Danh sách lý do để thêm vào
        previous_histories (list): Danh sách các cập nhật trước đó
    """
    for history in previous_histories:
        created = history.get("created", "")
        author = history.get("author", {}).get("displayName", "")
        
        created_date = ""
        if created:
            try:
                created_date = datetime.fromisoformat(created.replace('Z', '+00:00')).strftime('%d/%m/%Y %H:%M')
            except ValueError as e:
                print(f"⚠️ Lỗi định dạng thời gian trong lịch sử cập nhật trước đó: {e}")
                created_date = created
        
        for item in history.get("items", []):
            field = item.get("field")
            from_str = item.get("fromString", "")
            to_str = item.get("toString", "")
            reasons.append(f"{created_date}: {author} thay đổi {field} từ '{from_str}' sang '{to_str}'")

def get_actual_project(jira_project, components):
    """
    Xác định dự án thực tế dựa vào project Jira và components
    
    Args:
        components (list): Danh sách components của task
    
    Returns:
        str: Tên dự án thực tế
    """
    # DEBUG: Theo dõi tất cả các lời gọi đến hàm này cho PKT và WAK
    # if jira_project in ["PKT", "WAK"]:
    #     print(f"🔍 get_actual_project() được gọi với jira_project='{jira_project}', components={components}")
    
    # Chuyển đổi components thành chuỗi để dễ tìm kiếm
    components_str = ", ".join(components) if components else ""
    
    # Nếu project Jira là FC, phân loại theo component
    if jira_project == "FC":
        # RSA + RSA eCom + Shipment
        if any(comp in ["LC Offline Q1", "LC RSA Ecom", "B05. RSA/RSA ECOM", "LCD", "Tuning RSA Ecom"] for comp in components):
            return "RSA + RSA eCom + Shipment"
        
        # Payment FPT Pay - GIỮ LẠI trong FC theo yêu cầu
        if any(comp in ["PaymentTenacy"] for comp in components):
            return "Payment FPT Pay"
        
        # Web App KHLC - GIỮ LẠI trong FC theo yêu cầu  
        if any(comp.startswith("Ecom - ") for comp in components):
            return "Web App KHLC"
        
        # Các logic khác đã chuyển sang projects riêng:
        # - Noti + Loyalty + Core Cust → FSS project  
        # - IMS → đã loại bỏ
        # - Kho Tổng + PIM → PKT project
    elif jira_project == "PKT":
        # Kho tổng + PIM - project mới PKT, không cần lọc theo component
        # LUÔN trả về tên đã chuẩn hóa cho tất cả task từ PKT
        return "[Project] Kho Tổng + PIM"
    elif jira_project == "WAK":
        # Web App KHLC - project mới WAK, không cần lọc theo component
        return "Web App KHLC"
    elif jira_project == "PPFP":
        # Payment FPT Pay - project mới PPFP, không cần lọc theo component
        return "Payment FPT Pay"
    elif jira_project == "FSS":
        return "Noti + Loyalty + Core Cust"
    
    # DEBUG: Nếu không xác định được dự án cụ thể
    if jira_project == "PKT":
        # print(f"🚨 CẢNH BÁO: PKT task với components {components} không được xử lý đúng, trả về mặc định '[Project] Kho Tổng + PIM'")
        return "[Project] Kho Tổng + PIM"
    
    if jira_project == "WAK":
        # print(f"🚨 CẢNH BÁO: WAK task với components {components} không được xử lý đúng, trả về mặc định 'Web App KHLC'")
        return "Web App KHLC"
    
    # Nếu không xác định được, trả về project Jira
    return jira_project

def calculate_saved_time(estimated_hours, actual_hours):
    """
    Chuẩn hóa cách tính thời gian tiết kiệm để đảm bảo sự nhất quán giữa các báo cáo
    
    Args:
        estimated_hours (float): Tổng thời gian ước tính (giờ)
        actual_hours (float): Tổng thời gian thực tế (giờ)
        
    Returns:
        tuple: (thời gian tiết kiệm, tỷ lệ tiết kiệm %)
    """
    estimated_hours = float(estimated_hours or 0)
    actual_hours = float(actual_hours or 0)
    saved_hours = estimated_hours - actual_hours
    saving_ratio = (saved_hours / estimated_hours * 100) if estimated_hours > 0 else 0
    return saved_hours, saving_ratio

def create_employee_detailed_report(employee_name, employee_email, tasks, output_file):
    """
    Tạo báo cáo chi tiết về task của một nhân viên và lưu vào file txt
    
    Args:
        employee_name (str): Tên nhân viên
        employee_email (str): Email hoặc tài khoản của nhân viên
        tasks (list): Danh sách các task của nhân viên
        output_file (str): Đường dẫn đến file báo cáo
    """
    try:
         # Cập nhật trạng thái logwork cho task cha trước khi tạo báo cáo
        parent_to_children = {}
        for task in tasks:
            if task.get('is_subtask') and task.get('parent_key'):
                parent_key = task.get('parent_key')
                if parent_key not in parent_to_children:
                    parent_to_children[parent_key] = []
                parent_to_children[parent_key].append(task)
        
        # Cập nhật trạng thái task cha dựa trên task con
        for task in tasks:
            task_key = task.get('key')
            if task_key in parent_to_children:  # Nếu task này là task cha có con
                children = parent_to_children[task_key]
                
                # Nếu task cha không có estimate nhưng các task con có estimate
                if task.get('original_estimate_hours', 0) == 0:
                    total_child_estimate = sum(child.get('original_estimate_hours', 0) for child in children)
                    if total_child_estimate > 0:
                        # Cập nhật estimate cho task cha từ tổng estimate của các task con
                        task['original_estimate_hours'] = total_child_estimate
                        task['has_estimate'] = True
                        print(f"   ℹ️ Cập nhật estimate cho task cha {task_key} từ tổng estimate của các task con: {total_child_estimate:.2f}h")
                
                # Kiểm tra và cập nhật trạng thái logwork
                if not task.get('has_worklog'):  # Nếu task cha chưa có log work
                    children_with_logwork = [child for child in children if child.get('has_worklog', False)]
                    if children_with_logwork:  # Nếu có ít nhất một task con đã log work
                        # Đánh dấu task cha là đã log work
                        task['has_worklog'] = True
                        task['has_child_with_logwork'] = True  # Thêm trường để đánh dấu
                        
                        # Quan trọng: Cập nhật time_saved_hours nếu đang là -1 (không có logwork)
                        if task.get('time_saved_hours', -1) == -1:
                            # Tính tổng thời gian từ các task con
                            total_child_time = sum(child.get('total_hours', 0) for child in children_with_logwork)
                            
                            # Cập nhật thời gian thực tế cho task cha từ tổng thời gian của các task con
                            task['total_hours'] = total_child_time
                            
                            # Nếu task cha không có estimate nhưng các task con có estimate
                            if task.get('original_estimate_hours', 0) == 0:
                                # Tính tổng estimate từ task con
                                total_child_estimate = sum(child.get('original_estimate_hours', 0) for child in children)
                                if total_child_estimate > 0:
                                    # Cập nhật estimate cho task cha
                                    task['original_estimate_hours'] = total_child_estimate
                                    task['has_estimate'] = True
                                    print(f"   ℹ️ Cập nhật estimate cho task cha {task_key} từ tổng estimate của các task con: {total_child_estimate:.2f}h")
                            
                            # Sau đó tính time_saved_hours
                            if task.get('original_estimate_hours', 0) > 0:
                                saved_hours, saving_ratio = calculate_saved_time(task.get('original_estimate_hours', 0), total_child_time)
                                task['time_saved_hours'] = saved_hours
                                task['time_saved_percent'] = saving_ratio
                                print(f"   ℹ️ Cập nhật time_saved_hours cho task cha {task_key} từ task con: {saved_hours:.2f}h ({saving_ratio:.1f}%)")
                            else:
                                # Nếu thực sự không có estimate nào (cả cha và con đều không có)
                                task['time_saved_hours'] = -2  # Đánh dấu đặc biệt: có logwork nhưng không có estimate
                                print(f"   ℹ️ Task cha {task_key} đã được đánh dấu có logwork (từ task con) nhưng không có estimate")



        # Phân loại task theo tiêu chí mới
        tasks_with_logwork = [task for task in tasks if task.get('has_worklog', False)]
        tasks_without_logwork = [task for task in tasks if not task.get('has_worklog', False)]
        
        # Phân loại chi tiết theo thời gian tiết kiệm
        tasks_no_logwork = [task for task in tasks if task.get('time_saved_hours', -1) == -1]  # Không có logwork
        tasks_no_saving = [task for task in tasks if task.get('time_saved_hours', -1) == 0]    # Có logwork nhưng không tiết kiệm
        tasks_with_saving = [task for task in tasks if task.get('time_saved_hours', -1) > 0]   # Có logwork và tiết kiệm
        tasks_exceed_time = [task for task in tasks if task.get('time_saved_hours', -1) < 0 and task.get('time_saved_hours', -1) != -1]  # Vượt thời gian
        tasks_no_estimate = [task for task in tasks if task.get('time_saved_hours', -1) == -2]  # Có logwork nhưng không có estimate
        
        # Phân loại theo dự án
        projects = {}
        for task in tasks:
            project_key = task.get('actual_project', task.get('project', 'Unknown'))
            if project_key not in projects:
                projects[project_key] = {
                    'name': task.get('project_name', ''),
                    'total_tasks': 0,
                    'tasks_with_logwork': 0,
                    'tasks_without_logwork': 0,
                    'tasks_no_saving': 0,
                    'tasks_with_saving': 0,
                    'tasks_exceed_time': 0,
                    'total_estimate_hours': 0,
                    'total_actual_hours': 0,
                    'total_saved_hours': 0,
                }
            
            # Cập nhật thống kê dự án
            project_data = projects[project_key]
            project_data['total_tasks'] += 1
            project_data['total_estimate_hours'] += task.get('original_estimate_hours', 0)
            project_data['total_actual_hours'] += task.get('total_hours', 0)
            
            # Cập nhật phân loại task
            if task.get('time_saved_hours', -1) == -1:
                project_data['tasks_without_logwork'] += 1
            elif task.get('time_saved_hours', -1) == 0:
                project_data['tasks_with_logwork'] += 1
                project_data['tasks_no_saving'] += 1
            elif task.get('time_saved_hours', -1) > 0:
                project_data['tasks_with_logwork'] += 1
                project_data['tasks_with_saving'] += 1
                project_data['total_saved_hours'] += task.get('time_saved_hours', 0)
            else:
                project_data['tasks_with_logwork'] += 1
                project_data['tasks_exceed_time'] += 1

        # Phân loại theo component
        components = {}
        for task in tasks:
            for component in task.get('components', []):
                if not component:
                    continue
                    
                if component not in components:
                    components[component] = {
                        'total_tasks': 0,
                        'tasks_with_logwork': 0,
                        'tasks_without_logwork': 0,
                        'tasks_no_saving': 0,
                        'tasks_with_saving': 0,
                        'tasks_exceed_time': 0,
                        'total_estimate_hours': 0,
                        'total_actual_hours': 0,
                        'total_saved_hours': 0,
                    }
                
                # Cập nhật thống kê component
                component_data = components[component]
                component_data['total_tasks'] += 1
                component_data['total_estimate_hours'] += task.get('original_estimate_hours', 0)
                component_data['total_actual_hours'] += task.get('total_hours', 0)
                
                # Cập nhật phân loại task
                if task.get('time_saved_hours', -1) == -1:
                    component_data['tasks_without_logwork'] += 1
                elif task.get('time_saved_hours', -1) == 0:
                    component_data['tasks_with_logwork'] += 1
                    component_data['tasks_no_saving'] += 1
                elif task.get('time_saved_hours', -1) > 0:
                    component_data['tasks_with_logwork'] += 1
                    component_data['tasks_with_saving'] += 1
                    component_data['total_saved_hours'] += task.get('time_saved_hours', 0)
                else:
                    component_data['tasks_with_logwork'] += 1
                    component_data['tasks_exceed_time'] += 1

        # Xử lý task không có component
        no_component_tasks = [task for task in tasks if not task.get('components')]
        if no_component_tasks:
            components['Không có component'] = {
                'total_tasks': len(no_component_tasks),
                'tasks_with_logwork': len([t for t in no_component_tasks if t.get('has_worklog', False)]),
                'tasks_without_logwork': len([t for t in no_component_tasks if not t.get('has_worklog', False)]),
                'tasks_no_saving': len([t for t in no_component_tasks if t.get('time_saved_hours', -1) == 0]),
                'tasks_with_saving': len([t for t in no_component_tasks if t.get('time_saved_hours', -1) > 0]),
                'tasks_exceed_time': len([t for t in no_component_tasks if t.get('time_saved_hours', -1) < 0 and t.get('time_saved_hours', -1) != -1]),
                'total_estimate_hours': sum(t.get('original_estimate_hours', 0) for t in no_component_tasks),
                'total_actual_hours': sum(t.get('total_hours', 0) for t in no_component_tasks),
                'total_saved_hours': sum(max(0, t.get('time_saved_hours', 0)) for t in no_component_tasks if t.get('time_saved_hours', 0) > 0),
            }

        # Phân loại theo dự án thực tế
        actual_projects = {}
        for task in tasks:
            actual_project = task.get('actual_project', task.get('project', 'Unknown'))
            
            if actual_project not in actual_projects:
                actual_projects[actual_project] = {
                    'total_tasks': 0,
                    'tasks_with_logwork': 0,
                    'tasks_without_logwork': 0,
                    'tasks_no_saving': 0,
                    'tasks_with_saving': 0,
                    'tasks_exceed_time': 0,
                    'total_estimate_hours': 0,
                    'total_actual_hours': 0,
                    'total_saved_hours': 0,
                }
            
            # Cập nhật thống kê dự án thực tế
            project_data = actual_projects[actual_project]
            project_data['total_tasks'] += 1
            project_data['total_estimate_hours'] += task.get('original_estimate_hours', 0)
            project_data['total_actual_hours'] += task.get('total_hours', 0)
            
            # Cập nhật phân loại task
            if task.get('time_saved_hours', -1) == -1:
                project_data['tasks_without_logwork'] += 1
            elif task.get('time_saved_hours', -1) == 0:
                project_data['tasks_with_logwork'] += 1
                project_data['tasks_no_saving'] += 1
            elif task.get('time_saved_hours', -1) > 0:
                project_data['tasks_with_logwork'] += 1
                project_data['tasks_with_saving'] += 1
                project_data['total_saved_hours'] += task.get('time_saved_hours', 0)
            else:
                project_data['tasks_with_logwork'] += 1
                project_data['tasks_exceed_time'] += 1

        # Tính toán thống kê tổng thể
        total_estimate_hours = sum(task.get('original_estimate_hours', 0) for task in tasks)
        total_actual_hours = sum(task.get('total_hours', 0) for task in tasks)
        total_saved_hours = sum(max(0, task.get('time_saved_hours', 0)) for task in tasks if task.get('time_saved_hours', 0) > 0)
        saved_percentage = (total_saved_hours / total_estimate_hours * 100) if total_estimate_hours > 0 else 0
        
        # Tạo báo cáo
        with open(output_file, 'w', encoding='utf-8') as f:
            # Header trang trí
            f.write("=" * 80 + "\n")
            f.write(f"{'BÁO CÁO CHI TIẾT CÔNG VIỆC NHÂN VIÊN':^80}\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"📅 Thời gian tạo: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n\n")
            
            # Thông tin nhân viên
            f.write("📋 THÔNG TIN NHÂN VIÊN\n")
            f.write("-" * 80 + "\n")
            f.write(f"👤 Họ và tên: {employee_name}\n")
            f.write(f"📧 Mail/Account: {employee_email}\n\n")
            
            # Thống kê tổng quan
            f.write("📊 THỐNG KÊ TỔNG QUAN\n")
            f.write("-" * 80 + "\n")
            f.write(f"📈 Tổng task: {len(tasks)}\n")
            f.write(f"✅ Task có logwork: {len(tasks_with_logwork)}\n")
            f.write(f"⏳ Task không có logwork: {len(tasks_no_logwork)}\n")
            f.write(f"⚖️ Task có logwork nhưng không tiết kiệm: {len(tasks_no_saving)}\n")
            f.write(f"⚡ Task có logwork nhưng không có estimate: {len(tasks_no_estimate)}\n")
            f.write(f"💰 Task tiết kiệm thời gian: {len(tasks_with_saving)}\n")
            f.write(f"⚠️ Task vượt thời gian dự kiến: {len(tasks_exceed_time)}\n\n")
            
            # Thông tin thời gian
            f.write("⏱️ TỔNG HỢP THỜI GIAN\n")
            f.write("-" * 80 + "\n")
            f.write(f"🔍 Tổng thời gian dự kiến (không AI): {total_estimate_hours:.2f} giờ\n")
            f.write(f"⚙️ Tổng thời gian dùng AI: {total_actual_hours:.2f} giờ\n")
            if total_saved_hours > 0:
                f.write(f"💎 Thời gian tiết kiệm được: {total_saved_hours:.2f} giờ ({saved_percentage:.1f}%)\n")
                efficiency = (total_saved_hours / total_estimate_hours) * 100 if total_estimate_hours > 0 else 0
                f.write(f"📈 Hiệu suất làm việc: {efficiency:.1f}%\n\n")
            else:
                f.write(f"⚠️ Không tiết kiệm được thời gian\n\n")
            
            # Thống kê theo component
            f.write("\n📊 THỐNG KÊ THEO COMPONENT\n")
            f.write("-" * 80 + "\n")
            
            if components:
                for component_name, component_data in sorted(components.items(), key=lambda x: x[1]['total_tasks'], reverse=True):
                    component_saved_percentage = (component_data['total_saved_hours'] / component_data['total_estimate_hours'] * 100) if component_data['total_estimate_hours'] > 0 else 0
                    f.write(f"🔹 {component_name}\n")
                    f.write(f"   📌 Tổng task: {component_data['total_tasks']}\n")
                    f.write(f"   ✅ Task có logwork: {component_data['tasks_with_logwork']}\n")
                    f.write(f"   ⏳ Task không có logwork: {component_data['tasks_without_logwork']}\n")
                    f.write(f"   ⚖️ Task không tiết kiệm: {component_data['tasks_no_saving']}\n")
                    f.write(f"   💰 Task tiết kiệm: {component_data['tasks_with_saving']}\n")
                    f.write(f"   ⚠️ Task vượt thời gian: {component_data['tasks_exceed_time']}\n")
                    f.write(f"   ⏱️ Thời gian dự kiến: {component_data['total_estimate_hours']:.2f}h, Thực tế: {component_data['total_actual_hours']:.2f}h\n")
                    if component_data['total_saved_hours'] > 0:
                        f.write(f"   💎 Tiết kiệm: {component_data['total_saved_hours']:.2f}h ({component_saved_percentage:.1f}%)\n")
                    f.write("\n")
            else:
                f.write("   Không có dữ liệu component.\n\n")
            
            # Thống kê theo dự án thực tế
            f.write("\n📊 THỐNG KÊ THEO DỰ ÁN\n")
            f.write("-" * 80 + "\n")
            
            if actual_projects:
                for project_name, project_data in sorted(actual_projects.items(), key=lambda x: x[1]['total_tasks'], reverse=True):
                    project_saved_percentage = (project_data['total_saved_hours'] / project_data['total_estimate_hours'] * 100) if project_data['total_estimate_hours'] > 0 else 0
                    f.write(f"🔹 {project_name}\n")
                    f.write(f"   📌 Tổng task: {project_data['total_tasks']}\n")
                    f.write(f"   ✅ Task có logwork: {project_data['tasks_with_logwork']}\n")
                    f.write(f"   ⏳ Task không có logwork: {project_data['tasks_without_logwork']}\n")
                    f.write(f"   ⚖️ Task không tiết kiệm: {project_data['tasks_no_saving']}\n")
                    f.write(f"   💰 Task tiết kiệm: {project_data['tasks_with_saving']}\n")
                    f.write(f"   ⚠️ Task vượt thời gian: {project_data['tasks_exceed_time']}\n")
                    f.write(f"   ⏱️ Thời gian dự kiến: {project_data['total_estimate_hours']:.2f}h, Thực tế: {project_data['total_actual_hours']:.2f}h\n")
                    if project_data['total_saved_hours'] > 0:
                        f.write(f"   💎 Tiết kiệm: {project_data['total_saved_hours']:.2f}h ({project_saved_percentage:.1f}%)\n")
                    f.write("\n")
            else:
                f.write("   Không có dữ liệu dự án.\n\n")
            
            # Chi tiết các task có logwork và tiết kiệm thời gian
            f.write("\n" + "=" * 80 + "\n")
            f.write(f"{'💎 DANH SÁCH TASK TIẾT KIỆM THỜI GIAN':^80}\n")
            f.write("=" * 80 + "\n")
            if tasks_with_saving:
                for idx, task in enumerate(sorted(tasks_with_saving, key=lambda x: x.get('time_saved_hours', 0), reverse=True), 1):
                    time_saved = task.get('time_saved_hours', 0)
                    f.write(f"{idx}. [{task.get('key', '')}] {task.get('summary', '')}\n")
                    f.write(f"   🏷️ Trạng thái: {task.get('status', '')}, Loại: {task.get('type', '')}\n")
                    f.write(f"   📂 Dự án: {task.get('project', '')} - {task.get('project_name', '')}\n")
                    f.write(f"   🔖 Component: {task.get('component_str', 'Không có component')}\n")
                    f.write(f"   📌 Dự án thực tế: {task.get('actual_project', task.get('project', ''))}\n")
                    f.write(f"   ⏱️ Dự kiến: {task.get('original_estimate_hours', 0):.2f}h, Thực tế: {task.get('total_hours', 0):.2f}h\n")
                    f.write(f"   💰 Tiết kiệm: {time_saved:.2f}h ({task.get('time_saved_percent', 0):.1f}%)\n")
                    f.write(f"   🔗 Link: {task.get('link', '')}\n\n")
            else:
                f.write("   Không có task nào tiết kiệm thời gian.\n\n")
            
            # Ngăn cách
            f.write("\n" + "-" * 80 + "\n\n")
            
            # Chi tiết các task không có logwork
            f.write("⏳ DANH SÁCH TASK CHƯA CÓ LOGWORK\n")
            f.write("=" * 80 + "\n")
            if tasks_no_logwork:
                for idx, task in enumerate(sorted(tasks_no_logwork, key=lambda x: x.get('original_estimate_hours', 0), reverse=True), 1):
                    f.write(f"{idx}. [{task.get('key', '')}] {task.get('summary', '')}\n")
                    f.write(f"   🏷️ Trạng thái: {task.get('status', '')}, Loại: {task.get('type', '')}\n")
                    f.write(f"   📂 Dự án: {task.get('project', '')} - {task.get('project_name', '')}\n")
                    f.write(f"   🔖 Component: {task.get('component_str', 'Không có component')}\n")
                    f.write(f"   📌 Dự án thực tế: {task.get('actual_project', task.get('project', ''))}\n")
                    if task.get('original_estimate_hours', 0) > 0:
                        f.write(f"   ⏱️ Thời gian ước tính: {task.get('original_estimate_hours', 0):.2f}h\n")
                    else:
                        f.write(f"   ⚠️ Chưa có ước tính thời gian\n")
                    f.write(f"   🔗 Link: {task.get('link', '')}\n\n")
            else:
                f.write("   Không có task nào chưa có logwork.\n\n")
            
            # Ngăn cách
            f.write("\n" + "-" * 80 + "\n\n")
            
            # Chi tiết các task có logwork nhưng không tiết kiệm
            f.write("⚖️ DANH SÁCH TASK CÓ LOGWORK NHƯNG KHÔNG TIẾT KIỆM\n")
            f.write("=" * 80 + "\n")
            if tasks_no_saving:
                for idx, task in enumerate(sorted(tasks_no_saving, key=lambda x: x.get('original_estimate_hours', 0), reverse=True), 1):
                    f.write(f"{idx}. [{task.get('key', '')}] {task.get('summary', '')}\n")
                    f.write(f"   🏷️ Trạng thái: {task.get('status', '')}, Loại: {task.get('type', '')}\n")
                    f.write(f"   📂 Dự án: {task.get('project', '')} - {task.get('project_name', '')}\n")
                    f.write(f"   🔖 Component: {task.get('component_str', 'Không có component')}\n")
                    f.write(f"   📌 Dự án thực tế: {task.get('actual_project', task.get('project', ''))}\n")
                    f.write(f"   ⏱️ Dự kiến: {task.get('original_estimate_hours', 0):.2f}h, Thực tế: {task.get('total_hours', 0):.2f}h\n")
                    f.write(f"   ℹ️ Sử dụng đúng thời gian ước tính\n")
                    f.write(f"   🔗 Link: {task.get('link', '')}\n\n")
            else:
                f.write("   Không có task nào có logwork và estimate nhưng không tiết kiệm.\n\n")
                
            # Ngăn cách
            f.write("\n" + "-" * 80 + "\n\n")
            
            # Chi tiết các task có logwork nhưng không có estimate
            f.write("⚡ DANH SÁCH TASK CÓ LOGWORK NHƯNG KHÔNG CÓ ESTIMATE\n")
            f.write("=" * 80 + "\n")
            if tasks_no_estimate:
                for idx, task in enumerate(sorted(tasks_no_estimate, key=lambda x: x.get('total_hours', 0), reverse=True), 1):
                    f.write(f"{idx}. [{task.get('key', '')}] {task.get('summary', '')}\n")
                    f.write(f"   🏷️ Trạng thái: {task.get('status', '')}, Loại: {task.get('type', '')}\n")
                    f.write(f"   📂 Dự án: {task.get('project', '')} - {task.get('project_name', '')}\n")
                    f.write(f"   🔖 Component: {task.get('component_str', 'Không có component')}\n")
                    f.write(f"   📌 Dự án thực tế: {task.get('actual_project', task.get('project', ''))}\n")
                    f.write(f"   ⏱️ Thời gian log: {task.get('total_hours', 0):.2f}h (không có estimate)\n")
                    f.write(f"   ⚠️ Task này không có estimate nên không thể tính tiết kiệm\n")
                    f.write(f"   🔗 Link: {task.get('link', '')}\n\n")
            else:
                f.write("   Không có task nào có logwork nhưng thiếu estimate.\n\n")
            
            # Ngăn cách
            f.write("\n" + "-" * 80 + "\n\n")
            
            # Chi tiết các task vượt thời gian
            f.write("⚠️ DANH SÁCH TASK VƯỢT THỜI GIAN DỰ KIẾN\n")
            f.write("=" * 80 + "\n")
            if tasks_exceed_time:
                for idx, task in enumerate(sorted(tasks_exceed_time, key=lambda x: x.get('time_saved_hours', 0)), 1):
                    time_exceed = abs(task.get('time_saved_hours', 0))
                    f.write(f"{idx}. [{task.get('key', '')}] {task.get('summary', '')}\n")
                    f.write(f"   🏷️ Trạng thái: {task.get('status', '')}, Loại: {task.get('type', '')}\n")
                    f.write(f"   📂 Dự án: {task.get('project', '')} - {task.get('project_name', '')}\n")
                    f.write(f"   🔖 Component: {task.get('component_str', 'Không có component')}\n")
                    f.write(f"   📌 Dự án thực tế: {task.get('actual_project', task.get('project', ''))}\n")
                    f.write(f"   ⏱️ Dự kiến: {task.get('original_estimate_hours', 0):.2f}h, Thực tế: {task.get('total_hours', 0):.2f}h\n")
                    f.write(f"   ⚠️ Vượt: {time_exceed:.2f}h ({abs(task.get('time_saved_percent', 0)):.1f}%)\n")
                    f.write(f"   🔗 Link: {task.get('link', '')}\n\n")
            else:
                f.write("   Không có task nào vượt thời gian dự kiến.\n\n")
            
            # Footer
            f.write("\n" + "=" * 80 + "\n")
            f.write(f"{'KẾT THÚC BÁO CÁO':^80}\n")
            f.write("=" * 80 + "\n")
            
            # Thêm phần hiển thị danh sách task nếu cần
            f.write("\n📝 DANH SÁCH TASK CHI TIẾT\n")
            f.write("-" * 80 + "\n")
            
            for idx, task in enumerate(sorted(tasks, key=lambda x: x.get('key', '')), 1):
                key = task.get('key', '')
                summary = task.get('summary', '')
                status = task.get('status', '')
                updated = task.get('updated', '')
                has_worklog = "✓" if task.get('has_worklog', False) else "✗"
                
                f.write(f"{idx}. [{key}] {summary}\n")
                f.write(f"   Trạng thái: {status}, Cập nhật: {updated}, Logwork: {has_worklog}\n")
                f.write(f"   Link: {task.get('link', '')}\n")
                
                # Hiển thị chi tiết các log work
                worklogs = task.get('worklogs', [])
                if worklogs:
                    f.write(f"   Log work: {len(worklogs)} lần | Tổng: {task.get('total_hours', 0):.2f}h\n")
                    for log_idx, log in enumerate(sorted(worklogs, key=lambda x: x.get('started', '')), 1):
                        author = log.get('author', 'Unknown')
                        time_spent = log.get('time_spent', '')
                        hours = log.get('hours_spent', 0)
                        started = log.get('started', '')
                        comment = log.get('comment', 'Không có comment')
                        
                        # Rút gọn comment nếu quá dài
                        if len(comment) > 100:
                            comment = comment[:100] + "..."
                            
                        f.write(f"     {log_idx}. {author} - {started} - {hours:.2f}h\n")
                        if comment:
                            f.write(f"        {comment}\n")
                else:
                    f.write("   ⚠️ Chưa có log work nào!\n")
                
                # Chi tiết estimate và tiết kiệm
                est_hours = task.get('original_estimate_hours', 0)
                actual_hours = task.get('total_hours', 0)
                time_saved = task.get('time_saved_hours', -1)
                
                if est_hours > 0:
                    f.write(f"   Estimate: {est_hours:.2f}h | Actual: {actual_hours:.2f}h")
                    if time_saved > 0:
                        saved_percent = task.get('time_saved_percent', 0)
                        f.write(f" | Saved: {time_saved:.2f}h ({saved_percent:.1f}%)")
                    elif time_saved == 0:
                        f.write(" | No time saved")
                    elif time_saved < 0 and time_saved != -1 and time_saved != -2:
                        f.write(f" | ⚠️ Exceeded: {abs(time_saved):.2f}h")
                    f.write("\n")
                elif time_saved == -2:
                    f.write(f"   ℹ️ Đã log work {actual_hours:.2f}h nhưng không có estimate\n")
                
                # Thêm dòng trống giữa các task
                f.write("\n")
            
            print(f"✅ Đã tạo báo cáo chi tiết cho {employee_name}: {output_file}")
            return True
    except Exception as e:
        print(f"   ❌ Lỗi khi tạo báo cáo chi tiết: {str(e)}")
        return False

def create_project_report(project_name, tasks, employee_detailed_stats, output_file):
    """
    Tạo báo cáo chi tiết về một dự án và lưu vào file txt
    
    Args:
        project_name (str): Tên dự án
        tasks (list): Danh sách các task của dự án
        employee_detailed_stats (dict): Thống kê chi tiết của các nhân viên
        output_file (str): Đường dẫn đến file báo cáo
    """
    try:
         # Cập nhật trạng thái logwork cho task cha trước khi tạo báo cáo
        parent_to_children = {}
        for task in tasks:
            if task.get('is_subtask') and task.get('parent_key'):
                parent_key = task.get('parent_key')
                if parent_key not in parent_to_children:
                    parent_to_children[parent_key] = []
                parent_to_children[parent_key].append(task)
        
        # Cập nhật trạng thái task cha dựa trên task con
        for task in tasks:
            task_key = task.get('key')
            if task_key in parent_to_children:  # Nếu task này là task cha có con
                children = parent_to_children[task_key]
                
                # Nếu task cha không có estimate nhưng các task con có estimate
                if task.get('original_estimate_hours', 0) == 0:
                    total_child_estimate = sum(child.get('original_estimate_hours', 0) for child in children)
                    if total_child_estimate > 0:
                        # Cập nhật estimate cho task cha từ tổng estimate của các task con
                        task['original_estimate_hours'] = total_child_estimate
                        task['has_estimate'] = True
                        print(f"   ℹ️ Cập nhật estimate cho task cha {task_key} từ tổng estimate của các task con: {total_child_estimate:.2f}h")
                
                # Kiểm tra và cập nhật trạng thái logwork
                if not task.get('has_worklog'):  # Nếu task cha chưa có log work
                    children_with_logwork = [child for child in children if child.get('has_worklog', False)]
                    if children_with_logwork:  # Nếu có ít nhất một task con đã log work
                        # Đánh dấu task cha là đã log work
                        task['has_worklog'] = True
                        task['has_child_with_logwork'] = True  # Thêm trường để đánh dấu
                        
                        # Quan trọng: Cập nhật time_saved_hours nếu đang là -1 (không có logwork)
                        if task.get('time_saved_hours', -1) == -1:
                            # Tính tổng thời gian từ các task con
                            total_child_time = sum(child.get('total_hours', 0) for child in children_with_logwork)
                            
                            # Cập nhật thời gian thực tế cho task cha từ tổng thời gian của các task con
                            task['total_hours'] = total_child_time
                            
                            # Nếu task cha không có estimate nhưng các task con có estimate
                            if task.get('original_estimate_hours', 0) == 0:
                                # Tính tổng estimate từ task con
                                total_child_estimate = sum(child.get('original_estimate_hours', 0) for child in children)
                                if total_child_estimate > 0:
                                    # Cập nhật estimate cho task cha
                                    task['original_estimate_hours'] = total_child_estimate
                                    task['has_estimate'] = True
                                    print(f"   ℹ️ Cập nhật estimate cho task cha {task_key} từ tổng estimate của các task con: {total_child_estimate:.2f}h")
                            
                            # Sau đó tính time_saved_hours
                            if task.get('original_estimate_hours', 0) > 0:
                                saved_hours, saving_ratio = calculate_saved_time(task.get('original_estimate_hours', 0), total_child_time)
                                task['time_saved_hours'] = saved_hours
                                task['time_saved_percent'] = saving_ratio
                                print(f"   ℹ️ Cập nhật time_saved_hours cho task cha {task_key} từ task con: {saved_hours:.2f}h ({saving_ratio:.1f}%)")
                            else:
                                # Nếu thực sự không có estimate nào (cả cha và con đều không có)
                                task['time_saved_hours'] = -2  # Đánh dấu đặc biệt: có logwork nhưng không có estimate
                                print(f"   ℹ️ Task cha {task_key} đã được đánh dấu có logwork (từ task con) nhưng không có estimate")
        # Bỏ qua dự án FC
        if project_name == "FC":
            print(f"🚫 Bỏ qua tạo báo cáo cho dự án FC")
            return True
            
        # Lọc task thuộc dự án
        project_tasks = [task for task in tasks if task.get('actual_project', '') == project_name]
        
        if not project_tasks:
            print(f"⚠️ Không tìm thấy task nào thuộc dự án {project_name}")
            return False
            
        # Tạo từ điển để lưu thông tin của từng nhân viên
        employees = {}
        
        # Tạo từ điển ánh xạ từ task cha đến danh sách các task con
        parent_to_children = {}
        
        # Xác định mối quan hệ cha-con giữa các task
        for task in project_tasks:
            # Nếu là task con, thêm vào danh sách con của task cha
            if task.get('is_subtask') and task.get('parent_key'):
                parent_key = task.get('parent_key')
                if parent_key not in parent_to_children:
                    parent_to_children[parent_key] = []
                parent_to_children[parent_key].append(task)
        
        # Cập nhật trạng thái log work của task cha dựa trên con
        for task in project_tasks:
            # Nếu task là task cha (không phải là subtask) và có các task con
            task_key = task.get('key')
            if not task.get('is_subtask') and task_key in parent_to_children:
                # Kiểm tra xem có task con nào đã log work không
                if not task.get('has_worklog'):  # Nếu task cha chưa có log work
                    children_with_logwork = [child for child in parent_to_children[task_key] if child.get('has_worklog', False)]
                    if children_with_logwork:  # Nếu có ít nhất một task con đã log work
                        # Đánh dấu task cha là đã log work
                        task['has_worklog'] = True
                        task['has_child_with_logwork'] = True  # Thêm trường để đánh dấu
                        
                        # Quan trọng: Cập nhật time_saved_hours nếu đang là -1 (không có logwork)
                        if task.get('time_saved_hours', -1) == -1:
                            # Tính tổng thời gian thực tế từ các task con
                            children_total_hours = sum(child.get('total_hours', 0) for child in children_with_logwork)
                            
                            # Cập nhật thời gian thực tế cho task cha
                            if task.get('total_hours', 0) == 0:  # Chỉ cập nhật nếu task cha chưa có giá trị
                                task['total_hours'] = children_total_hours
                            
                            # Nếu task cha có estimate, tính time_saved_hours
                            if task.get('original_estimate_hours', 0) > 0:
                                task['time_saved_hours'] = task.get('original_estimate_hours', 0) - task.get('total_hours', 0)
                            else:
                                # Nếu không có estimate, đặt thành 0 (không tiết kiệm)
                                task['time_saved_hours'] = 0
        
        # Xử lý từng nhân viên
        for task in project_tasks:
            employee_name = task.get('employee_name', 'Unknown')
            employee_email = task.get('employee_email', '')
            
            if employee_name not in employees:
                employees[employee_name] = {
                    'email': employee_email,
                    'total_tasks': 0,
                    'tasks_with_logwork': 0,
                    'tasks_without_logwork': 0,
                    'estimated_hours': 0,
                    'actual_hours': 0,
                    'saved_hours': 0
                }
                
            # Cập nhật thống kê nhân viên
            employees[employee_name]['total_tasks'] += 1
            employees[employee_name]['estimated_hours'] += task.get('original_estimate_hours', 0) or 0
            employees[employee_name]['actual_hours'] += task.get('total_hours', 0) or 0
            
            if task.get('has_worklog', False):
                employees[employee_name]['tasks_with_logwork'] += 1
            else:
                employees[employee_name]['tasks_without_logwork'] += 1
            
            # Tính thời gian tiết kiệm
            time_saved = task.get('time_saved_hours', 0)
            if time_saved > 0:
                employees[employee_name]['saved_hours'] += time_saved
        
        # Tính tỷ lệ không logwork và tỷ lệ tiết kiệm
        for name, stats in employees.items():
            if stats['total_tasks'] > 0:
                stats['no_logwork_ratio'] = stats['tasks_without_logwork'] / stats['total_tasks'] * 100
            else:
                stats['no_logwork_ratio'] = 0
                
            # Tính lại thời gian tiết kiệm: tổng ước tính - tổng thực tế
            #stats['saved_hours'] = stats['estimated_hours'] - stats['actual_hours']
                
            if stats['estimated_hours'] > 0:
                stats['saving_ratio'] = stats['saved_hours'] / stats['estimated_hours'] * 100
            else:
                stats['saving_ratio'] = 0
                
        # Tính các chỉ số tổng hợp của dự án
        project_stats = {
            'total_tasks': len(project_tasks),
            'total_employees': len(employees),
            'total_estimated_hours': sum(task.get('original_estimate_hours', 0) or 0 for task in project_tasks),
            'total_actual_hours': sum(task.get('total_hours', 0) or 0 for task in project_tasks),
            'tasks_with_logwork': len([task for task in project_tasks if task.get('has_worklog', False)]),
            'tasks_without_logwork': len([task for task in project_tasks if not task.get('has_worklog', False)]),
        }
        
        project_stats['logwork_ratio'] = (project_stats['tasks_with_logwork'] / project_stats['total_tasks'] * 100) if project_stats['total_tasks'] > 0 else 0
        project_stats['saved_hours'] = project_stats['total_estimated_hours'] - project_stats['total_actual_hours']
        project_stats['saving_ratio'] = (project_stats['saved_hours'] / project_stats['total_estimated_hours'] * 100) if project_stats['total_estimated_hours'] > 0 else 0
        
        # Top 10 nhân viên không logwork
        top_no_logwork = sorted(
            [item for item in employees.items() if item[1]['tasks_without_logwork'] > 0],
            key=lambda x: x[1]['tasks_without_logwork'],
            reverse=True
        )[:10]
        
        # Top 10 nhân viên logwork nhiều nhất
        top_logwork = sorted(
            employees.items(),
            key=lambda x: x[1]['actual_hours'],
            reverse=True
        )[:10]
        
        # Top 10 nhân viên tiết kiệm thời gian nhiều nhất
        top_saving = sorted(
            [item for item in employees.items() if item[1]['estimated_hours'] > 0],
            key=lambda x: x[1]['saving_ratio'],
            reverse=True
        )[:10]
        
        # Top 10 nhân viên có tỷ lệ không logwork cao nhất
        top_no_logwork_ratio = sorted(
            [item for item in employees.items() if item[1]['tasks_without_logwork'] > 0],
            key=lambda x: x[1]['no_logwork_ratio'],
            reverse=True
        )[:10]
        
        # Tạo báo cáo
        with open(output_file, 'w', encoding='utf-8') as f:
            # Tiêu đề
            f.write(f"=== BÁO CÁO DỰ ÁN: {project_name} ===\n\n")
            
            # Thống kê tổng quan
            f.write("📊 THỐNG KÊ TỔNG QUAN:\n")
            f.write(f"- Tổng số task: {project_stats['total_tasks']}\n")
            f.write(f"- Số nhân viên: {project_stats['total_employees']}\n")
            f.write(f"- Số task có logwork: {project_stats['tasks_with_logwork']} ({project_stats['logwork_ratio']:.1f}%)\n")
            f.write(f"- Tổng thời gian ước tính: {project_stats['total_estimated_hours']:.1f} giờ\n")
            f.write(f"- Tổng thời gian thực tế: {project_stats['total_actual_hours']:.1f} giờ\n")
            f.write(f"- Thời gian tiết kiệm: {project_stats['saved_hours']:.1f} giờ ({project_stats['saving_ratio']:.1f}%)\n\n")
            
            # Danh sách nhân viên
            f.write("👥 DANH SÁCH NHÂN VIÊN TRONG DỰ ÁN:\n")
            for idx, (name, stats) in enumerate(sorted(employees.items(), key=lambda x: x[0]), 1):
                f.write(f"{idx}. {name} ({stats['email']}) - {stats['total_tasks']} task\n")
            f.write("\n")
            
            # Top 10 nhân viên không logwork
            f.write("⚠️ TOP ")
            f.write(f"{len(top_no_logwork)}" if top_no_logwork else "0")
            f.write(" NHÂN VIÊN CÓ NHIỀU TASK KHÔNG LOGWORK:\n")
            if top_no_logwork:
                header = f"{'STT':<5}{'Tên nhân viên':<30}{'Tổng task':<15}{'Không logwork':<15}{'Tỷ lệ':<10}\n"
                f.write(header)
                f.write("-" * 75 + "\n")
                
                for idx, (name, stats) in enumerate(top_no_logwork, 1):
                    row = f"{idx:<5}{name[:28]:<30}{stats['total_tasks']:<15}{stats['tasks_without_logwork']:<15}{stats['no_logwork_ratio']:.1f}%\n"
                    f.write(row)
            else:
                f.write("Không có nhân viên nào có task không logwork\n")
            f.write("\n")
            
            # Top 10 nhân viên có tỷ lệ không logwork cao nhất
            f.write("🚫 TOP ")
            f.write(f"{len(top_no_logwork_ratio)}" if top_no_logwork_ratio else "0")
            f.write(" NHÂN VIÊN CÓ TỶ LỆ KHÔNG LOGWORK CAO NHẤT:\n")
            if top_no_logwork_ratio:
                header = f"{'STT':<5}{'Tên nhân viên':<30}{'Tổng task':<15}{'Không logwork':<15}{'Tỷ lệ':<10}\n"
                f.write(header)
                f.write("-" * 75 + "\n")
                
                for idx, (name, stats) in enumerate(top_no_logwork_ratio, 1):
                    row = f"{idx:<5}{name[:28]:<30}{stats['total_tasks']:<15}{stats['tasks_without_logwork']:<15}{stats['no_logwork_ratio']:.1f}%\n"
                    f.write(row)
            else:
                f.write("Không có nhân viên nào có task không logwork\n")
            f.write("\n")
            
            # Top 10 nhân viên logwork nhiều nhất
            f.write("🔝 TOP 10 NHÂN VIÊN LOGWORK NHIỀU NHẤT:\n")
            if top_logwork:
                header = f"{'STT':<5}{'Tên nhân viên':<30}{'Tổng task':<15}{'Số giờ logwork':<20}\n"
                f.write(header)
                f.write("-" * 70 + "\n")
                
                for idx, (name, stats) in enumerate(top_logwork, 1):
                    row = f"{idx:<5}{name[:28]:<30}{stats['total_tasks']:<15}{stats['actual_hours']:.1f} giờ\n"
                    f.write(row)
            else:
                f.write("Không có dữ liệu\n")
            f.write("\n")
            
            # Top 10 nhân viên tiết kiệm thời gian
            f.write("💰 TOP 10 NHÂN VIÊN TIẾT KIỆM THỜI GIAN NHẤT:\n")
            if top_saving:
                header = f"{'STT':<5}{'Tên nhân viên':<30}{'Ước tính':<15}{'Thực tế':<15}{'Tiết kiệm':<15}{'Tỷ lệ':<10}\n"
                f.write(header)
                f.write("-" * 90 + "\n")
                
                for idx, (name, stats) in enumerate(top_saving, 1):
                    row = f"{idx:<5}{name[:28]:<30}{stats['estimated_hours']:.1f}h{' ':<10}{stats['actual_hours']:.1f}h{' ':<10}{stats['saved_hours']:.1f}h{' ':<10}{stats['saving_ratio']:.1f}%\n"
                    f.write(row)
            else:
                f.write("Không có dữ liệu\n")
            f.write("\n")
            
            # Chi tiết từng nhân viên
            f.write("📝 CHI TIẾT TỪNG NHÂN VIÊN:\n\n")
            
            for employee_name, stats in sorted(employees.items(), key=lambda x: x[0]):
                f.write(f"👤 {employee_name} ({stats['email']}):\n")
                f.write(f"- Tổng số task: {stats['total_tasks']}\n")
                f.write(f"- Số task có logwork: {stats['tasks_with_logwork']} ({(stats['tasks_with_logwork']/stats['total_tasks']*100) if stats['total_tasks'] > 0 else 0:.1f}%)\n")
                f.write(f"- Số task không logwork: {stats['tasks_without_logwork']}\n")
                f.write(f"- Thời gian ước tính: {stats['estimated_hours']:.1f} giờ\n")
                f.write(f"- Thời gian thực tế: {stats['actual_hours']:.1f} giờ\n")
                f.write(f"- Thời gian tiết kiệm: {stats['saved_hours']:.1f} giờ ({stats['saving_ratio']:.1f}%)\n")
                
                # Chi tiết các task của nhân viên
                employee_tasks = [task for task in project_tasks if task.get('employee_name', '') == employee_name]
                
                if employee_tasks:
                    f.write("\n   DANH SÁCH TASK:\n")
                    
                    for idx, task in enumerate(sorted(employee_tasks, key=lambda x: x.get('key', '')), 1):
                        key = task.get('key', '')
                        summary = task.get('summary', '')[:50] + ('...' if len(task.get('summary', '')) > 50 else '')
                        status = task.get('status', '')
                        est_hours = task.get('original_estimate_hours', 0) or 0
                        actual_hours = task.get('total_hours', 0) or 0
                        has_logwork = "✓" if task.get('has_worklog', False) else "✗"
                        
                        f.write(f"   {idx}. [{key}] {summary} - Trạng thái: {status}\n")
                        
                        # Hiển thị thông tin nếu task cha có log work thông qua task con
                        if task.get('has_child_with_logwork', False):
                            f.write(f"      Logwork: {has_logwork} (✓ qua task con), Ước tính: {est_hours:.1f}h, Thực tế: {actual_hours:.1f}h\n")
                            # Hiển thị danh sách task con có log work
                            if key in parent_to_children:
                                children_with_logwork = [child for child in parent_to_children[key] if child.get('has_worklog', False)]
                                f.write(f"      👉 Có {len(children_with_logwork)}/{len(parent_to_children[key])} task con đã log work:\n")
                                for idx_child, child in enumerate(children_with_logwork, 1):
                                    child_key = child.get('key', '')
                                    child_summary = child.get('summary', '')[:40] + ('...' if len(child.get('summary', '')) > 40 else '')
                                    child_hours = child.get('total_hours', 0) or 0
                                    f.write(f"        {idx_child}. [{child_key}] {child_summary} - {child_hours:.1f}h\n")
                        else:
                            f.write(f"      Logwork: {has_logwork}, Ước tính: {est_hours:.1f}h, Thực tế: {actual_hours:.1f}h\n")
                        
                        # Hiển thị chi tiết từng lần logwork nếu có
                        worklogs = task.get('worklogs', [])
                        if worklogs:
                            f.write(f"      Chi tiết logwork ({len(worklogs)} lần):\n")
                            for log_idx, log in enumerate(sorted(worklogs, key=lambda x: x.get('started', '')), 1):
                                author = log.get('author', 'Unknown')
                                started = log.get('started', 'Unknown')
                                hours = log.get('hours_spent', 0)
                                comment = log.get('comment', 'Không có comment')
                                comment_display = comment[:100] + '...' if len(comment) > 100 else comment
                                
                                f.write(f"        {log_idx}. {author} - {started} - {hours:.1f}h\n")
                                f.write(f"           Comment: {comment_display}\n")
                
                f.write("\n" + "-" * 80 + "\n\n")
        
        print(f"✅ Đã tạo báo cáo dự án {project_name}: {output_file}")
        return True
        
    except Exception as e:
        print(f"❌ Lỗi khi tạo báo cáo dự án {project_name}: {str(e)}")
        return False

def create_projects_summary_report(all_tasks, output_file, csv_output_file):
    """
    Tạo báo cáo tổng hợp cho tất cả các dự án thực tế
    
    Args:
        all_tasks (list): Danh sách tất cả các task
        output_file (str): Đường dẫn đến file báo cáo tổng hợp dạng txt
        csv_output_file (str): Đường dẫn đến file báo cáo tổng hợp dạng csv
        
    Returns:
        bool: True nếu thành công, False nếu thất bại
    """
    try:
        # Tạo từ điển để lưu thông tin của từng dự án
        projects = {}
        
        # Tạo từ điển để lưu thông tin nhân viên theo dự án
        project_employees = {}
        
        # Tạo từ điển để lưu thông tin nhân viên tổng hợp
        all_employees = {}
        
        # Tạo từ điển ánh xạ từ task cha đến danh sách các task con
        parent_to_children = {}
        
        # Xác định mối quan hệ cha-con giữa các task
        for task in all_tasks:
            # Nếu là task con, thêm vào danh sách con của task cha
            if task.get('is_subtask') and task.get('parent_key'):
                parent_key = task.get('parent_key')
                if parent_key not in parent_to_children:
                    parent_to_children[parent_key] = []
                parent_to_children[parent_key].append(task)
        
        # Cập nhật trạng thái log work của task cha dựa trên con
        for task in all_tasks:
            # Nếu task là task cha (không phải là subtask) và có các task con
            task_key = task.get('key')
            if not task.get('is_subtask') and task_key in parent_to_children:
                # Kiểm tra xem có task con nào đã log work không
                if not task.get('has_worklog'):  # Nếu task cha chưa có log work
                    children_with_logwork = [child for child in parent_to_children[task_key] if child.get('has_worklog', False)]
                    if children_with_logwork:  # Nếu có ít nhất một task con đã log work
                        # Đánh dấu task cha là đã log work
                        task['has_worklog'] = True
                        task['has_child_with_logwork'] = True  # Thêm trường để đánh dấu
                        
                        # Quan trọng: Cập nhật time_saved_hours nếu đang là -1 (không có logwork)
                        if task.get('time_saved_hours', -1) == -1:
                            # Tính tổng thời gian thực tế từ các task con
                            children_total_hours = sum(child.get('total_hours', 0) for child in children_with_logwork)
                            
                            # Cập nhật thời gian thực tế cho task cha
                            if task.get('total_hours', 0) == 0:  # Chỉ cập nhật nếu task cha chưa có giá trị
                                task['total_hours'] = children_total_hours
                            
                            # Nếu task cha không có estimate nhưng các task con có estimate
                            if task.get('original_estimate_hours', 0) == 0:
                                # Tính tổng estimate từ task con
                                total_child_estimate = sum(child.get('original_estimate_hours', 0) for child in parent_to_children[task_key])
                                if total_child_estimate > 0:
                                    # Cập nhật estimate cho task cha
                                    task['original_estimate_hours'] = total_child_estimate
                                    task['has_estimate'] = True

                            # Sau đó tính time_saved_hours
                            if task.get('original_estimate_hours', 0) > 0:
                                task['time_saved_hours'] = task.get('original_estimate_hours', 0) - task.get('total_hours', 0)
                            else:
                                # Nếu thực sự không có estimate nào (cả cha và con đều không có)
                                task['time_saved_hours'] = -2  # Đánh dấu đặc biệt: có logwork nhưng không có estimate
        
        # Xử lý từng task để thu thập thông tin
        for task in all_tasks:
            project_name = task.get('actual_project', task.get('project', 'Unknown'))
            
            # Bỏ qua các dự án không mong muốn trong báo cáo tổng hợp
            if project_name in ["FC", "IMS"]:
                continue
                
            # DEBUG: Kiểm tra PKT và WAK có được gán đúng actual_project không
            # if task.get('project') == 'PKT' and project_name != '[Project] Kho Tổng + PIM':
            #     print(f"🔍 DEBUG: Task {task.get('key')} từ PKT có actual_project = '{project_name}' thay vì '[Project] Kho Tổng + PIM'!")
            
            # if task.get('project') == 'WAK' and project_name != 'Web App KHLC':
            #     print(f"🔍 DEBUG: Task {task.get('key')} từ WAK có actual_project = '{project_name}' thay vì 'Web App KHLC'!")
                
            # DEBUG: Cảnh báo nếu PKT hoặc WAK vẫn xuất hiện như tên dự án
            # if project_name in ['PKT', 'WAK']:
            #     print(f"🚨 CẢNH BÁO: Task {task.get('key')} có actual_project = '{project_name}' - logic get_actual_project() KHÔNG hoạt động!")
                
            employee_name = task.get('employee_name', 'Unknown')
            employee_email = task.get('employee_email', '')
            has_worklog = task.get('has_worklog', False)
            estimated_hours = task.get('original_estimate_hours', 0) or 0
            actual_hours = task.get('total_hours', 0) or 0
            time_saved = task.get('time_saved_hours', 0)
            
            # Cập nhật thông tin dự án
            if project_name not in projects:
                projects[project_name] = {
                    'total_tasks': 0,
                    'tasks_with_worklog': 0,
                    'tasks_without_worklog': 0,
                    'estimated_hours': 0,
                    'actual_hours': 0,
                    'saved_hours': 0,
                    'employee_set': set(),
                    'employees_with_worklog': set(),
                    'employees_without_worklog': set(),
                    'employee_task_status': {}  # Thêm từ điển để theo dõi trạng thái log work của nhân viên
                }
            
            projects[project_name]['total_tasks'] += 1
            projects[project_name]['employee_set'].add(employee_name)
            
            # Khởi tạo trạng thái log work của nhân viên nếu chưa có
            if employee_name not in projects[project_name]['employee_task_status']:
                projects[project_name]['employee_task_status'][employee_name] = {'has_log': False, 'no_log': False}
            
            if has_worklog:
                projects[project_name]['tasks_with_worklog'] += 1
                projects[project_name]['estimated_hours'] += estimated_hours
                projects[project_name]['actual_hours'] += actual_hours
                
                # Cập nhật trạng thái log work của nhân viên
                projects[project_name]['employee_task_status'][employee_name]['has_log'] = True
                
                # Tính toán thời gian tiết kiệm cho những task có log work và có estimate
                if estimated_hours > 0 and time_saved != -1 and time_saved != -2:
                    # time_saved == -1 nghĩa là không có log work
                    # time_saved == -2 nghĩa là có log work nhưng không có estimate
                    if time_saved > 0:
                        projects[project_name]['saved_hours'] += time_saved
            else:
                projects[project_name]['tasks_without_worklog'] += 1
                # Cập nhật trạng thái log work của nhân viên
                projects[project_name]['employee_task_status'][employee_name]['no_log'] = True
            
            # Cập nhật thông tin nhân viên trong dự án
            if project_name not in project_employees:
                project_employees[project_name] = {}
            
            if employee_name not in project_employees[project_name]:
                project_employees[project_name][employee_name] = {
                    'email': employee_email,
                    'total_tasks': 0,
                    'tasks_with_worklog': 0,
                    'tasks_without_worklog': 0,
                    'estimated_hours': 0,
                    'actual_hours': 0,
                    'saved_hours': 0
                }
            
            project_employees[project_name][employee_name]['total_tasks'] += 1
            
            if has_worklog:
                project_employees[project_name][employee_name]['tasks_with_worklog'] += 1
                project_employees[project_name][employee_name]['estimated_hours'] += estimated_hours
                project_employees[project_name][employee_name]['actual_hours'] += actual_hours
                
                # Tính toán thời gian tiết kiệm
                if estimated_hours > 0 and time_saved != -1 and time_saved != -2:
                    if time_saved > 0:
                        project_employees[project_name][employee_name]['saved_hours'] += time_saved
            else:
                project_employees[project_name][employee_name]['tasks_without_worklog'] += 1
            
            # Cập nhật thông tin nhân viên tổng hợp
            if employee_name not in all_employees:
                all_employees[employee_name] = {
                    'email': employee_email,
                    'total_tasks': 0,
                    'tasks_with_worklog': 0,
                    'tasks_without_worklog': 0,
                    'estimated_hours': 0,
                    'actual_hours': 0,
                    'saved_hours': 0,
                    'projects': set()
                }
            
            all_employees[employee_name]['total_tasks'] += 1
            all_employees[employee_name]['projects'].add(project_name)
            
            if has_worklog:
                all_employees[employee_name]['tasks_with_worklog'] += 1
                all_employees[employee_name]['estimated_hours'] += estimated_hours
                all_employees[employee_name]['actual_hours'] += actual_hours
                
                # Tính toán thời gian tiết kiệm
                if estimated_hours > 0 and time_saved != -1 and time_saved != -2:
                    if time_saved > 0:
                        all_employees[employee_name]['saved_hours'] += time_saved
            else:
                all_employees[employee_name]['tasks_without_worklog'] += 1
        
        # Tính toán các chỉ số phái sinh cho dự án
        for project_name, stats in projects.items():
            if stats['total_tasks'] > 0:
                stats['logwork_ratio'] = (stats['tasks_with_worklog'] / stats['total_tasks']) * 100
            else:
                stats['logwork_ratio'] = 0
                
            # Tính lại thời gian tiết kiệm: tổng ước tính - tổng thực tế
            #stats['saved_hours'] = stats['estimated_hours'] - stats['actual_hours']
                
            if stats['estimated_hours'] > 0:
                stats['saving_ratio'] = (stats['saved_hours'] / stats['estimated_hours']) * 100
            else:
                stats['saving_ratio'] = 0
                
            # Cập nhật danh sách nhân viên dựa trên trạng thái log work
            stats['employees_with_worklog'] = set()
            stats['employees_without_worklog'] = set()
            
            for emp_name, emp_status in stats['employee_task_status'].items():
                if emp_status['has_log']:
                    stats['employees_with_worklog'].add(emp_name)
                if emp_status['no_log'] and not emp_status['has_log']:
                    stats['employees_without_worklog'].add(emp_name)
            
            stats['total_employees'] = len(stats['employee_set'])
            stats['employees_with_worklog_count'] = len(stats['employees_with_worklog'])
            stats['employees_without_worklog_count'] = len(stats['employees_without_worklog'])
        
        # Tính toán các chỉ số phái sinh cho nhân viên trong từng dự án
        for project_name, employees in project_employees.items():
            for employee_name, stats in employees.items():
                if stats['total_tasks'] > 0:
                    stats['logwork_ratio'] = (stats['tasks_with_worklog'] / stats['total_tasks']) * 100
                    stats['no_logwork_ratio'] = (stats['tasks_without_worklog'] / stats['total_tasks']) * 100
                else:
                    stats['logwork_ratio'] = 0
                    stats['no_logwork_ratio'] = 0
                    
                if stats['estimated_hours'] > 0:
                    stats['saving_ratio'] = (stats['saved_hours'] / stats['estimated_hours']) * 100
                else:
                    stats['saving_ratio'] = 0
        
        # Tính toán các chỉ số phái sinh cho nhân viên tổng hợp
        for employee_name, stats in all_employees.items():
            if stats['total_tasks'] > 0:
                stats['logwork_ratio'] = (stats['tasks_with_worklog'] / stats['total_tasks']) * 100
                stats['no_logwork_ratio'] = (stats['tasks_without_worklog'] / stats['total_tasks']) * 100
            else:
                stats['logwork_ratio'] = 0
                stats['no_logwork_ratio'] = 0
                
            if stats['estimated_hours'] > 0:
                stats['saving_ratio'] = (stats['saved_hours'] / stats['estimated_hours']) * 100
            else:
                stats['saving_ratio'] = 0
            
            stats['project_count'] = len(stats['projects'])
        
        # Tạo danh sách top 10
        # Top 10 nhân viên có tỷ lệ log work cao nhất
        top_logwork_ratio = sorted(
            [item for item in all_employees.items()],
            key=lambda x: x[1]['logwork_ratio'],
            reverse=True
        )[:10]
        
        # Thêm kiểm tra tính nhất quán giữa báo cáo dự án và báo cáo tổng hợp
        # Lưu thông tin để so sánh sau khi tạo báo cáo chi tiết
        project_stats_for_comparison = {}
        for project_name, stats in projects.items():
            project_stats_for_comparison[project_name] = {
                'estimated_hours': stats['estimated_hours'],
                'actual_hours': stats['actual_hours'],
                'saved_hours': stats['saved_hours']
            }
        
        # Top 10 nhân viên có thời gian tiết kiệm lớn nhất
        top_time_saving = sorted(
            all_employees.items(),
            key=lambda x: x[1]['saved_hours'],
            reverse=True
        )[:10]
        
        # Top 10 nhân viên không log work
        top_no_logwork = sorted(
            [item for item in all_employees.items() if item[1]['tasks_without_worklog'] > 0],
            key=lambda x: (x[1]['tasks_without_worklog'], -x[1]['total_tasks']),
            reverse=True
        )[:10]
        
        # Top 10 nhân viên có tỷ lệ không log work cao nhất
        top_no_logwork_ratio = sorted(
            [item for item in all_employees.items() if item[1].get('tasks_without_logwork', 0) > 0],
            key=lambda x: x[1].get('no_logwork_ratio', 0),
            reverse=True
        )[:10]
        
        # Tạo báo cáo tổng hợp
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("=== BÁO CÁO TỔNG HỢP CÁC DỰ ÁN ===\n\n")
            
            # Thống kê tổng quan
            # Loại bỏ dự án FC khỏi tổng số (dự phòng)
            filtered_projects = {name: stats for name, stats in projects.items() if name != "FC"}
            total_projects = len(filtered_projects)
            total_tasks = sum(stats['total_tasks'] for stats in filtered_projects.values())
            total_employees = len(all_employees)
            total_estimated_hours = sum(stats['estimated_hours'] for stats in filtered_projects.values())
            total_actual_hours = sum(stats['actual_hours'] for stats in filtered_projects.values())
            total_saved_hours = sum(stats['saved_hours'] for stats in filtered_projects.values())
            
            f.write("📊 THỐNG KÊ TỔNG QUAN:\n")
            f.write(f"- Tổng số dự án: {total_projects}\n")
            f.write(f"- Tổng số task: {total_tasks}\n")
            f.write(f"- Tổng số nhân viên: {total_employees}\n")
            f.write(f"- Tổng thời gian ước tính: {total_estimated_hours:.1f} giờ\n")
            f.write(f"- Tổng thời gian thực tế: {total_actual_hours:.1f} giờ\n")
            f.write(f"- Tổng thời gian tiết kiệm: {total_saved_hours:.1f} giờ ({(total_saved_hours/total_estimated_hours*100) if total_estimated_hours > 0 else 0:.1f}%)\n\n")
            
            # Bảng thống kê các dự án
            f.write("📋 THỐNG KÊ THEO DỰ ÁN:\n")
            header = "| {:<30} | {:>5} | {:>5} | {:>6} | {:>8} | {:>8} | {:>8} | {:>6} | {:>8} | {:>8} | {:>8} |\n".format(
                "Dự án", "Tasks", "Log", "%Log", "Est(h)", "Actual(h)", "Saved(h)", "%Save", "NV", "Log NV", "No Log"
            )
            separator = "|-{:-<30}-|-{:->5}-|-{:->5}-|-{:->6}-|-{:->8}-|-{:->8}-|-{:->8}-|-{:->6}-|-{:->8}-|-{:->8}-|-{:->8}-|\n".format(
                "", "", "", "", "", "", "", "", "", "", ""
            )
            
            f.write(separator)
            f.write(header)
            f.write(separator)
            
            # In dữ liệu từng dự án
            for project_name, stats in sorted(projects.items(), key=lambda x: x[1]['total_tasks'], reverse=True):
                # Bỏ qua dự án FC trong bảng thống kê
                if project_name == "FC":
                    continue
                    
                row = "| {:<30} | {:>5} | {:>5} | {:>6.1f} | {:>8.1f} | {:>8.1f} | {:>8.1f} | {:>6.1f} | {:>8} | {:>8} | {:>8} |\n".format(
                    project_name[:30],
                    stats['total_tasks'],
                    stats['tasks_with_worklog'],
                    stats['logwork_ratio'],
                stats['estimated_hours'], 
                    stats['actual_hours'],
                    stats['saved_hours'],
                    stats['saving_ratio'],
                    stats['total_employees'],
                    stats['employees_with_worklog_count'],
                    stats['employees_without_worklog_count']
                )
                f.write(row)
            
            f.write(separator)
            
            # Tổng cộng
            total_row = "| {:<30} | {:>5} | {:>5} | {:>6.1f} | {:>8.1f} | {:>8.1f} | {:>8.1f} | {:>6.1f} | {:>8} | {:>8} | {:>8} |\n".format(
                "TỔNG CỘNG",
                total_tasks,
                sum(stats['tasks_with_worklog'] for stats in filtered_projects.values()),
                (sum(stats['tasks_with_worklog'] for stats in filtered_projects.values()) / total_tasks * 100) if total_tasks > 0 else 0,
                total_estimated_hours,
                total_actual_hours,
                total_saved_hours,
                (total_saved_hours / total_estimated_hours * 100) if total_estimated_hours > 0 else 0,
                total_employees,
                len([e for e in all_employees.values() if e['tasks_with_worklog'] > 0]),
                len([e for e in all_employees.values() if e['tasks_with_worklog'] == 0])
            )
            f.write(total_row)
            f.write(separator)
            f.write("\n\n")
            
            # Top 10 nhân viên có tỷ lệ log work cao nhất
            f.write("🔝 TOP 10 NHÂN VIÊN CÓ TỶ LỆ LOG WORK CAO NHẤT:\n")
            if top_logwork_ratio:
                header = f"{'STT':<5}{'Tên nhân viên':<30}{'Số dự án':<10}{'Tổng task':<10}{'Có log':<10}{'Tỷ lệ log':<10}{'Thời gian':<10}\n"
                f.write(header)
                f.write("-" * 85 + "\n")
                
                for idx, (name, stats) in enumerate(top_logwork_ratio, 1):
                    row = f"{idx:<5}{name[:28]:<30}{stats['project_count']:<10}{stats['total_tasks']:<10}{stats['tasks_with_worklog']:<10}{stats['logwork_ratio']:.1f}%{' ':<5}{stats['actual_hours']:.1f}h\n"
                    f.write(row)
            else:
                f.write("Không có dữ liệu\n")
            f.write("\n")
            
            # Top 10 nhân viên có thời gian tiết kiệm lớn nhất
            f.write("💰 TOP 10 NHÂN VIÊN TIẾT KIỆM THỜI GIAN NHIỀU NHẤT:\n")
            if top_time_saving:
                header = f"{'STT':<5}{'Tên nhân viên':<30}{'Tổng task':<10}{'Ước tính':<10}{'Thực tế':<10}{'Tiết kiệm':<10}{'Tỷ lệ':<10}\n"
                f.write(header)
                f.write("-" * 85 + "\n")
                
                for idx, (name, stats) in enumerate(top_time_saving, 1):
                    row = f"{idx:<5}{name[:28]:<30}{stats['total_tasks']:<10}{stats['estimated_hours']:.1f}h{' ':<5}{stats['actual_hours']:.1f}h{' ':<5}{stats['saved_hours']:.1f}h{' ':<5}{stats['saving_ratio']:.1f}%\n"
                    f.write(row)
            else:
                f.write("Không có dữ liệu\n")
            f.write("\n")
            
            # Top 10 nhân viên không log work
            f.write("⚠️ TOP 10 NHÂN VIÊN CÓ NHIỀU TASK KHÔNG LOG WORK:\n")
            if top_no_logwork:
                header = f"{'STT':<5}{'Tên nhân viên':<30}{'Số dự án':<10}{'Tổng task':<10}{'Không log':<10}{'Tỷ lệ':<10}\n"
                f.write(header)
                f.write("-" * 75 + "\n")
                
                for idx, (name, stats) in enumerate(top_no_logwork, 1):
                    row = f"{idx:<5}{name[:28]:<30}{stats['project_count']:<10}{stats['total_tasks']:<10}{stats['tasks_without_worklog']:<10}{stats['no_logwork_ratio']:.1f}%\n"
                    f.write(row)
            else:
                f.write("Không có dữ liệu\n")
            f.write("\n")
            
            # Top 10 nhân viên có tỷ lệ không log work cao nhất
            f.write("🚫 TOP 10 NHÂN VIÊN CÓ TỶ LỆ KHÔNG LOG WORK CAO NHẤT:\n")
            if top_no_logwork_ratio:
                header = f"{'STT':<5}{'Tên nhân viên':<30}{'Số dự án':<10}{'Tổng task':<10}{'Không log':<10}{'Tỷ lệ':<10}\n"
                f.write(header)
                f.write("-" * 75 + "\n")
                
                for idx, (name, stats) in enumerate(top_no_logwork_ratio, 1):
                    row = f"{idx:<5}{name[:28]:<30}{stats['project_count']:<10}{stats['total_tasks']:<10}{stats['tasks_without_worklog']:<10}{stats['no_logwork_ratio']:.1f}%\n"
                    f.write(row)
            else:
                f.write("Không có dữ liệu\n")
        
        # Tạo báo cáo CSV
        with open(csv_output_file, 'w', encoding='utf-8', newline='') as f:
            # Header
            f.write("Project,Tasks,TasksWithLog,LogRatio,EstimatedHours,ActualHours,SavedHours,SavingRatio,Employees,EmployeesWithLog,EmployeesWithoutLog\n")
            
            # Dữ liệu từng dự án
            for project_name, stats in sorted(projects.items(), key=lambda x: x[1]['total_tasks'], reverse=True):
                # Bỏ qua dự án FC trong báo cáo CSV
                if project_name == "FC":
                    continue
                    
                row = f"{project_name.replace(',', ';')},{stats['total_tasks']},{stats['tasks_with_worklog']},{stats['logwork_ratio']:.1f},{stats['estimated_hours']:.1f},{stats['actual_hours']:.1f},{stats['saved_hours']:.1f},{stats['saving_ratio']:.1f},{stats['total_employees']},{stats['employees_with_worklog_count']},{stats['employees_without_worklog_count']}\n"
                f.write(row)
                
            # Tổng cộng
            total_row = f"TỔNG CỘNG,{total_tasks},{sum(stats['tasks_with_worklog'] for stats in filtered_projects.values())},{(sum(stats['tasks_with_worklog'] for stats in filtered_projects.values()) / total_tasks * 100) if total_tasks > 0 else 0:.1f},{total_estimated_hours:.1f},{total_actual_hours:.1f},{total_saved_hours:.1f},{(total_saved_hours / total_estimated_hours * 100) if total_estimated_hours > 0 else 0:.1f},{total_employees},{len([e for e in all_employees.values() if e['tasks_with_worklog'] > 0])},{len([e for e in all_employees.values() if e['tasks_with_worklog'] == 0])}\n"
            f.write(total_row)
        
        print(f"✅ Đã tạo báo cáo tổng hợp: {output_file}")
        print(f"✅ Đã tạo báo cáo CSV: {csv_output_file}")
        
        return project_stats_for_comparison    
    except Exception as e:
        print(f"❌ Lỗi khi tạo báo cáo tổng hợp: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def check_consistency(project_summary_stats, project_report_files):
    """
    Kiểm tra tính nhất quán giữa báo cáo tổng hợp và báo cáo chi tiết dự án
    
    Args:
        project_summary_stats (dict): Thông tin thời gian tiết kiệm từ báo cáo tổng hợp
        project_report_files (list): Danh sách file báo cáo dự án
        
    Returns:
        bool: True nếu nhất quán, False nếu có sự khác biệt
    """
            # print("\n🔍 KIỂM TRA TÍNH NHẤT QUÁN GIỮA BÁO CÁO TỔNG HỢP VÀ BÁO CÁO DỰ ÁN:")
    
    inconsistencies = []
    
    for report_file in project_report_files:
        # Lấy tên dự án từ tên file báo cáo
        file_name = os.path.basename(report_file)
        
        # Bỏ qua file báo cáo tổng hợp
        if file_name.startswith("all_projects_summary"):
            continue
            
        # Trích xuất thông tin thời gian tiết kiệm từ file báo cáo dự án
        try:
            with open(report_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
                # Tìm tên dự án
                match = re.search(r"=== BÁO CÁO DỰ ÁN: (.*?) ===", content)
                if not match:
                    continue
                    
                project_name = match.group(1)
                
                # Bỏ qua nếu dự án không có trong báo cáo tổng hợp
                if project_name not in project_summary_stats:
                    continue
                
                # Tìm thông tin thời gian từ báo cáo dự án
                est_match = re.search(r"- Tổng thời gian ước tính: ([\d\.]+) giờ", content)
                act_match = re.search(r"- Tổng thời gian thực tế: ([\d\.]+) giờ", content)
                save_match = re.search(r"- Thời gian tiết kiệm: ([\d\.]+) giờ", content)
                
                if est_match and act_match and save_match:
                    report_est = float(est_match.group(1))
                    report_act = float(act_match.group(1))
                    report_save = float(save_match.group(1))
                    
                    # So sánh với thông tin từ báo cáo tổng hợp
                    summary_est = project_summary_stats[project_name]['estimated_hours']
                    summary_act = project_summary_stats[project_name]['actual_hours']
                    summary_save = project_summary_stats[project_name]['saved_hours']
                    
                    # Kiểm tra sự chênh lệch (cho phép sai số nhỏ do làm tròn)
                    est_diff = abs(report_est - summary_est)
                    act_diff = abs(report_act - summary_act)
                    save_diff = abs(report_save - summary_save)
                    
                    tolerance = 1.0  # Tăng dung sai lên 1 giờ
                    
                    if est_diff > tolerance or act_diff > tolerance or save_diff > tolerance:
                        inconsistencies.append({
                            'project': project_name,
                            'report_file': file_name,
                            'report_est': report_est,
                            'summary_est': summary_est,
                            'est_diff': est_diff,
                            'report_act': report_act,
                            'summary_act': summary_act,
                            'act_diff': act_diff,
                            'report_save': report_save,
                            'summary_save': summary_save,
                            'save_diff': save_diff
                        })
        except Exception as e:
            print(f"⚠️ Lỗi khi kiểm tra file {file_name}: {str(e)}")
    
    # Hiển thị kết quả
    if inconsistencies:
        print("\n⚠️ Phát hiện sự không nhất quán giữa báo cáo tổng hợp và báo cáo dự án:")
        
        # Nhóm các vấn đề theo dự án
        grouped_issues = {}
        for item in inconsistencies:
            project = item['project']
            if project not in grouped_issues:
                grouped_issues[project] = []
            grouped_issues[project].append(item)
        
        # In ra theo từng dự án
        for project, issues in grouped_issues.items():
            print(f"\n{'-'*80}")
            # print(f"🔍 DỰ ÁN: {project}")
            print(f"{'-'*80}")
            
            # Chỉ hiển thị file với chênh lệch lớn nhất cho mỗi dự án
            max_diff_issue = max(issues, key=lambda x: max(x['est_diff'], x['act_diff'], x['save_diff']))
            
            print(f"  File: {max_diff_issue['report_file']}")
            print(f"  Ước tính:   Báo cáo dự án: {max_diff_issue['report_est']:.1f}h    Tổng hợp: {max_diff_issue['summary_est']:.1f}h    Chênh lệch: {max_diff_issue['est_diff']:.1f}h")
            print(f"  Thực tế:    Báo cáo dự án: {max_diff_issue['report_act']:.1f}h    Tổng hợp: {max_diff_issue['summary_act']:.1f}h    Chênh lệch: {max_diff_issue['act_diff']:.1f}h")
            print(f"  Tiết kiệm:  Báo cáo dự án: {max_diff_issue['report_save']:.1f}h    Tổng hợp: {max_diff_issue['summary_save']:.1f}h    Chênh lệch: {max_diff_issue['save_diff']:.1f}h")
            
            if len(issues) > 1:
                print(f"  (Còn {len(issues)-1} file khác có chênh lệch tương tự)")
        
        return False
    else:
        print("✅ Tất cả báo cáo đều nhất quán!")
        return True

def synchronize_reports(all_tasks, output_dir, timestamp):
    """
    Đồng bộ hóa báo cáo tổng hợp và báo cáo chi tiết dự án
    
    Args:
        all_tasks (list): Danh sách tất cả các task
        output_dir (str): Thư mục chứa báo cáo
        timestamp (str): Dấu thời gian để xác định báo cáo cùng đợt
        
    Returns:
        bool: True nếu thành công, False nếu thất bại
    """
    try:
        # Lấy danh sách tất cả file báo cáo
        report_files = [os.path.join(output_dir, f) for f in os.listdir(output_dir) 
                         if f.endswith(f"{timestamp}.txt")]
        
        # Đọc báo cáo tổng hợp
        summary_file = os.path.join(output_dir, f"all_projects_summary_{timestamp}.txt")
        if not os.path.exists(summary_file):
            print(f"⚠️ Không tìm thấy báo cáo tổng hợp: {summary_file}")
            return False
            
        # Tạo dictionary để lưu thông tin dự án
        projects_tasks = {}
        for task in all_tasks:
            project_name = task.get('actual_project', task.get('project', 'Unknown'))
            if project_name in ["FC", "IMS"]:  # Bỏ qua các dự án không mong muốn
                continue
                
            if project_name not in projects_tasks:
                projects_tasks[project_name] = []
            projects_tasks[project_name].append(task)
        
        # Tính toán lại thời gian tiết kiệm cho từng dự án
        project_stats = {}
        for project_name, tasks in projects_tasks.items():
            total_estimated_hours = sum(task.get('original_estimate_hours', 0) or 0 for task in tasks)
            total_actual_hours = sum(task.get('total_hours', 0) or 0 for task in tasks)
            saved_hours = total_estimated_hours - total_actual_hours
            saving_ratio = (saved_hours / total_estimated_hours * 100) if total_estimated_hours > 0 else 0
            
            project_stats[project_name] = {
                'total_tasks': len(tasks),
                'tasks_with_logwork': len([task for task in tasks if task.get('has_worklog', False)]),
                'total_estimated_hours': total_estimated_hours,
                'total_actual_hours': total_actual_hours,
                'saved_hours': saved_hours,
                'saving_ratio': saving_ratio
            }
        
        # Cập nhật báo cáo tổng hợp
        with open(summary_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Cập nhật từng dự án trong báo cáo tổng hợp
        for project_name, stats in project_stats.items():
            # Tìm dòng thông tin dự án trong báo cáo
            pattern = r"\|\s+" + re.escape(project_name) + r"\s+\|\s+(\d+)\s+\|\s+(\d+)\s+\|\s+([\d\.]+)\s+\|\s+([\d\.]+)\s+\|\s+([\d\.]+)\s+\|\s+([\d\.]+)\s+\|\s+([\d\.]+)\s+\|\s+(\d+)\s+\|\s+(\d+)\s+\|\s+(\d+)\s+\|"
            replacement = f"| {project_name:<30} | {stats['total_tasks']:>5} | {stats['tasks_with_logwork']:>5} | {(stats['tasks_with_logwork']/stats['total_tasks']*100) if stats['total_tasks'] > 0 else 0:>6.1f} | {stats['total_estimated_hours']:>8.1f} | {stats['total_actual_hours']:>8.1f} | {stats['saved_hours']:>8.1f} | {stats['saving_ratio']:>6.1f} | {0:>8} | {0:>8} | {0:>8} |"
            content = re.sub(pattern, replacement, content)
            
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write(content)
            
        # Cập nhật từng báo cáo dự án
        for project_name, stats in project_stats.items():
            # Tìm file báo cáo dự án
            project_file = None
            for file in report_files:
                if project_name in os.path.basename(file) and "all_projects_summary" not in os.path.basename(file):
                    project_file = file
                    break
                    
            if not project_file:
                continue
                
            # Đọc nội dung file
            with open(project_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Cập nhật thời gian tiết kiệm
            est_pattern = r"- Tổng thời gian ước tính: ([\d\.]+) giờ"
            act_pattern = r"- Tổng thời gian thực tế: ([\d\.]+) giờ"
            save_pattern = r"- Thời gian tiết kiệm: ([\d\.]+) giờ \(([\d\.]+)%\)"
            
            # Đảm bảo số liệu ước tính và thực tế khớp với đã tính toán
            content = re.sub(est_pattern, f"- Tổng thời gian ước tính: {stats['total_estimated_hours']:.1f} giờ", content)
            content = re.sub(act_pattern, f"- Tổng thời gian thực tế: {stats['total_actual_hours']:.1f} giờ", content)
            content = re.sub(save_pattern, f"- Thời gian tiết kiệm: {stats['saved_hours']:.1f} giờ ({stats['saving_ratio']:.1f}%)", content)
            
            # Ghi nội dung mới
            with open(project_file, 'w', encoding='utf-8') as f:
                f.write(content)
                
        print(f"✅ Đã đồng bộ hóa tất cả các báo cáo thành công!")
        return True
        
    except Exception as e:
        print(f"❌ Lỗi khi đồng bộ hóa báo cáo: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    main() 
