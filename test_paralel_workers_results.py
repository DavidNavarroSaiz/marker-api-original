import asyncio
import aiohttp
import requests
import os
import time
from datetime import datetime

# API Endpoints
CONVERT_API_URL = "http://localhost:9090/convert"
CELERY_API_URL = "http://localhost:9090/celery/convert"
MULTI_CONVERT_API_URL = "http://localhost:9090/batch_convert"
RESULT_API_URL = "http://localhost:9090/celery/result"

# PDF File Path
TEST_FILES_FOLDER = "test_files"

def format_celery_result(result):
    """Format the Celery result in a clean, readable way."""
    if not result:
        return {"error": "Empty result data"}

    task_id = result.get("task_id", "N/A")
    status = result.get("status", "N/A")
    result_data = result.get("result", {})

    # Safely access nested fields
    filename = result_data.get("filename", "N/A")
    markdown_preview = (result_data.get("markdown", "")[:100] + "...") if result_data.get("markdown") else "No Markdown"
    metadata = result_data.get("metadata", {})
    pages = metadata.get("pages", "N/A")
    languages = metadata.get("languages", "N/A")
    images = result_data.get("images", {})

    formatted_result = {
        "task_id": task_id,
        "status": status,
        "filename": filename,
        "markdown": markdown_preview,
        "metadata": {
            "languages": languages,
            "pages": pages
        },
        "images": {k: v[:50] + "..." for k, v in images.items()}
    }
    return formatted_result

async def send_pdf_to_worker(session, pdf_file, pdf_path):
    """Send a single PDF to the worker and return task info."""
    print(f"\n📤 [{datetime.now().strftime('%H:%M:%S')}] Sending: {pdf_file}")
    try:
        with open(pdf_path, "rb") as file:
            pdf_file_to_send = {"pdf_file": file}
            response = requests.post(CELERY_API_URL, files=pdf_file_to_send)

        if response.status_code == 200:
            result = response.json()
            task_id = result.get("task_id")
            if task_id:
                print(f"✅ Task created for {pdf_file} - Task ID: {task_id}")
                return {"filename": pdf_file, "task_id": task_id, "status": "Processing"}
        else:
            print(f"❌ Error for {pdf_file}: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"⚠️ Failed to process {pdf_file}: {e}")
    return None

async def send_pdfs_to_workers(n_times=1, limit=15):
    """Send PDFs to workers and return task IDs with filenames."""
    pdf_files = [f for f in os.listdir(TEST_FILES_FOLDER) if f.endswith(".pdf")]
    tasks_info = []

    if not pdf_files:
        print("❌ No PDF files found in the test_files folder.")
        return tasks_info

    async with aiohttp.ClientSession() as session:
        for _ in range(n_times):
            print(f"\n🚀 Sending {len(pdf_files)} PDF(s) to the API...")

            for pdf_file in pdf_files[:limit]:
                pdf_path = os.path.join(TEST_FILES_FOLDER, pdf_file)
                task_info = await send_pdf_to_worker(session, pdf_file, pdf_path)
                if task_info:
                    tasks_info.append(task_info)
                await asyncio.sleep(2)

    print(f"\n📋 Total files sent: {len(tasks_info)}")
    return tasks_info

async def check_task_status(session, task):
    """Check the status of a single task."""
    try:
        async with session.get(f"{RESULT_API_URL}/{task['task_id']}") as response:
            if response.status == 200:
                result_data = await response.json()
                if result_data:
                    status = result_data.get('status', 'Unknown')
                    print(f"\n📥 [{datetime.now().strftime('%H:%M:%S')}] Result for {task['filename']} - Status: {status}")

                    if status == 'Success':
                        formatted_result = format_celery_result(result_data)
                        print(f"✅ Success: {formatted_result}")
                        return formatted_result
                return {"status": "Processing"}
            elif response.status == 202:
                print(f"⏳ Still processing: {task['filename']}")
                return {"status": "Processing"}
            else:
                print(f"⚠️ Unexpected status {response.status} for {task['filename']}")
    except Exception as e:
        print(f"⚠️ Error checking {task['filename']}: {e}")
    return None

async def poll_results(tasks_info, max_retries=60, delay=5):
    """Poll for task results until completion or timeout."""
    results = {}
    pending_tasks = tasks_info.copy()
    retry_count = 0

    print(f"\n📊 Collecting results for {len(pending_tasks)} tasks...")

    async with aiohttp.ClientSession() as session:
        while pending_tasks and retry_count < max_retries:
            await asyncio.sleep(delay)

            for task in pending_tasks[:]:
                result_data = await check_task_status(session, task)
                if result_data and result_data.get("status") == "Success":
                    results[task['filename']] = result_data
                    pending_tasks.remove(task)
                    print(f"✅ Completed: {task['filename']}")
                elif result_data and result_data.get("status") == "Failed":
                    results[task['filename']] = {"task_id": task['task_id'], "error": "Processing failed"}
                    pending_tasks.remove(task)

            retry_count += 1
            print(f"\n⏳ {len(pending_tasks)} tasks remaining (Attempt {retry_count}/{max_retries})")

    if pending_tasks:
        print(f"\n⚠️ Timed out waiting for {len(pending_tasks)} tasks.")
        for task in pending_tasks:
            results[task['filename']] = {"task_id": task['task_id'], "error": "Timed out"}

    return results

async def process_pdfs_with_results(n_times=1, limit=15):
    """Main function to process PDFs and collect results."""
    tasks_info = await send_pdfs_to_workers(n_times, limit)
    if not tasks_info:
        return {}

    results = await poll_results(tasks_info)
    print("\n📑 Final Summary:")
    print(f"   └── Total processed: {len(results)}")
    print(f"   └── Success: {sum(1 for r in results.values() if r.get('status') == 'Success')}")
    print(f"   └── Failed: {sum(1 for r in results.values() if 'error' in r)}")

    return results

if __name__ == "__main__":
    results = asyncio.run(process_pdfs_with_results(n_times=1, limit=3))
    
    print("\n🔍 Detailed Results:")
    for filename, data in results.items():
        print(f"\n📄 {filename}:")
        print(f"   └── Task ID: {data.get('task_id', 'N/A')}")
        if data.get('status') == 'Success':
            print(f"   └── Status: {data.get('status', 'N/A')}")
            print(f"   └── Pages: {data.get('metadata', {}).get('pages', 'N/A')}")
            print(f"   └── Number of Images: {len(data.get('images', {}))}")
            print(f"   └── Markdown Preview: {data.get('markdown', '')[:100]}...")
        else:
            print(f"   └── Error: {data.get('error', 'Unknown error')}")