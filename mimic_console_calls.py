import subprocess
import os
import time
TEST_FILES_FOLDER = "test_files"

def mimic_manual_console_calls():
    """Calls convert_pdf.py (which calls celery_convert_pdf) once per PDF,
    in its own process, to mimic manual console calls in the terminal."""
    
    pdf_files = [f for f in os.listdir(TEST_FILES_FOLDER) if f.lower().endswith(".pdf")]
    if not pdf_files:
        print("❌ No PDF files found in test_files folder.")
        return

    for pdf_file in pdf_files:
        pdf_path = os.path.join(TEST_FILES_FOLDER, pdf_file)
        print(f"\n---> Spawning new process for: {pdf_path}")

        # Here we call: python convert_pdf.py --file path/to/this.pdf
        # Each call is a fresh process, just like typing it manually in your shell.
        result = subprocess.run(["python", "convert_pdf.py", "--file", pdf_path])
        time.sleep(3) 
        if result.returncode == 0:
            print(f"✅ Success for {pdf_file}")
        else:
            print(f"❌ Error in sub-process for {pdf_file}, return code {result.returncode}")

if __name__ == "__main__":
    mimic_manual_console_calls()
