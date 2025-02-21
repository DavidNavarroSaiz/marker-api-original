import argparse
import requests
import sys

# Adjust these to your environment
CELERY_API_URL = "http://localhost:9090/celery/convert"

def celery_convert_pdf(pdf_filename: str):
    """Send a single PDF to the /celery/convert endpoint."""
    print(f"\n📌 Using Celery Method (Asynchronous) for {pdf_filename}")
    try:
        with open(pdf_filename, "rb") as pdf_file:
            files = {"pdf_file": pdf_file}
            response = requests.post(CELERY_API_URL, files=files)

        if response.status_code == 200:
            result = response.json()
            print(f"✅ Celery Conversion successful! Response: {result}")
        else:
            print(f"❌ Error: {response.status_code} - {response.text}")
    except FileNotFoundError:
        print(f"❌ The file {pdf_filename} was not found.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ An error occurred: {str(e)}")
        sys.exit(1)

def parse_args():
    parser = argparse.ArgumentParser(description="Convert a PDF file with Celery")
    parser.add_argument("--file", type=str, required=True, help="Path to the PDF file")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    celery_convert_pdf(args.file)
