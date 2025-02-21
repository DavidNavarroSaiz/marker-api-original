import argparse
import requests
import sys

# Adjust this to your environment
CONVERT_API_URL = "http://localhost:9090/convert"

def convert_pdf(pdf_filename: str):
    """Send a single PDF filename to the /convert endpoint."""
    print(f"\n📌 Using Convert Endpoint for {pdf_filename}")
    try:
        payload = {"pdf_filename": pdf_filename}
        response = requests.post(CONVERT_API_URL, json=payload)

        if response.status_code == 200:
            result = response.json()
            print(f"✅ Conversion successful! Response: {result}")
        else:
            print(f"❌ Error: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ An error occurred: {str(e)}")
        sys.exit(1)

def parse_args():
    parser = argparse.ArgumentParser(description="Convert a PDF file via the Convert endpoint")
    parser.add_argument("--file", type=str, required=True, help="Path to the PDF file")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    convert_pdf(args.file)
