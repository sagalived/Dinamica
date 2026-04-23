import os
import zipfile
import datetime
from pathlib import Path

# Note: This is a placeholder structure for Google Drive Upload.
# To make this functional, you must:
# 1. pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client
# 2. Place 'credentials.json' in the same folder.
# 3. Enable Google Drive API in GCP.

# try:
#     from google.oauth2.service_account import Credentials
#     from googleapiclient.discovery import build
#     from googleapiclient.http import MediaFileUpload
# except ImportError:
#     pass

SCOPES = ['https://www.googleapis.com/auth/drive.file']
CREDENTIALS_FILE = 'credentials.json'
DRIVE_FOLDER_ID = ''  # You can specify a parent folder ID here

def create_zip_backup(source_dir: str, output_path: str):
    """Zips an entire directory."""
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(source_dir):
            for file in files:
                file_path = os.path.join(root, file)
                zipf.write(file_path, os.path.relpath(file_path, source_dir))
    return output_path

def upload_to_drive(file_path: str):
    """Uploads a file to Google drive using Service Account.
       Fails gracefully if credentials are not present.
    """
    if not os.path.exists(CREDENTIALS_FILE):
        return {"success": False, "error": f"Credencial [{CREDENTIALS_FILE}] não encontrada."}
    
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload
    except ImportError:
         return {"success": False, "error": "Bibliotecas do google não instaladas. Rode: pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib"}

    try:
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds)

        file_metadata = {'name': os.path.basename(file_path)}
        if DRIVE_FOLDER_ID:
            file_metadata['parents'] = [DRIVE_FOLDER_ID]

        media = MediaFileUpload(file_path, mimetype='application/zip')
        
        file = service.files().create(body=file_metadata,
                                      media_body=media,
                                      fields='id').execute()
        return {"success": True, "file_id": file.get('id')}
    except Exception as e:
        return {"success": False, "error": str(e)}

def run_backup():
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    if not os.path.exists(data_dir):
        return {"success": False, "error": "Pasta /data não encontrada para fazer backup."}
        
    date_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_name = f"backup_dinamica_{date_str}.zip"
    zip_path = os.path.join(os.path.dirname(__file__), zip_name)
    
    # 1. Create ZIP
    create_zip_backup(data_dir, zip_path)
    
    # 2. Upload
    upload_result = upload_to_drive(zip_path)
    
    # 3. Cleanup local zip to save space
    if os.path.exists(zip_path):
        os.remove(zip_path)
        
    return upload_result

if __name__ == "__main__":
    print(run_backup())
