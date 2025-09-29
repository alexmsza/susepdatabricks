import requests
import zipfile
import io
import os

# URL of the ZIaP file
url = "https://www2.susep.gov.br/redarq.asp?arq=Autoseg2021A%2ezip"

# Download the ZIP file without verifying the certificate
response = requests.get(url, verify=False)
response.raise_for_status()

# Local directory for extracted files
output_dir = "/Volumes/workspace/default/susep-data/autoseg2021/raw"
os.makedirs(output_dir, exist_ok=True)

# Extract the ZIP contents
with zipfile.ZipFile(io.BytesIO(response.content)) as z:
    z.printdir()
    z.extractall(output_dir)

print(f"Folder created at: {output_dir}")

#caso faça o upload manualmente do arquivo zip e precise extrair no volume
#%sh unzip /Volumes/workspace/default/susep-data/Autoseg2021A.zip -d /Volumes/workspace/default/susep-data/raw
