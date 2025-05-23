import os
import shutil
import dask
from dask.diagnostics import ProgressBar
import io # Para MediaIoBaseDownload

# Para Google Drive (requiere configuración de credenciales OAuth2)
try:
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
    GOOGLE_LIBS_AVAILABLE = True
except ImportError:
    GOOGLE_LIBS_AVAILABLE = False
    print("Advertencia: Bibliotecas de Google no encontradas. Funcionalidad de Google Drive no disponible.")
    print("Instale con: pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib")


# Rutas para credenciales de Google Drive (deben estar en el mismo directorio o ser configurables)
SCOPES = ['https://www.googleapis.com/auth/drive']
TOKEN_PATH = 'token.json' 
CREDS_PATH = 'credentials.json' 

def get_gdrive_service():
    if not GOOGLE_LIBS_AVAILABLE:
        return None
    creds = None
    if os.path.exists(TOKEN_PATH):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
        except Exception as e:
            print(f"Error cargando token.json: {e}. Se intentará re-autenticar.")
            creds = None
            
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                print("Refrescando token de Google Drive...")
                creds.refresh(Request())
            except Exception as e:
                print(f"Error refrescando token: {e}. Se requiere re-autenticación manual.")
                creds = None 
        
        if not creds: 
            if not os.path.exists(CREDS_PATH):
                print(f"Error: Falta el archivo de credenciales '{CREDS_PATH}'.")
                print("Descárguelo desde Google Cloud Console y colóquelo en el directorio del script.")
                return None
            try:
                print("Realizando autenticación de Google Drive (puede requerir abrir navegador)...")
                flow = InstalledAppFlow.from_client_secrets_file(CREDS_PATH, SCOPES)
                creds = flow.run_local_server(port=0) 
            except Exception as e:
                print(f"Fallo en el flujo de autenticación de Google: {e}")
                return None

        with open(TOKEN_PATH, 'w') as token_file:
            token_file.write(creds.to_json())
        print("Token de Google Drive guardado/actualizado.")
            
    try:
        service = build('drive', 'v3', credentials=creds)
        return service
    except Exception as e:
        print(f"Error creando servicio de Google Drive: {e}")
        return None

def copy_to_local_disk(source_file_path, destination_path):
    if not os.path.exists(source_file_path):
        raise FileNotFoundError(f"Archivo fuente no encontrado: {source_file_path}")
    try:
        destination_dir = os.path.dirname(destination_path)
        if not os.path.exists(destination_dir):
            os.makedirs(destination_dir, exist_ok=True)
        shutil.copy2(source_file_path, destination_path)
        print(f"Archivo copiado de '{source_file_path}' a '{destination_path}'")
    except Exception as e:
        raise IOError(f"Error al copiar a disco local '{destination_path}': {e}")

@dask.delayed
def _upload_file_to_gdrive_task(local_file_path, cloud_folder_id, filename_on_cloud):
    service = get_gdrive_service() 
    if not service:
        print("Servicio de Google Drive no disponible para la tarea de subida.")
        return None, filename_on_cloud # Devolver también el nombre para identificar el fallo
    
    file_metadata = {
        'name': filename_on_cloud,
        'parents': [cloud_folder_id] if cloud_folder_id else []
    }
    # Determinar mimetype (opcional pero bueno)
    mimetype = 'application/octet-stream'
    if filename_on_cloud.lower().endswith('.zip'):
        mimetype = 'application/zip'
    elif filename_on_cloud.lower().endswith(('.txt', '.log')):
        mimetype = 'text/plain'
    
    media = MediaFileUpload(local_file_path, resumable=True, mimetype=mimetype) 
    
    try:
        print(f"\rDask: Subiendo '{local_file_path}' a Google Drive como '{filename_on_cloud}' (Carpeta ID: {cloud_folder_id})...", end="")
        file_drive = service.files().create(body=file_metadata, media_body=media, fields='id, name').execute()
        print(f"\nArchivo '{file_drive.get('name')}' subido a Google Drive con ID: {file_drive.get('id')}")
        return file_drive.get('id'), filename_on_cloud
    except Exception as e:
        print(f"\nError al subir '{local_file_path}' a Google Drive: {e}")
        return None, filename_on_cloud

def upload_to_google_drive(local_file_path, cloud_folder_id, filename_on_cloud=None):
    if not GOOGLE_LIBS_AVAILABLE:
        raise EnvironmentError("Bibliotecas de Google no disponibles.")
        
    if not os.path.exists(local_file_path):
        raise FileNotFoundError(f"Archivo local no encontrado para subir: {local_file_path}")

    if filename_on_cloud is None:
        filename_on_cloud = os.path.basename(local_file_path)
    
    service = get_gdrive_service() # Obtener servicio una vez aquí para fallo rápido si no está disponible
    if not service:
        raise ConnectionError("No se pudo obtener el servicio de Google Drive.")

    task = _upload_file_to_gdrive_task(local_file_path, cloud_folder_id, filename_on_cloud)
    print(f"Programando subida de '{filename_on_cloud}' a Google Drive con Dask...")
    with ProgressBar():
        results = dask.compute(task, scheduler='threads')
        file_id, name = results[0] # dask.compute devuelve una tupla de resultados
    
    if file_id is None:
        raise IOError(f"Falló la subida del archivo '{name}' a Google Drive.")
    return file_id

def find_file_in_gdrive(service, folder_id, filename):
    """Busca un archivo por nombre dentro de una carpeta específica en Google Drive."""
    if not service: return None, None
    try:
        safe_filename = filename.replace("'", "\\'")
        query = f"name = '{safe_filename}' and '{folder_id}' in parents and trashed = false"
        
        response = service.files().list(q=query,
                                        spaces='drive',
                                        pageSize=1, # Solo necesitamos uno si existe
                                        fields='files(id, name, size)').execute()
        files = response.get('files', [])
        if files:
            if len(files) > 1: # Debería ser raro con pageSize=1, pero por si acaso
                print(f"Advertencia: Se encontraron múltiples archivos con el nombre '{filename}' en la carpeta ID '{folder_id}'. Se usará el primero.")
            return files[0]['id'], files[0].get('size')
        return None, None
    except Exception as e:
        print(f"Error buscando archivo '{filename}' en Google Drive (carpeta ID {folder_id}): {e}")
        return None, None

def download_from_google_drive(folder_id_or_file_id, filename_on_drive, local_download_path):
    """
    Descarga un archivo de Google Drive.
    """
    if not GOOGLE_LIBS_AVAILABLE:
        raise EnvironmentError("Bibliotecas de Google no disponibles.")

    service = get_gdrive_service()
    if not service:
        raise ConnectionError("No se pudo obtener el servicio de Google Drive.")

    file_id_to_download = None
    file_size_str = None
    actual_filename_on_drive = filename_on_drive

    try:
        file_metadata = service.files().get(fileId=folder_id_or_file_id, fields="id, name, size, mimeType").execute()
        # Si lo anterior tiene éxito, folder_id_or_file_id era un file_id
        file_id_to_download = file_metadata.get('id')
        actual_filename_on_drive = file_metadata.get('name') 
        file_size_str = file_metadata.get('size')
        # Ignorar si el archivo es una carpeta de Google Drive
        if file_metadata.get('mimeType') == 'application/vnd.google-apps.folder':
            print(f"El ID '{folder_id_or_file_id}' corresponde a una carpeta de Google Drive, no a un archivo descargable directamente por nombre '{filename_on_drive}'.")
            file_id_to_download = None # Forzar la búsqueda si se proporcionó un nombre
    except Exception:
        # Si falla, folder_id_or_file_id probablemente era un folder_id, o un file_id inválido
        file_id_to_download = None # Asegurar que se resetee

    if not file_id_to_download: # Si no se obtuvo un ID de archivo directamente o era una carpeta
        if not actual_filename_on_drive: # filename_on_drive original
            raise ValueError("Se requiere `filename_on_drive` si `folder_id_or_file_id` es un ID de carpeta o no se pudo resolver como ID de archivo.")
        print(f"Buscando archivo '{actual_filename_on_drive}' en carpeta de Google Drive ID '{folder_id_or_file_id}'...")
        file_id_to_download, file_size_str = find_file_in_gdrive(service, folder_id_or_file_id, actual_filename_on_drive)

    if not file_id_to_download:
        raise FileNotFoundError(f"Archivo '{actual_filename_on_drive}' no encontrado en Google Drive (ubicación especificada: {folder_id_or_file_id}).")

    # Crear directorio de descarga si no existe
    # Validar local_download_path
    if os.path.isdir(local_download_path): # Si el usuario pasó un directorio, añadir el nombre del archivo
        local_file_destination = os.path.join(local_download_path, actual_filename_on_drive)
    else: # El usuario pasó la ruta completa del archivo
        local_file_destination = local_download_path
    
    os.makedirs(os.path.dirname(local_file_destination), exist_ok=True)

    print(f"Descargando archivo '{actual_filename_on_drive}' (ID: {file_id_to_download}, Tamaño: {file_size_str if file_size_str else 'N/A'}) de Google Drive a '{local_file_destination}'...")
    request = service.files().get_media(fileId=file_id_to_download)
    
    try:
        with io.FileIO(local_file_destination, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request, chunksize=1024*1024*5) # Chunks de 5MB
            done = False
            while not done:
                status, done = downloader.next_chunk(num_retries=3) # Añadir reintentos
                if status:
                    print(f"\rProgreso de descarga: {int(status.progress() * 100)}%", end="")
            print("\nDescarga completada.")
    except Exception as e:
        if os.path.exists(local_file_destination):
            try:
                os.remove(local_file_destination)
            except OSError:
                pass
        raise IOError(f"Error durante la descarga de Google Drive: {e}")

    return local_file_destination


@dask.delayed
def _copy_single_fragment_to_usb_task(fragment_path, usb_destination_file_path):
    try:
        # print(f"Dask: Copiando {os.path.basename(fragment_path)} a USB...")
        shutil.copy2(fragment_path, usb_destination_file_path)
        return usb_destination_file_path, True
    except Exception as e:
        print(f"Error copiando fragmento {os.path.basename(fragment_path)} a USB '{usb_destination_file_path}': {e}")
        return usb_destination_file_path, False

def copy_fragments_to_usb(fragments_source_dir, usb_target_fragments_dir):
    if not os.path.isdir(fragments_source_dir):
        raise FileNotFoundError(f"Directorio fuente de fragmentos no encontrado: {fragments_source_dir}")
    
    try:
        os.makedirs(usb_target_fragments_dir, exist_ok=True)
        print(f"Directorio de destino en USB: {usb_target_fragments_dir}")
    except OSError as e:
        raise IOError(f"No se pudo crear el directorio de destino en USB '{usb_target_fragments_dir}': {e}")

    tasks = []
    for fragment_filename in os.listdir(fragments_source_dir):
        fragment_source_path = os.path.join(fragments_source_dir, fragment_filename)
        if os.path.isfile(fragment_source_path) and ".part" in fragment_filename: # Simple check for fragment
            usb_destination_file_path = os.path.join(usb_target_fragments_dir, fragment_filename)
            tasks.append(_copy_single_fragment_to_usb_task(fragment_source_path, usb_destination_file_path))
    
    if tasks:
        print(f"Copiando {len(tasks)} fragmentos a USB con Dask...")
        with ProgressBar():
            results = dask.compute(*tasks, scheduler='threads')
        
        num_failed = sum(1 for _, success in results if not success)
        if num_failed > 0:
            failed_paths = [path for path, success in results if not success]
            raise IOError(f"Falló la copia de {num_failed} fragmentos a USB: {', '.join(os.path.basename(str(p)) for p in failed_paths)}")
    else:
        print("No hay fragmentos para copiar a USB.")