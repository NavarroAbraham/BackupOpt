import os
import pyzipper # Para ZIP con encriptación AES
import shutil
import dask
from dask.diagnostics import ProgressBar
import logging

logging.getLogger('pyzipper').setLevel(logging.WARNING)


def get_all_file_paths(source_folders):
    """ Recorre las carpetas y obtiene todas las rutas de archivos. """
    file_paths = []
    for folder in source_folders:
        abs_folder = os.path.abspath(folder)
        if not os.path.isdir(abs_folder):
            print(f"Advertencia: La carpeta fuente {abs_folder} no existe o no es un directorio.")
            continue
        for root, _, files in os.walk(abs_folder):
            for file in files:
                file_paths.append(os.path.join(root, file))
    return file_paths

def create_backup_archive(source_folders, output_zip_path, compress_type='zip', password=None):
    """
    Crea un archivo ZIP, opcionalmente encriptado con AES-256.
    La escritura al archivo ZIP es secuencial.
    """
    if compress_type != 'zip':
        raise NotImplementedError("Solo compresión ZIP con pyzipper está implementada.")

    all_files_to_backup = get_all_file_paths(source_folders)


    if not all_files_to_backup:
        print("No se encontraron archivos para respaldar.")
    output_dir = os.path.dirname(os.path.abspath(output_zip_path))
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    print(f"Creando archivo ZIP en {output_zip_path} con {len(all_files_to_backup)} archivos...")

    with pyzipper.AESZipFile(output_zip_path,
                             'w',
                             compression=pyzipper.ZIP_DEFLATED,
                             encryption=(pyzipper.WZ_AES if password else None)) as zf:
        if password:
            zf.setpassword(password.encode('utf-8'))
        
        for filepath in all_files_to_backup:
            arcname = None
            matched_source_folder = None
            for sf_raw in source_folders:
                sf = os.path.abspath(sf_raw)
                if os.path.abspath(filepath).startswith(sf):
                    if matched_source_folder is None or len(sf) > len(matched_source_folder):
                        matched_source_folder = sf
            
            if matched_source_folder:
                base_source_name = os.path.basename(matched_source_folder)
                relative_path_from_matched_source_parent = os.path.relpath(filepath, os.path.dirname(matched_source_folder))
                arcname = relative_path_from_matched_source_parent
            else:
                arcname = os.path.basename(filepath)
            
            zf.write(filepath, arcname=arcname)
            
    print(f"Archivo ZIP '{output_zip_path}' creado exitosamente.")
    return output_zip_path

# --- Paralelización en la restauración (extracción) ---
@dask.delayed
def extract_single_member(zip_filepath, member_name, target_path, password_bytes):
    """
    Tarea Dask para extraer un único miembro de un archivo ZIP.
    Cada tarea abre el ZIP, establece la contraseña y extrae un archivo.
    """
    try:
        with pyzipper.AESZipFile(zip_filepath, 'r') as zf_worker:
            if password_bytes:
                zf_worker.setpassword(password_bytes)
            zf_worker.extract(member_name, path=target_path)
        return member_name, True
    except Exception as e:
        print(f"Error extrayendo {member_name}: {e}")
        return member_name, False


def restore_from_archive(backup_zip_path, restore_to_path, password=None):
    """
    Restaura desde un archivo ZIP, opcionalmente desencriptando.
    Utiliza Dask para paralelizar la extracción de archivos individuales.
    """
    if not os.path.exists(backup_zip_path):
        raise FileNotFoundError(f"El archivo de backup {backup_zip_path} no fue encontrado.")
    
    print(f"Restaurando {backup_zip_path} a {restore_to_path}...")
    os.makedirs(restore_to_path, exist_ok=True)

    password_bytes = password.encode('utf-8') if password else None
    delayed_extract_tasks = []

    try:
        # Abrir el ZIP una vez para obtener la lista de miembros y probar la contraseña
        with pyzipper.AESZipFile(backup_zip_path, 'r') as zf_main:
            if password_bytes:
                zf_main.setpassword(password_bytes)
                try:
                    member_list = zf_main.namelist()
                except RuntimeError as e:
                    if "Bad password" in str(e) or "ęż" in str(e) or "CRC error" in str(e):
                        raise ValueError("Contraseña incorrecta o archivo corrupto.") from e
                    raise
            else:
                member_list = zf_main.namelist()

            if not member_list:
                print("El archivo ZIP está vacío. Nada que restaurar.")
                return

            print(f"Se encontraron {len(member_list)} miembros en el ZIP. Preparando extracción paralela...")
            for member in member_list:
                member_path = os.path.join(restore_to_path, member)
                if member.endswith('/'): # Es un directorio explícito en el ZIP
                    os.makedirs(member_path, exist_ok=True)
                else: # Es un archivo
                    member_dir = os.path.dirname(member_path)
                    if member_dir: # Asegurar que el directorio del archivo exista
                         os.makedirs(member_dir, exist_ok=True)
                
                delayed_extract_tasks.append(
                    extract_single_member(backup_zip_path, member, restore_to_path, password_bytes)
                )
        
        if delayed_extract_tasks:
            print(f"Ejecutando {len(delayed_extract_tasks)} tareas de extracción con Dask...")
            with ProgressBar():
                results = dask.compute(*delayed_extract_tasks, scheduler='threads') 
            
            num_failed = sum(1 for _, success in results if not success)
            if num_failed > 0:
                print(f"Advertencia: {num_failed} archivos no pudieron ser extraídos.")
            else:
                print("Todos los archivos extraídos exitosamente.")
        else:
            print("No hay archivos para extraer (solo directorios o ZIP vacío).")

        print("Restauración completada.")

    except pyzipper.zipfile.BadZipFile:
        raise ValueError("Archivo ZIP inválido o corrupto.")
    except Exception as e:
        print(f"Excepción durante la restauración: {e}")
        raise

@dask.delayed
def write_fragment(data_chunk, fragment_path):
    # print(f"Dask: Escribiendo fragmento {os.path.basename(fragment_path)}")
    try:
        with open(fragment_path, 'wb') as f_frag:
            f_frag.write(data_chunk)
        return fragment_path, True
    except Exception as e:
        print(f"Error escribiendo fragmento {fragment_path}: {e}")
        return fragment_path, False # Indicar fallo

def split_file(large_file_path, fragments_dir_local, fragment_size_bytes):
    """ Divide un archivo grande en fragmentos usando Dask para escrituras paralelas. """
    if not os.path.exists(large_file_path):
        raise FileNotFoundError(f"Archivo a fragmentar no encontrado: {large_file_path}")
    
    os.makedirs(fragments_dir_local, exist_ok=True)
    base_filename = os.path.basename(large_file_path)
    file_number = 0
    tasks = []

    with open(large_file_path, 'rb') as f_in:
        while True:
            chunk = f_in.read(fragment_size_bytes)
            if not chunk:
                break
            file_number += 1
            fragment_path = os.path.join(fragments_dir_local, f"{base_filename}.part{file_number:03d}")
            tasks.append(write_fragment(chunk, fragment_path))
    
    if tasks:
        print(f"Procesando {len(tasks)} fragmentos con Dask para escritura...")
        with ProgressBar():
            results = dask.compute(*tasks, scheduler='threads')
        
        num_failed = sum(1 for _, success in results if not success)
        if num_failed > 0:
            failed_paths = [path for path, success in results if not success]
            raise IOError(f"Falló la escritura de {num_failed} fragmentos: {', '.join(os.path.basename(p) for p in failed_paths)}")
    else:
        print("No se generaron fragmentos (archivo podría ser más pequeño que el tamaño del fragmento o estar vacío).")
    return fragments_dir_local

def merge_files(fragments_source_dir, base_filename_with_ext, output_file_path):
    """
    Une fragmentos para recrear el archivo original.
    """
    if not os.path.isdir(fragments_source_dir):
        raise FileNotFoundError(f"Directorio de fragmentos no encontrado: {fragments_source_dir}")

    fragment_paths = sorted([
        os.path.join(fragments_source_dir, f)
        for f in os.listdir(fragments_source_dir)
        if f.startswith(base_filename_with_ext) and f.endswith(tuple(f".part{i:03d}" for i in range(1000)))
    ])

    if not fragment_paths:
        raise FileNotFoundError(f"No se encontraron fragmentos para '{base_filename_with_ext}' en {fragments_source_dir}")

    print(f"Uniendo {len(fragment_paths)} fragmentos en {output_file_path}...")
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

    # La unión es secuencial. Paralelizar la lectura de fragmentos y encolarlos
    with open(output_file_path, 'wb') as f_out:
        for frag_path in fragment_paths:
            try:
                with open(frag_path, 'rb') as f_in:
                    shutil.copyfileobj(f_in, f_out)
            except Exception as e:
                raise IOError(f"Error leyendo el fragmento {frag_path}: {e}")
    print(f"Archivo '{output_file_path}' unido exitosamente.")