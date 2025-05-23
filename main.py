import click
import os
import sys
import shutil
import logging # Para silenciar logs de Dask
from backup_processing import (
    create_backup_archive,
    restore_from_archive,
    split_file,
    merge_files
)
from storage_backends import (
    copy_to_local_disk,
    upload_to_google_drive,
    copy_fragments_to_usb,
    download_from_google_drive
)
from dask.distributed import Client, LocalCluster

dask_client = None
dask_cluster = None
# ------------------------------------------

DEFAULT_COMPRESSION_ALGORITHM = 'zip'

@click.group()
def cli():
    """
    Sistema de Respaldo con Dask.
    Permite respaldar y restaurar carpetas, con opciones de compresión,
    encriptación y múltiples destinos de almacenamiento.
    """
    pass

@cli.command()
@click.option('--sources', '-s', multiple=True, required=True,
              type=click.Path(exists=True, file_okay=False, readable=True, resolve_path=True),
              help="Carpetas a respaldar (se pueden especificar múltiples veces).")
@click.option('--output-name', '-o', required=True,
              help="Nombre base para el archivo de backup (ej. 'mi_backup'). La extensión .zip se añadirá.")
@click.option('--temp-dir', default='./backup_temp', type=click.Path(file_okay=False, resolve_path=True),
              help="Directorio temporal para generar el backup antes de moverlo.")
@click.option('--compress', default=DEFAULT_COMPRESSION_ALGORITHM,
              type=click.Choice(['zip']), 
              help="Algoritmo de compresión.")
@click.option('--encrypt', is_flag=True, help="Encriptar el archivo de backup (AES-256 para ZIP).")
@click.option('--password', help="Contraseña para encriptación. Requerido si --encrypt está activo.")
@click.option('--storage-type', type=click.Choice(['local', 'gdrive', 'usb']), default='local',
              help="Tipo de almacenamiento para el backup.")
@click.option('--storage-path', required=True,
              help="Ruta para 'local'/'usb' (ej. /mnt/externo) o ID de carpeta para 'gdrive'.")
@click.option('--fragment-size', type=int, default=0,
              help="Tamaño de fragmento en MB para USB (0 para no fragmentar).")
def backup(sources, output_name, temp_dir, compress, encrypt, password, storage_type, storage_path, fragment_size):
    """Crea un nuevo respaldo."""
    if encrypt and not password:
        click.echo("Error: La contraseña es requerida para encriptación (--password).", err=True)
        sys.exit(1)
    
    if storage_type == 'gdrive' and not storage_path:
        click.echo("Error: Se requiere --storage-path (ID de carpeta de Google Drive) para 'gdrive'.", err=True)
        sys.exit(1)

    # Asegurar que el directorio temporal exista
    os.makedirs(temp_dir, exist_ok=True)
    backup_filename = f"{output_name}.zip"
    local_temp_backup_path = os.path.join(temp_dir, backup_filename)

    click.echo(f"Iniciando respaldo de: {', '.join(sources)}")
    click.echo(f"Archivo de backup temporal: {local_temp_backup_path}")

    try:
        create_backup_archive(list(sources), local_temp_backup_path,
                              compress_type=compress,
                              password=password if encrypt else None)
        click.echo(f"Backup temporal creado en: {local_temp_backup_path}")

        file_to_store = local_temp_backup_path
        fragments_dir_local = None 

        if storage_type == 'usb' and fragment_size > 0:
            fragments_dir_local = os.path.join(temp_dir, f"{output_name}_fragments")
            os.makedirs(fragments_dir_local, exist_ok=True)
            click.echo(f"Fragmentando {local_temp_backup_path} en trozos de {fragment_size}MB...")
            split_file(local_temp_backup_path, fragments_dir_local, fragment_size * 1024 * 1024)
            click.echo(f"Fragmentos creados en: {fragments_dir_local}")
            
        click.echo(f"Almacenando en: {storage_type} en la ruta/ID: {storage_path}")
        if storage_type == 'local':
            final_dest_path = os.path.join(storage_path, backup_filename)
            copy_to_local_disk(file_to_store, final_dest_path)
            click.echo(f"Backup almacenado localmente en: {final_dest_path}")
        
        elif storage_type == 'gdrive':
            upload_to_google_drive(file_to_store, storage_path, backup_filename)
            click.echo(f"Backup subido a Google Drive (carpeta ID: {storage_path}) como {backup_filename}.")

        elif storage_type == 'usb':
            if fragments_dir_local: 
                usb_fragments_target_dir = os.path.join(storage_path, os.path.basename(fragments_dir_local))
                copy_fragments_to_usb(fragments_dir_local, usb_fragments_target_dir)
                click.echo(f"Fragmentos copiados a USB en: {usb_fragments_target_dir}")
            else: 
                final_dest_path = os.path.join(storage_path, backup_filename)
                copy_to_local_disk(file_to_store, final_dest_path)
                click.echo(f"Backup copiado a USB en: {final_dest_path}")
        
        click.echo("Proceso de backup completado.")

    except Exception as e:
        click.echo(f"Error durante el proceso de backup: {e}", err=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        click.echo(f"Considere limpiar el directorio temporal: {temp_dir} y sus contenidos: {local_temp_backup_path}, {fragments_dir_local if fragments_dir_local else ''}")


@cli.command()
@click.option('--source-type', type=click.Choice(['local', 'gdrive', 'usb']), required=True,
              help="Tipo de origen del backup.")
@click.option('--source-path', required=True,
              help="Ruta al archivo/primer fragmento ('local'/'usb') o ID de carpeta ('gdrive').")
@click.option('--backup-filename', help="Nombre del archivo de backup en GDrive o en USB fragmentado (ej. 'mi_backup.zip').")
@click.option('--restore-to', '-r', required=True, type=click.Path(file_okay=False, resolve_path=True),
              help="Carpeta destino para la restauración.")
@click.option('--temp-dir', default='./restore_temp', type=click.Path(file_okay=False, resolve_path=True),
              help="Directorio temporal para descargar/unir archivos.")
@click.option('--password', help="Contraseña si el backup está encriptado.")
@click.option('--is-fragmented', is_flag=True,
              help="Indica si el backup está fragmentado (relevante para 'usb' o 'local' si se almacenaron fragmentos).")
def restore(source_type, source_path, backup_filename, restore_to, temp_dir, password, is_fragmented):
    """Restaura archivos desde un backup."""
    os.makedirs(restore_to, exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)
    
    click.echo(f"Iniciando restauración desde {source_type}: {source_path}")

    local_backup_file_to_process = None

    try:
        if source_type == 'local':
            if is_fragmented:
                if not backup_filename:
                    click.echo("Error: --backup-filename es requerido para restaurar desde fragmentos locales.", err=True)
                    sys.exit(1)
                click.echo(f"Uniendo fragmentos desde {source_path} para el archivo base {backup_filename}...")
                local_backup_file_to_process = os.path.join(temp_dir, backup_filename)
                merge_files(source_path, backup_filename, local_backup_file_to_process)
            else:
                local_backup_file_to_process = source_path 

        elif source_type == 'gdrive':
            if not backup_filename: 
                click.echo("Error: --backup-filename es requerido para restaurar desde Google Drive.", err=True)
                sys.exit(1)
            local_backup_file_to_process = os.path.join(temp_dir, backup_filename)
            click.echo(f"Descargando {backup_filename} de Google Drive (ID de carpeta: {source_path}) a {local_backup_file_to_process}...")
            download_from_google_drive(source_path, backup_filename, local_backup_file_to_process)
            click.echo("Descarga completada.")
            
        elif source_type == 'usb':
            if not backup_filename:
                click.echo("Error: --backup-filename es requerido para restaurar desde USB.", err=True)
                sys.exit(1)

            if is_fragmented:
                usb_fragments_dir = os.path.join(source_path, f"{os.path.splitext(backup_filename)[0]}_fragments")
                click.echo(f"Uniendo fragmentos desde USB (directorio: {usb_fragments_dir}) para el archivo base {backup_filename}...")
                local_backup_file_to_process = os.path.join(temp_dir, backup_filename)
                merge_files(usb_fragments_dir, backup_filename, local_backup_file_to_process)
            else:
                file_on_usb = os.path.join(source_path, backup_filename)
                if not os.path.exists(file_on_usb):
                    click.echo(f"Error: Archivo {file_on_usb} no encontrado en el USB.", err=True)
                    sys.exit(1)
                local_backup_file_to_process = os.path.join(temp_dir, backup_filename)
                shutil.copy2(file_on_usb, local_backup_file_to_process)
                
        if not local_backup_file_to_process or not os.path.exists(local_backup_file_to_process):
            click.echo(f"Error: No se pudo obtener el archivo de backup para procesar: {local_backup_file_to_process}", err=True)
            sys.exit(1)

        click.echo(f"Restaurando desde: {local_backup_file_to_process} hacia {restore_to}")
        restore_from_archive(local_backup_file_to_process, restore_to, password=password)
        click.echo("Restauración completada.")

    except ValueError as ve: 
        click.echo(f"Error de restauración: {ve}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error durante el proceso de restauración: {e}", err=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        click.echo(f"Considere limpiar el directorio temporal: {temp_dir} y sus contenidos.")


if __name__ == '__main__':
    try:        
        num_workers = os.cpu_count() # Usar todos los cores disponibles
        print(f"Iniciando Dask LocalCluster con {num_workers} workers...")
        dask_cluster = LocalCluster(n_workers=num_workers, threads_per_worker=1) # 1 hilo por worker, procesos separados
        dask_client = Client(dask_cluster)
        print(f"Dask Client conectado: {dask_client}")
        print(f"Dask dashboard link: {dask_client.dashboard_link}")
        # ---------------------------------------
        
        cli() # Ejecutar la interfaz de línea de comandos

    except KeyboardInterrupt:
        print("\nProceso interrumpido por el usuario.")
        sys.exit(1)
    except Exception as e:
        print(f"Error general en la ejecución: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        # --- Cerrar Dask Client y LocalCluster ---
        print("Cerrando Dask Client y LocalCluster...")
        if dask_client:
            try:
                dask_client.close()
                print("Cliente Dask cerrado.")
            except Exception as e:
                print(f"Error cerrando cliente Dask: {e}", file=sys.stderr)
        if dask_cluster:
            try:
                dask_cluster.close()
                print("Cluster Dask cerrado.")
            except Exception as e:
                print(f"Error cerrando cluster Dask: {e}", file=sys.stderr)
        print("Limpieza de Dask completada.")