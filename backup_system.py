import os
import datetime
import logging
import zipfile
import schedule
import time
import subprocess
from pcloud import PyCloud

# Logger konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='backup.log'
)
logger = logging.getLogger('BackupSystem')

class BackupSystem:
    def __init__(self, email, password, backup_dir='backups', db_path='market_data.db'):
        """
        Initialisiert das Backup-System
        
        Args:
            email: E-Mail für pCloud
            password: Passwort für pCloud
            backup_dir: Lokales Verzeichnis für Backups
            db_path: Pfad zur SQLite-Datenbank
        """
        self.email = email
        self.password = password
        self.backup_dir = backup_dir
        self.db_path = db_path
        
        # Backup-Verzeichnis erstellen, falls es nicht existiert
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        
        logger.info("BackupSystem initialized")
    
    def create_local_backup(self):
        """
        Erstellt ein lokales Backup der Datenbank
        
        Returns:
            Pfad zur Backup-Datei oder None bei Fehler
        """
        try:
            # Zeitstempel für Dateinamen generieren
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_filename = f"market_data_backup_{timestamp}.zip"
            backup_path = os.path.join(self.backup_dir, backup_filename)
            
            # ZIP-Datei erstellen
            with zipfile.ZipFile(backup_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(self.db_path, os.path.basename(self.db_path))
            
            logger.info(f"Local backup created at {backup_path}")
            return backup_path
        except Exception as e:
            logger.error(f"Error creating local backup: {str(e)}")
            return None
    
    def upload_to_pcloud(self, file_path):
        """
        Lädt ein Backup in pCloud hoch
        
        Args:
            file_path: Pfad zur hochzuladenden Datei
            
        Returns:
            True bei Erfolg, False bei Fehler
        """
        try:
            # Bei pCloud anmelden
            pc = PyCloud(self.email, self.password, endpoint="nearest")
            
            # Backup-Ordner in pCloud erstellen oder finden
            folder_name = "TradingSignalSystem_Backups"
            
            # Prüfen, ob der Ordner bereits existiert
            folders = pc.listfolder(folderid=0)
            folder_id = 0  # Root-Ordner als Standard
            
            for item in folders['metadata']['contents']:
                if item['name'] == folder_name and item['isfolder']:
                    folder_id = item['folderid']
                    break
            
            # Ordner erstellen, falls er nicht existiert
            if folder_id == 0:
                result = pc.createfolder(name=folder_name, folderid=0)
                if 'metadata' in result and 'folderid' in result['metadata']:
                    folder_id = result['metadata']['folderid']
                else:
                    logger.error("Failed to create backup folder in pCloud")
                    return False
            
            # Datei hochladen
            with open(file_path, 'rb') as f:
                file_data = f.read()
                filename = os.path.basename(file_path)
                result = pc.uploadfile(
                    data=file_data,
                    filename=filename,
                    folderid=folder_id
                )
            
            if 'metadata' in result and 'fileid' in result['metadata']:
                logger.info(f"Backup {filename} uploaded to pCloud successfully")
                return True
            else:
                logger.error("Failed to upload backup to pCloud")
                return False
        except Exception as e:
            logger.error(f"Error uploading to pCloud: {str(e)}")
            return False
    
    def cleanup_old_backups(self, keep_days=30):
        """
        Bereinigt alte lokale Backups
        
        Args:
            keep_days: Anzahl der Tage, für die Backups behalten werden sollen
        """
        try:
            now = datetime.datetime.now()
            cutoff = now - datetime.timedelta(days=keep_days)
            
            for filename in os.listdir(self.backup_dir):
                if filename.startswith("market_data_backup_") and filename.endswith(".zip"):
                    filepath = os.path.join(self.backup_dir, filename)
                    file_time = datetime.datetime.fromtimestamp(os.path.getmtime(filepath))
                    
                    if file_time < cutoff:
                        os.remove(filepath)
                        logger.info(f"Removed old backup: {filepath}")
            
            logger.info(f"Cleanup completed, removed backups older than {keep_days} days")
        except Exception as e:
            logger.error(f"Error cleaning up old backups: {str(e)}")
    
    def perform_backup(self):
        """Führt den vollständigen Backup-Prozess durch"""
        logger.info("Starting backup process")
        
        # Lokales Backup erstellen
        backup_path = self.create_local_backup()
        if not backup_path:
            logger.error("Backup process failed at local backup creation")
            return False
        
        # Backup in pCloud hochladen
        success = self.upload_to_pcloud(backup_path)
        if not success:
            logger.error("Backup process failed at pCloud upload")
            return False
        
        # Alte Backups bereinigen
        self.cleanup_old_backups()
        
        logger.info("Backup process completed successfully")
        return True
