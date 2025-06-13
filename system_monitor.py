import psutil
import os
import logging
import time
import datetime
import sqlite3
import schedule
import subprocess

# Logger konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='monitor.log'
)
logger = logging.getLogger('SystemMonitor')

class SystemMonitor:
    def __init__(self, db_path='market_data.db', scripts_dir='.'):
        """
        Initialisiert den System-Monitor
        
        Args:
            db_path: Pfad zur SQLite-Datenbank
            scripts_dir: Verzeichnis mit den Python-Skripten
        """
        self.db_path = db_path
        self.scripts_dir = scripts_dir
        
        # Status-Tabelle in der Datenbank erstellen
        self._create_status_table()
        
        logger.info("SystemMonitor initialized")
    
    def _create_status_table(self):
        """Erstellt die Status-Tabelle in der Datenbank"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS system_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                cpu_usage REAL,
                memory_usage REAL,
                disk_usage REAL,
                db_size INTEGER,
                data_collector_running INTEGER,
                technical_analyzer_running INTEGER,
                signal_generator_running INTEGER,
                notifier_running INTEGER
            )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("Status table created or already exists")
        except Exception as e:
            logger.error(f"Error creating status table: {str(e)}")
    
    def check_system_resources(self):
        """
        Überprüft die Systemressourcen
        
        Returns:
            Dict mit Ressourcendaten
        """
        try:
            cpu_percent = psutil.cpu_percent()
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            # Größe der Datenbankdatei
            db_size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
            
            resources = {
                'cpu_usage': cpu_percent,
                'memory_usage': memory.percent,
                'disk_usage': disk.percent,
                'db_size': db_size
            }
            
            logger.info(f"System resources: CPU {cpu_percent}%, Memory {memory.percent}%, Disk {disk.percent}%")
            return resources
        except Exception as e:
            logger.error(f"Error checking system resources: {str(e)}")
            return None
    
    def check_processes(self):
        """
        Überprüft, ob die wichtigen Prozesse laufen
        
        Returns:
            Dict mit Prozessstatus
        """
        try:
            # Namen der zu überprüfenden Skripte
            script_names = {
                'data_collector': 'run_collector.py',
                'technical_analyzer': 'run_technical_analysis.py',
                'signal_generator': 'run_signal_generator.py',
                'notifier': 'run_notifier.py'
            }
            
            # Status sammeln
            status = {}
            for key, script in script_names.items():
                # Prüfen, ob der Prozess läuft
                running = False
                for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                    try:
                        # Für Python-Prozesse die Kommandozeile prüfen
                        if 'python' in proc.info['name'].lower() and proc.info['cmdline']:
                            cmd = ' '.join(proc.info['cmdline'])
                            if script in cmd:
                                running = True
                                break
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                        pass
                
                status[f"{key}_running"] = 1 if running else 0
                logger.info(f"Process {script} is {'running' if running else 'not running'}")
            
            return status
        except Exception as e:
            logger.error(f"Error checking processes: {str(e)}")
            return None
    
    def restart_process(self, script_name):
        """
        Startet einen nicht laufenden Prozess neu
        
        Args:
            script_name: Name des zu startenden Skripts
        """
        try:
            script_path = os.path.join(self.scripts_dir, script_name)
            
            # Prüfen, ob das Skript existiert
            if not os.path.exists(script_path):
                logger.error(f"Script {script_path} does not exist")
                return False
            
            # Prozess im Hintergrund starten
            if os.name == 'nt':  # Windows
                subprocess.Popen(['start', 'python', script_path], shell=True)
            else:  # Linux/Unix
                subprocess.Popen(['python3', script_path], 
                                stdout=subprocess.DEVNULL, 
                                stderr=subprocess.DEVNULL, 
                                start_new_session=True)
            
            logger.info(f"Restarted process {script_name}")
            return True
        except Exception as e:
            logger.error(f"Error restarting process {script_name}: {str(e)}")
            return False
    
    def check_and_restart_processes(self):
        """Überprüft alle Prozesse und startet nicht laufende neu"""
        try:
            # Prozessstatus abrufen
            status = self.check_processes()
            
            if not status:
                logger.error("Failed to check process status")
                return
            
            # Mapping von Status-Schlüsseln zu Skriptnamen
            script_mapping = {
                'data_collector_running': 'run_collector.py',
                'technical_analyzer_running': 'run_technical_analysis.py',
                'signal_generator_running': 'run_signal_generator.py',
                'notifier_running': 'run_notifier.py'
            }
            
            # Nicht laufende Prozesse neustarten
            for status_key, script in script_mapping.items():
                if status.get(status_key, 0) == 0:
                    logger.warning(f"Process {script} is not running, attempting to restart")
                    self.restart_process(script)
        except Exception as e:
            logger.error(f"Error in check_and_restart_processes: {str(e)}")
    
    def save_status(self):
        """Speichert den aktuellen Systemstatus in der Datenbank"""
        try:
            # Ressourcen und Prozessstatus abrufen
            resources = self.check_system_resources()
            processes = self.check_processes()
            
            if not resources or not processes:
                logger.error("Failed to collect system status")
                return False
            
            # Daten kombinieren
            data = {
                'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                **resources,
                **processes
            }
            
            # In die Datenbank speichern
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
            INSERT INTO system_status
            (timestamp, cpu_usage, memory_usage, disk_usage, db_size, 
            data_collector_running, technical_analyzer_running, 
            signal_generator_running, notifier_running)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data['timestamp'],
                data['cpu_usage'],
                data['memory_usage'],
                data['disk_usage'],
                data['db_size'],
                data['data_collector_running'],
                data['technical_analyzer_running'],
                data['signal_generator_running'],
                data['notifier_running']
            ))
            
            conn.commit()
            conn.close()
            
            logger.info("System status saved to database")
            return True
        except Exception as e:
            logger.error(f"Error saving system status: {str(e)}")
            return False
    
    def run_monitoring(self):
        """Führt den vollständigen Monitoring-Prozess durch"""
        logger.info("Starting system monitoring")
        
        # Systemstatus speichern
        self.save_status()
        
        # Prozesse überprüfen und ggf. neustarten
        self.check_and_restart_processes()
        
        logger.info("System monitoring completed")
