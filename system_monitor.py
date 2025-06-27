import os
import sys
import time
import shutil
import sqlite3
import logging
import argparse
import subprocess
import schedule
import psutil
import datetime
from datetime import datetime, timedelta

# Logger konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='monitor.log'
)
logger = logging.getLogger('SystemMonitor')

# Add console handler for terminal output
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)

class SystemMonitor:
    def __init__(self, db_path='market_data.db', scripts_dir='.', backup_dir='backups'):
        """
        Initialisiert den System-Monitor
        
        Args:
            db_path: Pfad zur SQLite-Datenbank
            scripts_dir: Verzeichnis mit den Python-Skripten
            backup_dir: Verzeichnis für Datenbank-Backups
        """
        self.db_path = db_path
        self.scripts_dir = scripts_dir
        self.backup_dir = backup_dir
        
        # Backup-Verzeichnis erstellen, wenn es nicht existiert
        os.makedirs(self.backup_dir, exist_ok=True)
        
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
                ml_processor_running INTEGER,
                status TEXT
            )
            ''')
            
            conn.commit()
            conn.close()
        except sqlite3.Error as e:
            logger.error(f"Error creating status table: {e}")
    
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
    
    def create_backup(self, custom_name=None):
        """
        Erstellt ein Backup der Datenbank
        
        Args:
            custom_name: Optionaler benutzerdefinierter Name für das Backup
            
        Returns:
            Pfad zur erstellten Backup-Datei
        """
        if not os.path.exists(self.db_path):
            logger.error(f"Datenbank nicht gefunden: {self.db_path}")
            return None
            
        # Backup-Dateiname generieren
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if custom_name:
            backup_filename = f"{custom_name}_{timestamp}.db"
        else:
            backup_filename = f"market_data_backup_{timestamp}.db"
            
        backup_path = os.path.join(self.backup_dir, backup_filename)
        
        try:
            # Kopie erstellen
            shutil.copy2(self.db_path, backup_path)
            
            # Backup überprüfen
            backup_size = os.path.getsize(backup_path)
            original_size = os.path.getsize(self.db_path)
            
            if backup_size != original_size:
                logger.warning(f"Backup-Größe ({backup_size}) stimmt nicht mit Original ({original_size}) überein")
                
            logger.info(f"Backup erfolgreich erstellt: {backup_path}")
            return backup_path
        except Exception as e:
            logger.error(f"Backup fehlgeschlagen: {e}")
            return None
    
    def restore_backup(self, backup_path):
        """
        Stellt die Datenbank aus einem Backup wieder her
        
        Args:
            backup_path: Pfad zur Backup-Datei
            
        Returns:
            Erfolgsstatus als Boolean
        """
        if not os.path.exists(backup_path):
            logger.error(f"Backup-Datei nicht gefunden: {backup_path}")
            return False
            
        try:
            # Sicherheits-Backup der aktuellen Datenbank erstellen
            current_backup = self.create_backup(custom_name="pre_restore")
            logger.info(f"Sicherheits-Backup vor Wiederherstellung erstellt: {current_backup}")
            
            # Backup wiederherstellen
            shutil.copy2(backup_path, self.db_path)
            logger.info(f"Datenbank aus Backup wiederhergestellt: {backup_path}")
            
            return True
        except Exception as e:
            logger.error(f"Wiederherstellung fehlgeschlagen: {e}")
            return False
    
    def schedule_backups(self, interval_hours=24):
        """
        Plant regelmäßige Backups der Datenbank
        
        Args:
            interval_hours: Intervall in Stunden zwischen Backups
        """
        schedule.every(interval_hours).hours.do(self.create_backup)
        logger.info(f"Regelmäßige Backups alle {interval_hours} Stunden geplant")
    
    def run_monitoring(self):
        """Führt den vollständigen Monitoring-Prozess durch"""
        logger.info("Starting system monitoring")
        
        # Systemstatus speichern
        self.save_status()
        
        # Prozesse überprüfen und ggf. neustarten
        self.check_and_restart_processes()
        
        # Datenbank-Backup erstellen
        self.create_backup()
        
        logger.info("System monitoring completed")
    
    def check_system_health(self):
        """
        Überprüft den Systemzustand
        
        Returns:
            Dict mit Statuswerten
        """
        # System-Ressourcen prüfen
        cpu_percent = psutil.cpu_percent()
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        # Datenbankzustand prüfen
        db_status = self._check_database_health()
        
        # Prozesse prüfen
        processes = self._check_required_processes()
        
        # Daten-Pipeline prüfen
        pipeline_status = self._check_data_pipeline()
        
        # Probleme identifizieren
        issues = []
        critical_issues = []
        
        # Ressourcen-Probleme
        if cpu_percent > 90:
            issues.append(f"Hohe CPU-Auslastung: {cpu_percent}%")
        if memory.percent > 90:
            issues.append(f"Hohe Speicherauslastung: {memory.percent}%")
        if disk.percent > 90:
            issues.append(f"Wenig Speicherplatz: {100-disk.percent}% übrig")
            
        # Datenbank-Probleme
        if not db_status['database_accessible']:
            critical_issues.append("Datenbank ist nicht zugänglich")
        if not db_status['integrity_ok']:
            critical_issues.append("Datenbank-Integrität gefährdet")
            
        # Pipeline-Probleme
        if pipeline_status.get('data_gap_days', 0) > 3:
            issues.append(f"Datenlücke von {pipeline_status['data_gap_days']} Tagen")
            
        # Prozess-Probleme
        if not processes['required_processes_running']:
            missing = processes.get('missing_processes', [])
            issues.append(f"Fehlende erforderliche Prozesse: {missing}")
            
        # Gesamtzustand
        healthy = len(issues) == 0 and len(critical_issues) == 0
        
        return {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'healthy': healthy,
            'issues': issues,
            'critical_issues': critical_issues,
            'system_info': {
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent,
                'disk_percent': disk.percent,
                'database_status': db_status,
                'pipeline_status': pipeline_status,
                'processes': processes
            }
        }
        
    def _check_database_health(self):
        """
        Überprüft den Zustand der Datenbank
        
        Returns:
            Dict mit Datenbank-Zustand
        """
        result = {
            'database_accessible': False,
            'database_size_mb': 0,
            'table_count': 0,
            'integrity_ok': False
        }
        
        if not os.path.exists(self.db_path):
            result['error'] = "Datenbank-Datei nicht gefunden"
            return result
            
        try:
            # Dateigröße
            result['database_size_mb'] = os.path.getsize(self.db_path) / (1024 * 1024)
            
            # Verbindung zur Datenbank
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Tabellenliste
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            result['table_count'] = len(tables)
            
            # Integrität prüfen
            cursor.execute("PRAGMA integrity_check")
            integrity = cursor.fetchone()[0]
            result['integrity_ok'] = (integrity == 'ok')
            
            conn.close()
            result['database_accessible'] = True
            
        except Exception as e:
            result['error'] = str(e)
            
        return result
        
    def _check_data_pipeline(self):
        """
        Überprüft den Zustand der Daten-Pipeline
        
        Returns:
            Dict mit Pipeline-Zustand
        """
        result = {}
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Prüfen, wann die letzten Daten gesammelt wurden
            query = "SELECT MAX(timestamp) FROM market_data"
            cursor = conn.cursor()
            cursor.execute(query)
            last_date = cursor.fetchone()[0]
            
            if last_date:
                # In Datetime umwandeln
                last_date = datetime.strptime(last_date.split(' ')[0], '%Y-%m-%d')
                result['last_data_date'] = last_date.strftime('%Y-%m-%d')
                
                # Tage seit letztem Daten berechnen
                days_since = (datetime.now() - last_date).days
                result['data_gap_days'] = days_since
                result['pipeline_healthy'] = days_since <= 1  # Gesund, wenn Daten nicht älter als 1 Tag
            
            conn.close()
            
        except Exception as e:
            result['error'] = str(e)
            
        return result
        
    def _check_required_processes(self):
        """
        Überprüft, ob erforderliche Prozesse laufen
        
        Returns:
            Dict mit Prozess-Status
        """
        # Liste der erforderlichen Prozessnamen
        required_processes = [
            'python',  # Für Python-Skripts
            'streamlit',  # Für Dashboard
        ]
        
        result = {
            'required_processes_running': False,
            'running_processes': [],
            'missing_processes': []
        }
        
        try:
            # Alle laufenden Prozesse
            running_procs = []
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                info = proc.info
                proc_name = info['name'].lower() if info['name'] else ""
                
                # Prüfen, ob dies einer unserer erforderlichen Prozesse ist
                if any(req in proc_name for req in required_processes):
                    # Kommandozeile prüfen, um zu bestätigen, dass es unser Skript ist
                    cmdline = ' '.join(info.get('cmdline', [])).lower()
                    if 'daxtracker' in cmdline or 'market_data' in cmdline or 'dashboard' in cmdline:
                        running_procs.append({
                            'pid': info['pid'],
                            'name': proc_name,
                            'cmdline': cmdline
                        })
            
            result['running_processes'] = running_procs
            
            # Nach bestimmten Skripten suchen
            processes_found = set()
            for proc in running_procs:
                cmdline = proc['cmdline']
                if 'dashboard.py' in cmdline:
                    processes_found.add('dashboard')
                elif 'data_collector.py' in cmdline or 'run_collector.py' in cmdline:
                    processes_found.add('data_collector')
                
            # Erforderliche Prozesse
            required_scripts = {'dashboard', 'data_collector'}  # Minimum erforderlich                # Prüfen, ob alle erforderlichen Prozesse laufen
            missing = required_scripts - processes_found
            result['missing_processes'] = list(missing)
            result['required_processes_running'] = len(missing) == 0
            
        except Exception as e:
            result['error'] = str(e)
            
        return result
    
    def run_monitoring_loop(self):
        """
        Startet den Monitoring-Loop
        """
        logger.info("Starting monitoring loop")
        
        while True:
            try:
                # System-Zustand prüfen
                health = self.check_system_health()
                
                # Status in Datenbank aktualisieren
                self._update_status(health)
                
                # Bei kritischen Problemen Warnung ausgeben
                if not health['healthy'] and health.get('critical_issues'):
                    logger.warning(f"Critical issues detected: {health['critical_issues']}")
                
                # Geplante Aufgaben ausführen (z.B. Backups)
                schedule.run_pending()
                
                # Warten
                time.sleep(60)
                
            except KeyboardInterrupt:
                logger.info("Monitoring loop stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                time.sleep(60)  # Bei Fehler kurz warten und neu versuchen
    
    def _update_status(self, health):
        """
        Aktualisiert den Status in der Datenbank
        
        Args:
            health: Systemzustand aus check_system_health()
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Aktuelle Prozess-Status
            procs = health['system_info']['processes']
            dashboard_running = any(p.get('cmdline', '').find('dashboard.py') >= 0 
                                  for p in procs.get('running_processes', []))
            collector_running = any(p.get('cmdline', '').find('data_collector.py') >= 0 
                                  for p in procs.get('running_processes', []))
            ta_running = any(p.get('cmdline', '').find('technical_analyzer.py') >= 0 
                           for p in procs.get('running_processes', []))
            sg_running = any(p.get('cmdline', '').find('signal_generator.py') >= 0 
                           for p in procs.get('running_processes', []))
            ml_running = any(p.get('cmdline', '').find('ml_processor.py') >= 0 
                           for p in procs.get('running_processes', []))
            
            # Status in DB einfügen
            cursor.execute('''
            INSERT INTO system_status 
            (timestamp, cpu_usage, memory_usage, disk_usage, db_size,
             data_collector_running, technical_analyzer_running, 
             signal_generator_running, ml_processor_running, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                health['system_info']['cpu_percent'],
                health['system_info']['memory_percent'],
                health['system_info']['disk_percent'],
                health['system_info']['database_status']['database_size_mb'],
                int(collector_running),
                int(ta_running),
                int(sg_running),
                int(ml_running),
                'healthy' if health['healthy'] else 'warning'
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error updating status: {e}")


# Command-line interface
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='DaxTracker3 System Monitor')
    parser.add_argument('--monitor', action='store_true', help='Startet den Überwachungsdienst')
    parser.add_argument('--backup', action='store_true', help='Erstellt ein Datenbank-Backup')
    parser.add_argument('--restore', help='Stellt aus einem bestimmten Backup wieder her')
    parser.add_argument('--health-check', action='store_true', help='Führt eine Systemprüfung durch')
    args = parser.parse_args()
    
    # SystemMonitor initialisieren
    monitor = SystemMonitor()
    
    if args.monitor:
        # Backup-Pläne erstellen
        monitor.schedule_backups(interval_hours=24)
        # Überwachungsschleife starten
        try:
            print("System monitoring started. Press Ctrl+C to stop.")
            monitor.run_monitoring_loop()
        except KeyboardInterrupt:
            print("Monitoring stopped.")
    
    elif args.backup:
        # Backup erstellen
        backup_path = monitor.create_backup()
        if backup_path:
            print(f"Backup created: {backup_path}")
        else:
            print("Backup failed.")
            exit(1)
    
    elif args.restore:
        # Aus Backup wiederherstellen
        success = monitor.restore_backup(args.restore)
        if success:
            print(f"Database restored from {args.restore}")
        else:
            print("Restore failed.")
            exit(1)
    
    elif args.health_check:
        # Systemzustand prüfen
        health = monitor.check_system_health()
        print(f"System health: {'HEALTHY' if health['healthy'] else 'ISSUES DETECTED'}")
        
        if health['issues']:
            print("\nIssues:")
            for issue in health['issues']:
                print(f"- {issue}")
                
        if health['critical_issues']:
            print("\nCRITICAL ISSUES:")
            for issue in health['critical_issues']:
                print(f"- {issue}")
    
    else:
        # Standardmäßig Hilfe anzeigen
        parser.print_help()
