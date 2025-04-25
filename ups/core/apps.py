from django.apps import AppConfig
import threading
import os
import psycopg2
import time
import sys

def wait_for_db_ready(config, required_tables=None, timeout=30):
    """
    Wait until the database is ready and required tables are created.
    
    Args:
        config: dict, psycopg2 database config
        required_tables: list[str], list of table names to wait for (e.g., ['packages'])
        timeout: int, max wait time in seconds
    """
    if required_tables is None:
        required_tables = []

    for second in range(timeout):
        try:
            conn = psycopg2.connect(**config)
            with conn.cursor() as cursor:
                all_exist = True
                for table in required_tables:
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT 1 
                            FROM information_schema.tables 
                            WHERE table_schema = 'public' AND table_name = %s
                        );
                    """, (table,))
                    exists = cursor.fetchone()[0]
                    if not exists:
                        print(f"⏳ Waiting for table '{table}' to exist...")
                        all_exist = False
                conn.close()
                if all_exist:
                    print(f"✅ All required tables {required_tables} exist. Database is ready.")
                    return
        except Exception as e:
            print("❗ Database not ready yet:", e)

        time.sleep(1)

    raise Exception(f"❌ Timeout: Some tables {required_tables} not ready after {timeout}s.")

class CoreConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'core'



    def ready(self):
        from ups_daemon import UPSDaemon
        if 'runserver' not in sys.argv:
            return 
        
        world_host = os.environ.get('WORLD_HOST', 'localhost')
        world_port = int(os.environ.get('WORLD_PORT', 12345))
        amazon_url = os.environ.get('AMAZON_URL', 'http://amazon-service:8080')

        db_config = {
            'dbname': os.environ.get('DB_NAME', 'ups_db'),
            'user': os.environ.get('DB_USER', 'postgres'),
            'password': os.environ.get('DB_PASSWORD', 'postgres_password'),
            'host': os.environ.get('DB_HOST', 'ups-db'),
            'port': int(os.environ.get('DB_PORT', 5432)),
        }

        wait_for_db_ready(db_config, required_tables=['packages', 'items', 'notifications','trucks','warehouses','error_logs','world_state','sequence_num','amazon_messages'])

        # daemon = UPSDaemon(world_host, world_port, amazon_url, db_config)

        # 启动 daemon 线程
        # t = threading.Thread(target=daemon.start, kwargs={"world_id": None}, daemon=True)
        # t.start()
        if not hasattr(self, '_daemon_started'):
            self._daemon_started = True
            daemon = UPSDaemon(world_host, world_port, amazon_url, db_config)

            threading.Thread(target=daemon.start, kwargs={"world_id": None}, daemon=True).start()

