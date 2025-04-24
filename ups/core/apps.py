from django.apps import AppConfig
import threading
import os

class CoreConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'core'

    def ready(self):
        from ups_daemon import UPSDaemon

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

        daemon = UPSDaemon(world_host, world_port, amazon_url, db_config)

        # 启动 daemon 线程
        t = threading.Thread(target=daemon.start, kwargs={"world_id": None}, daemon=True)
        t.start()
