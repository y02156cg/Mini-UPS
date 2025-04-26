# in ups_daemon
def initialize_database(self):
    """创建所需的数据库表（如果不存在）"""
    try:
        conn = self.db_pool.getconn()
        with conn.cursor() as cursor:
            # 创建卡车表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS trucks (
                id INTEGER PRIMARY KEY,
                status VARCHAR(20) NOT NULL,
                x INTEGER NOT NULL,
                y INTEGER NOT NULL,
                world_id BIGINT,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
            """)
            
            # 创建仓库表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS warehouses (
                id INTEGER PRIMARY KEY,
                x INTEGER NOT NULL,
                y INTEGER NOT NULL,
                world_id BIGINT,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
            """)
            
            # 创建包裹表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS packages (
                id VARCHAR(50) PRIMARY KEY,
                user_id VARCHAR(50),
                warehouse_id INTEGER REFERENCES warehouses(id),
                truck_id INTEGER REFERENCES trucks(id),
                status VARCHAR(20) NOT NULL,
                destination_x INTEGER NOT NULL,
                destination_y INTEGER NOT NULL,
                description TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
            """)
            
            # 创建命令日志表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS command_logs (
                id SERIAL PRIMARY KEY,
                seq_num BIGINT NOT NULL,
                command_type VARCHAR(20) NOT NULL,
                command_data JSONB NOT NULL,
                created_at TIMESTAMP NOT NULL,
                acknowledged_at TIMESTAMP,
                retry_count INTEGER DEFAULT 0
            );
            """)
            
            # 创建错误日志表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS error_logs (
                id SERIAL PRIMARY KEY,
                seq_num BIGINT,
                error_message TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL
            );
            """)
            
            # 创建用户通知表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS notifications (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(50) NOT NULL,
                message TEXT NOT NULL,
                read BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
            """)
            
            # 创建Amazon消息队列表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS amazon_messages (
                id SERIAL PRIMARY KEY,
                message_type VARCHAR(50) NOT NULL,
                message_content JSONB NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                created_at TIMESTAMP NOT NULL,
                processed_at TIMESTAMP
            );
            """)
            
            # 创建世界状态表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS world_state (
                world_id BIGINT PRIMARY KEY,
                sim_speed INTEGER DEFAULT 100,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
            """)
            
            # 创建命令重试队列
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS command_retry_queue (
                id SERIAL PRIMARY KEY,
                original_seq_num BIGINT NOT NULL,
                retry_count INTEGER NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                created_at TIMESTAMP NOT NULL,
                completed_at TIMESTAMP
            );
            """)
            
            conn.commit()
            logger.info("数据库表初始化完成")
    
    except Exception as e:
        logger.error(f"初始化数据库时出错: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            self.db_pool.putconn(conn)