import socket
import threading
import time
import logging
import psycopg2
import psycopg2.extras
from psycopg2 import pool
import json
import os

# 导入自定义模块
from world_connection import WorldConnection
from amazon_communication import AmazonCommunication
from core.models import *

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('ups_daemon')

class UPSDaemon:
    """
    UPS守护进程，处理与世界模拟器和Amazon系统的通信，
    以及管理包裹递送的业务逻辑。
    """
    
    def __init__(self, world_host, world_port, amazon_url, db_config):
        """
        初始化UPS守护进程。
        
        Args:
            world_host: 世界模拟器主机
            world_port: 世界模拟器端口
            amazon_url: Amazon API URL
            db_config: 数据库配置
        """
        # 创建数据库连接池
        self.db_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1, 
            maxconn=20, 
            **db_config
        )
        
        # 初始化连接
        self.world_connection = None
        self.amazon_communication = None
        
        # 配置
        self.world_host = world_host
        self.world_port = world_port
        self.amazon_url = amazon_url
        
        # 状态
        self.running = False
        self.world_id = None
        
        # 线程
        self.main_thread = None
        self.truck_assignment_thread = None
    
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
    
    def start(self, world_id=None, num_trucks=5):
        """
        启动UPS守护进程。
        
        Args:
            world_id: 可选世界ID，如果不提供则创建新世界
            num_trucks: 创建新世界时的卡车数量
            
        Returns:
            bool: 成功返回True，失败返回False
        """
        try:
            logger.info("启动UPS守护进程...")
            
            # 初始化数据库
            # self.initialize_database()
            
            # 创建WorldConnection
            self.world_connection = WorldConnection(
                self.world_host, 
                self.world_port, 
                self.db_pool
            )
            
            # 创建AmazonCommunication
            self.amazon_communication = AmazonCommunication(
                self.amazon_url, 
                self.db_pool, 
                self.world_connection
            )
            
            # 设置互相引用
            self.world_connection.set_amazon_communication(self.amazon_communication)
            
            # 连接到世界模拟器
            if world_id:
                # 连接到已存在的世界
                success = self.world_connection.connect(world_id)
                if not success:
                    logger.error(f"连接到世界 {world_id} 失败")
                    return False
                self.world_id = world_id
            else:
                # 创建新世界
                trucks = []
                for i in range(num_trucks):
                    trucks.append({
                        'id': i + 1,
                        'x': 0,  # 初始位置
                        'y': 0
                    })
                
                success = self.world_connection.connect(world_id=None, trucks=trucks)
                if not success:
                    logger.error("创建新世界失败")
                    return False
                
                self.world_id = self.world_connection.world_id
                logger.info(f"创建了新世界，ID: {self.world_id}")
                
                # 创建世界状态记录
                conn = self.db_pool.getconn()
                try:
                    # with conn.cursor() as cursor:
                    #     cursor.execute(
                    #         "INSERT INTO world_state (world_id) VALUES (%s) ON CONFLICT DO NOTHING",
                    #         (self.world_id,)
                    #     )
                    #     conn.commit()
                    WorldState.objects.get_or_create(
                        world_id=self.world_id,
                        defaults={
                            "sim_speed": 100
                        }
                    )
                except Exception as e:
                    logger.error(f"创建世界状态记录时出错: {e}")
                    if conn:
                        conn.rollback()
                finally:
                    self.db_pool.putconn(conn)
            
            # 启动通信处理
            self.running = True
            
            # 启动WorldConnection响应处理线程
            world_thread = threading.Thread(target=self.world_connection.process_responses)
            world_thread.daemon = True
            world_thread.start()
            
            # 启动AmazonCommunication处理线程
            self.amazon_communication.start()
            
            # 启动卡车分配线程
            self.truck_assignment_thread = threading.Thread(target=self._assign_idle_trucks_loop)
            self.truck_assignment_thread.daemon = True
            self.truck_assignment_thread.start()
            
            # 启动主循环
            self.main_thread = threading.Thread(target=self._main_loop)
            self.main_thread.daemon = True
            self.main_thread.start()
            
            logger.info(f"UPS守护进程已启动 (世界ID: {self.world_id})")
            return True
            
        except Exception as e:
            logger.error(f"启动UPS守护进程时出错: {e}")
            self.stop()
            return False
    
    def stop(self):
        """停止UPS守护进程"""
        logger.info("停止UPS守护进程...")
        self.running = False
        
        # 停止Amazon通信
        if self.amazon_communication:
            self.amazon_communication.stop()
        
        # 断开与世界的连接
        if self.world_connection:
            self.world_connection.disconnect()
        
        logger.info("UPS守护进程已停止")
    
    def _main_loop(self):
        """主循环，处理各种后台任务"""
        logger.info("启动主循环")
        
        while self.running:
            try:
                # 处理未完成的任务、检查系统状态等
                self._process_pending_tasks()
                
                # 监控卡车状态
                self._monitor_truck_status()
                
                # 清理过期记录
                self._cleanup_old_records()
                
                # 休眠一段时间
                time.sleep(10)
                
            except Exception as e:
                logger.error(f"主循环中出错: {e}")
                time.sleep(5)
        
        logger.info("主循环已停止")
    
    def _assign_idle_trucks_loop(self):
        """卡车分配循环"""
        logger.info("启动卡车分配循环")
        
        while self.running:
            try:
                self._assign_idle_trucks()
                time.sleep(2)  # 每2秒检查一次
            except Exception as e:
                logger.error(f"卡车分配中出错: {e}")
                time.sleep(5)
        
        logger.info("卡车分配循环已停止")
    
    def _assign_idle_trucks(self):
        """分配空闲卡车到待处理的提货任务"""
        conn = None
        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cursor:
                # 查找空闲卡车
                cursor.execute(
                    "SELECT id FROM trucks WHERE status = 'idle' LIMIT 5"
                )
                idle_trucks = cursor.fetchall()
                
                if not idle_trucks:
                    return
                
                # 查找等待提货的包裹
                cursor.execute(
                    """
                    SELECT id, warehouse_id FROM packages 
                    WHERE status = 'waiting_for_pickup' AND truck_id IS NULL
                    LIMIT %s
                    """,
                    (len(idle_trucks),)
                )
                pending_pickups = cursor.fetchall()
                
                # 分配卡车给包裹
                for (truck_id,), (package_id, warehouse_id) in zip(idle_trucks, pending_pickups):
                    logger.info(f"分配卡车 {truck_id} 提取包裹 {package_id} 从仓库 {warehouse_id}")
                    
                    # 更新包裹状态
                    cursor.execute(
                        """
                        UPDATE packages 
                        SET status = 'pickup_assigned', truck_id = %s, updated_at = NOW() 
                        WHERE id = %s
                        """,
                        (truck_id, package_id)
                    )
                    
                    # 更新卡车状态
                    cursor.execute(
                        """
                        UPDATE trucks 
                        SET status = 'traveling', updated_at = NOW() 
                        WHERE id = %s
                        """,
                        (truck_id,)
                    )
                    
                    # 提交以确保状态已更新
                    conn.commit()
                    
                    # 发送提货命令
                    self.world_connection.send_pickup(warehouse_id, truck_id)
            
        except Exception as e:
            logger.error(f"分配空闲卡车时出错: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                self.db_pool.putconn(conn)
    
    def _process_pending_tasks(self):
        """处理待处理的任务"""
        # 可以在这里添加更多任务处理逻辑
        pass
    
    def _monitor_truck_status(self):
        """监控卡车状态，查询长时间没有状态更新的卡车"""
        conn = None
        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cursor:
                # 查找长时间未更新的卡车
                cursor.execute(
                    """
                    SELECT id FROM trucks 
                    WHERE updated_at < NOW() - INTERVAL '5 minutes'
                    AND status NOT IN ('idle', 'error')
                    LIMIT 5
                    """
                )
                stale_trucks = cursor.fetchall()
                
                # 查询这些卡车的状态
                for truck_id, in stale_trucks:
                    logger.info(f"查询长时间无更新的卡车 {truck_id}")
                    self.world_connection.query_truck(truck_id)
            
        except Exception as e:
            logger.error(f"监控卡车状态时出错: {e}")
        finally:
            if conn:
                self.db_pool.putconn(conn)
    
    def _cleanup_old_records(self):
        """清理旧记录"""
        conn = None
        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cursor:
                # # 清理旧命令日志
                # cursor.execute(
                #     """
                #     DELETE FROM command_logs 
                #     WHERE created_at < NOW() - INTERVAL '7 days'
                #     """
                # )
                
                # 清理旧错误日志
                cursor.execute(
                    """
                    DELETE FROM error_logs 
                    WHERE created_at < NOW() - INTERVAL '7 days'
                    """
                )
                
                # 清理旧通知
                cursor.execute(
                    """
                    DELETE FROM notifications 
                    WHERE created_at < NOW() - INTERVAL '30 days'
                    """
                )
                
                conn.commit()
            
        except Exception as e:
            logger.error(f"清理旧记录时出错: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                self.db_pool.putconn(conn)

# 如果直接运行此文件，则启动UPS守护进程
if __name__ == "__main__":
    # 从环境变量获取配置
    world_host = os.environ.get('WORLD_HOST', 'world-simulator')
    world_port = int(os.environ.get('WORLD_PORT', 12345))
    amazon_url = os.environ.get('AMAZON_URL', 'http://amazon-service:8080')
    
    db_config = {
        'dbname': os.environ.get('DB_NAME', 'ups_db'),
        'user': os.environ.get('DB_USER', 'postgres'),
        'password': os.environ.get('DB_PASSWORD', 'postgres_password'),
        'host': os.environ.get('DB_HOST', 'ups-db'),
        'port': int(os.environ.get('DB_PORT', 5432))
    }
    
    # 创建并启动UPS守护进程
    daemon = UPSDaemon(world_host, world_port, amazon_url, db_config)
    
    # 尝试获取现有世界ID
    world_id = os.environ.get('WORLD_ID')
    if world_id:
        world_id = int(world_id)
    
    try:
        # 启动守护进程
        daemon.start(world_id=world_id)
        
        # 保持主线程运行
        while daemon.running:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("接收到中断信号")
    except Exception as e:
        logger.error(f"运行守护进程时出错: {e}")
    finally:
        daemon.stop()