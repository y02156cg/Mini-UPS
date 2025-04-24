import socket
import threading
import time
import psycopg2
from psycopg2 import pool
import psycopg2.pool
import google.protobuf.io.coded_stream as coded_stream
import google.protobuf.io.zero_copy_stream_impl as zero_copy_stream

import world_ups_1_pb2 as ups_pb2

class WorldConnection:
    def __init__(self, host, port, db_pool):
        self.host = host
        self.port = port
        self.db_pool = db_pool
        self.socket = None
        self.world_id = None
        self.connected = False
        self.seq_num = 0
        self.acks = set()
        self.lock = threading.Lock()

    def connect(self, world_id=None):
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))

            connect_msg = ups_pb2.UConnect()
            if world_id is not None:
                connect_msg.worldid = world_id

            self._send_message(connect_msg)

            response = self._receive_message(ups_pb2.UConnected())

            if response.result == "connected!":
                self.world_id = response.worldid
                self.connected = True
                print(f"Connected to world {self.world_id}")
                return True
            else:
                print(f"Failed to connect: {response.result}")
                return False
        except Exception as e:
            print(f"Connection error: {e}")
            return False
        
    def disconnect(self):
        if self.connected:
            try:
                command = ups_pb2.UCommands()
                command.disconnect = True
                self._send_command(command)

                response = self._receive_message(ups_pb2.UResponse())

                if response.HasField("finished") and response.finished:
                    print("Disconnect from world simulator")

                self.socket.close()
                self.connected = False
            except Exception as e:
                print(f"Disconnect error: {e}")

    def send_pickup(self, warehouse_id, truck_id):
        with self.lock:
            try:
                command = ups_pb2.UCommands()
                pickup = command.pickups.add()
                pickup.truckid = truck_id
                pickup.whid = warehouse_id
                pickup.seqnum = self._get_next_seq_num()

                return self._send_command(command)
            except Exception as e:
                print(f"Send pickup errpr: {e}")
                return False
            
    def send_delivery(self, truck_id, package_id, x, y):
        with self.lock:
            try:
                command = ups_pb2.UCommands()
                delivery = command.deliveries.add()
                delivery.truckid = truck_id
                delivery.packageid = package_id
                delivery.x = x
                delivery.y = y
                delivery.seqnum = self._get_next_seq_num()

                return self._send_command(command)
            except Exception as e:
                print(f"Send delivery error: {e}")
                return False
            
    def query_truck(self, truck_id):
        with self.lock:
            try:
                command = ups_pb2.UCommands()
                query = command.queries.add()
                query.truckid = truck_id
                query.seqnum = self._get_next_seq_num()

                return self._send_command(command)
            except Exception as e:
                print(f"Query truck error: {e}")
                return False
            
    def set_sim_speed(self, speed):
        with self.lock:
            try:
                command = ups_pb2.UCommands()
                command.simspeed = speed

                return self._send_command(command)
            except Exception as e:
                print(f"Set sim speed error: {e}")
                return False
            
    def _get_next_seq_num(self):
        self.seq_num += 1
        return self.seq_num
    
    def _send_command(self, command):
        # Add acknowledges for received messages
        with self.lock:
            if self.acks:
                command.acks.extend(self.acks)
                self.acks.clear()

        return self._send_message(command)
    
    def _send_mesage(self, message):
        try:
            size = message.Bytesize()
            data = message.SerializeToString()

            size_bytes = self._encode_varint(size)
            self.socket.sendall(size_bytes)

            self.socket.sendall(data)
            return True
        except Exception as e:
            print(f"Send message error: {e}")
            return False
        
    def _receive_message(self, message):
        try:
            size_bytes = self._read_varint_size()
            size = self._decode_varint(size_bytes)

            data = self.socket.recv(size)

            message.ParseFromString(data)
            return message
        except Exception as e:
            print(f"Receive message error: {e}")
            return None
        
    def _encode_varint(self, value):
        result = bytearray()
        while True:
            byte = value & 0x7F # get lower 7 bits
            value >>= 7
            if value:
                byte |= 0x80 # set MSB 1
            result.append(byte)
            if not value:
                break
        return bytes(result)
    
    def _decode_varint(self, data):
        value = 0
        shift = 0
        for byte in data:
            value |= (byte & 0x7F) << shift
            if not (byte & 0x80):
                break
            shift += 7
        return value
    
    def _read_varint_size(self):
        result = bytearray()
        while True:
            byte = self.socket.recv(1)
            if not byte:
                raise Exception("Connection closed")
            result.extend(byte)
            if not (byte[0] & 0x80):
                break
        return bytes(result)
    
    def process_responses(self):
        while self.connected:
            try:
                response = self._receive_message(ups_pb2.UResponses())

                if response is None:
                    time.sleep(0.1)
                    continue

                for completion in response.completions:
                    self._handle_completion(completion)
                    self.ack.add(completion.seqnum)

                for delivered in response.delivered:
                    self._handle_delivered(delivered)
                    self.ack.add(delivered.seqnum)

                for status in response.truckstatus:
                    self._handle_truck_status(status)
                    self.acks.add(status.seqnum)

                for error in response.error:
                    self._handle_error(error)
                    self.acks.add(error.seqnum)

                if response.HasField("finished") and response.finished:
                    print("World simulator has finished processing and disconnected")
                    self.connected = False
                    break

                if self.acks:
                    self._send_acks()

            except Exception as e:
                print(f"Process responses error: {e}")
                time.sleep(1)

    def _send_acks(self):
        with self.lock:
            if self.acks:
                command = ups_pb2.UCommands()
                command.acks.extend(self.acks)
                self.acks.clear()
                self._send_mesage(command)

    def _handle_completion(self, completion):
        print(f"Completion: Truck {completion.truckid} at ({completion.x}, {completion.y})")

        conn = self.db_pool.getconn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE trucks SET status = %s, x = %s, y = %s WHERE id = %s",
                    ("idle" if completion.status == "idle" else "arrive_warehouse",
                     completion.x, completion.y, completion.truckid)
                )

                if completion.status != "idle":
                    cursor.execute(
                        "SELECT package_id FROM packages WHERE status = 'ready_for_pickup' AND warehouse_id = %s LIMIT 1",
                        (completion.whid,)
                    )
                    result = cursor.fetchone()
                    if result:
                        package_id = result[0]
                        self._notify_amazon_truck_arrived(completion.truckid, completion.whid, package_id)

            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"Database error in handle_completion: {e}")
        finally:
            self.db_pool.putconn(conn)

    def _handle_delivered(self, delivered):
        print(f"Delivered: Package {delivered.packageid}")

        conn = self.db_pool.getconn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE packages SET status = 'delivered' WHERE id = %s",
                    (delivered.packageid,)
                )

                cursor.execute(
                    "SELECT user_id FROM packages WHERE id = %s",
                    (delivered.packageid,)
                )
                result = cursor.fetchone()
                if result:
                    user_id = result[0]
                    cursor.execute(
                        "INSERT INTO notifications (user_id, message, created_at) VALUES (%s, %s, NOW())",
                        (user_id, f"Your package {delivered.packageid} has been delivered!")
                    )
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"Database error in handle_delivered: {e}")
        finally:
            self.db_pool.putconn(conn)


    def _handle_truck_status(self, status):
        print(f"Truck Status: Trucl {status.truckid} is {status.status} at ({status.x}, {status.y})")

        conn = self.db_pool.getconn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE trucks SET status = %s, x = %s, y = %s WHERE id = %s",
                    (status.status, status.x, status.y, status.truckid)
                )

            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"Database error in handle_truck_status: {e}")
        finally:
            self.db_pool.putconn(conn)

    def _handle_error(self, error):
        print(f"Error from world: {error.err} (Original seqnum: {error.originseqnum})")

        conn = self.db_pool.getconn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO error_logs (seqnum, error_message, created_at) VALUES (%s, %s, NOW())",
                    (error.originseqnum, error.err)
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"Database error in handle_error: {e}")
        finally:
            self.db_pool.putconn(conn)

    def _notify_amazon_truck_arrived(self, truck_id, warehouse_id, package_id):
        print(f"Notifying Amazon: Truck {truck_id} arrived at warehouse {warehouse_id} for package {package_id}")
        
class UPSDaemon:
    def __init__(self, world_host, world_port, db_config):
        self.world_host = world_host
        self.world_port = world_port
        self.db_pool = psycopg2.pool.ThreadedConnectionPool(1, 10, **db_config)
        self.world_connection = None
        self.amazon_handler = None
        self.running = False

    def start(self, world_id=None):
        try:
            print("Starting UPS daemon...")

            self.world_connection = WorldConnection(self.world_host, self.world_port, self.db_pool)

            if not self.world_connection.connect(world_id):
                print("Failed to connect to world simulator")
                return False
            
            response_thread = threading.Thread(target=self.world_connection.process_responses)
            response_thread.daemon = True
            response_thread.start()

            self.running = True
            print(f"UPS daemon started (World ID: {self.world_connection.world_id})")

            while self.running:
                try:
                    self._assign_idle_trucks()

                    time.sleep(1)

                except Exception as e:
                    print(f"Error in main loop: {e}")
                    time.sleep(5)

            return True
        except Exception as e:
            print(f"Error starting UPS daemon: {e}")
            return False
        
    def stop(self):
        print("Stopping UPS daemon...")
        self.running = False

        if self.world_connection:
            self.world_connection.disconnect()

        print("UPS daemon stopped")

    def _assign_idle_trucks(self):
        """Check for idle trucks and assign them to pending pickups"""
        conn = self.db_pool.getconn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id FROM trucks WHERE status = 'idle' LIMIT 5")
                idle_trucks = cursor.fetchall()

                if not idle_trucks:
                    return
                
                cursor.executed(
                    "SELECT id, warehouse_id FROM packages WHERE status = 'waiting_for_pickup' LIMIT %s",
                    (len(idle_trucks),)
                )
                pending_pickups = cursor.fetall()

                for (truck_id,), (package_id, warehouse_id) in zip(idle_trucks, pending_pickups):
                    print(f"Assigning truck {truck_id} to pick up package {package_id} from warehouse {warehouse_id}")
                    
                    cursor.execute(
                        "UPDATE packages SET status = 'pickup_assigned', truck_id = %s WHERE id = %s",
                        (truck_id, package_id)
                    )
                    
                    cursor.execute(
                        "UPDATE trucks SET status = 'traveling' WHERE id = %s",
                        (truck_id,)
                    )
                    
                    self.world_connection.send_pickup(warehouse_id, truck_id)
            
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"Database error in assign_idle_trucks: {e}")
        finally:
            self.db_pool.putconn(conn)

def main():
    db_config = {
        'dbname': 'ups_db',
        'user': 'postgres',
        'password': 'postgres_password',
        'host': 'ups-db',
        'port': 5432
    }

    world_host = 'world-simulator'
    world_port = 12345

    daemon = UPSDaemon(world_host, world_port, db_config)

    try:
        daemon.start()
        while daemon.running:
            time.sleep(1)

    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        daemon.stop()

if __name__ == "__main__":
    main()
        

