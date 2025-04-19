import socket
import threading
import time
import json
import logging
import psycopg2
import psycopg2.extras
import os
import sys

# Import generated Protocol Buffer classes
import world_ups_1_pb2 as ups_pb2

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('world_connection')

class WorldConnection:
    """
    Handles communication with the world simulator using Protocol Buffers.
    """
    
    def __init__(self, host, port, db_pool):
        """
        Initialize the world connection.
        
        Args:
            host: Host of the world simulator
            port: Port of the world simulator
            db_pool: Database connection pool
        """
        self.host = host
        self.port = port
        self.db_pool = db_pool
        self.socket = None
        self.world_id = None
        self.connected = False
        self.seq_num = 0
        self.acks = set()
        self.lock = threading.Lock()
        self.amazon_communication = None  # Set later
    
    def set_amazon_communication(self, amazon_communication):
        """Set the Amazon communication handler"""
        self.amazon_communication = amazon_communication
    
    def connect(self, world_id=None, trucks=None):
        """
        Connect to the world simulator.
        
        Args:
            world_id: Optional world ID to connect to existing world
            trucks: Optional list of truck data for initialization
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            logger.info(f"Connected to world simulator at {self.host}:{self.port}")
            
            # Create UConnect message
            connect_msg = ups_pb2.UConnect()
            connect_msg.isAmazon = False  # We are UPS
            
            if world_id is not None:
                connect_msg.worldid = world_id
            
            # Add initial trucks if provided
            if trucks:
                for truck in trucks:
                    truck_msg = connect_msg.trucks.add()
                    truck_msg.id = truck['id']
                    truck_msg.x = truck['x']
                    truck_msg.y = truck['y']
            
            # Send the connect message
            self._send_message(connect_msg)
            logger.info("Sent UConnect message to world")
            
            # Receive the response
            response = self._receive_message(ups_pb2.UConnected())
            
            if response and response.result == "connected!":
                self.world_id = response.worldid
                self.connected = True
                logger.info(f"Successfully connected to world {self.world_id}")
                
                # Save truck information in database if new trucks
                if trucks:
                    self._save_trucks_to_db(trucks, self.world_id)
                
                return True
            else:
                error_msg = response.result if response else "No response received"
                logger.error(f"Failed to connect to world: {error_msg}")
                return False
                
        except Exception as e:
            logger.error(f"Error connecting to world: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from the world simulator"""
        if self.connected:
            try:
                # Create UCommands with disconnect flag
                command = ups_pb2.UCommands()
                command.disconnect = True
                
                # Add pending acknowledgments
                if self.acks:
                    command.acks.extend(self.acks)
                    self.acks.clear()
                
                # Send disconnect command
                self._send_message(command)
                logger.info("Sent disconnect command to world")
                
                # Wait for finished response
                try:
                    response = self._receive_message(ups_pb2.UResponses())
                    if response and response.HasField("finished") and response.finished:
                        logger.info("World confirmed disconnect")
                except Exception as e:
                    logger.warning(f"Error receiving disconnect confirmation: {e}")
                
                # Close socket
                if self.socket:
                    self.socket.close()
                
                self.connected = False
                logger.info("Disconnected from world simulator")
                
            except Exception as e:
                logger.error(f"Error disconnecting from world: {e}")
    
    def _save_trucks_to_db(self, trucks, world_id):
        """
        Save truck information to the database.
        
        Args:
            trucks: List of truck data
            world_id: World ID
        """
        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cursor:
                for truck in trucks:
                    cursor.execute(
                        """
                        INSERT INTO trucks (id, status, x, y, world_id) 
                        VALUES (%s, 'idle', %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE 
                        SET status = 'idle', x = %s, y = %s, world_id = %s
                        """,
                        (truck['id'], truck['x'], truck['y'], world_id, 
                         truck['x'], truck['y'], world_id)
                    )
                conn.commit()
            logger.info(f"Saved {len(trucks)} trucks to database for world {world_id}")
        except Exception as e:
            logger.error(f"Database error in save_trucks_to_db: {e}")
            if conn:
                conn.rollback()
        finally:
            self.db_pool.putconn(conn)
    
    def _get_next_seq_num(self):
        """Get the next sequence number for commands"""
        with self.lock:
            self.seq_num += 1
            return self.seq_num
    
    def send_pickup(self, truck_id, warehouse_id):
        """
        Send a truck to pick up packages from a warehouse.
        
        Args:
            truck_id: ID of the truck
            warehouse_id: ID of the warehouse
            
        Returns:
            bool: True if successful, False otherwise
        """
        with self.lock:
            try:
                # Create UCommands message
                command = ups_pb2.UCommands()
                
                # Create UGoPickup message
                pickup = command.pickups.add()
                pickup.truckid = truck_id
                pickup.whid = warehouse_id
                pickup.seqnum = self._get_next_seq_num()
                
                # Add acknowledgments
                if self.acks:
                    command.acks.extend(self.acks)
                    self.acks.clear()
                
                # Send command to world
                success = self._send_message(command)
                
                if success:
                    logger.info(f"Sent pickup command: Truck {truck_id} to Warehouse {warehouse_id} (seqnum: {pickup.seqnum})")
                    
                    # Log command to database
                    conn = self.db_pool.getconn()
                    try:
                        with conn.cursor() as cursor:
                            cursor.execute(
                                """
                                INSERT INTO command_logs 
                                (seq_num, command_type, command_data, created_at) 
                                VALUES (%s, 'pickup', %s, NOW())
                                """,
                                (pickup.seqnum, json.dumps({
                                    'truck_id': truck_id,
                                    'warehouse_id': warehouse_id
                                }))
                            )
                            conn.commit()
                    except Exception as e:
                        logger.error(f"Database error updating sim speed: {e}")
                        if conn:
                            conn.rollback()
                    finally:
                        self.db_pool.putconn(conn)
                
                return success
                
            except Exception as e:
                logger.error(f"Error setting simulation speed: {e}")
                return False
    
    def _send_message(self, message):
        """
        Send a Protocol Buffer message to the world simulator.
        
        Args:
            message: Protocol Buffer message to send
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not self.socket:
                logger.error("Socket not connected")
                return False
            
            # Serialize the message
            serialized = message.SerializeToString()
            
            # Get the size of the message
            size = len(serialized)
            
            # Encode the size as varint32
            size_bytes = bytearray()
            while True:
                byte = size & 0x7F
                size >>= 7
                if size:
                    byte |= 0x80
                size_bytes.append(byte)
                if not size:
                    break
            
            # Send the size followed by the message
            self.socket.sendall(bytes(size_bytes))
            self.socket.sendall(serialized)
            
            return True
            
        except Exception as e:
            logger.error(f"Error sending message to world: {e}")
            self.connected = False
            return False
    
    def _receive_message(self, message_type):
        """
        Receive a Protocol Buffer message from the world simulator.
        
        Args:
            message_type: Type of message to receive (used as template)
            
        Returns:
            Protocol Buffer message or None if error
        """
        try:
            if not self.socket:
                logger.error("Socket not connected")
                return None
            
            # Read the size varint
            size_bytes = bytearray()
            while True:
                byte = self.socket.recv(1)
                if not byte:
                    logger.error("Connection closed while reading size")
                    self.connected = False
                    return None
                
                size_bytes.append(byte[0])
                if not (byte[0] & 0x80):
                    break
            
            # Decode the size varint
            size = 0
            for i, byte in enumerate(size_bytes):
                size |= (byte & 0x7F) << (7 * i)
            
            # Read the message data
            data = bytearray()
            while len(data) < size:
                chunk = self.socket.recv(size - len(data))
                if not chunk:
                    logger.error("Connection closed while reading message")
                    self.connected = False
                    return None
                data.extend(chunk)
            
            # Parse the message
            message = type(message_type)()
            message.ParseFromString(bytes(data))
            
            return message
            
        except Exception as e:
            logger.error(f"Error receiving message from world: {e}")
            self.connected = False
            return None
    
    def process_responses(self):
        """Process responses from the world simulator"""
        logger.info("Starting world response processor")
        
        while self.connected:
            try:
                # Receive UResponses
                response = self._receive_message(ups_pb2.UResponses())
                
                if not response:
                    logger.warning("No response received, retrying in 1 second")
                    time.sleep(1)
                    continue
                
                # Process completions (UFinished)
                for completion in response.completions:
                    self._handle_completion(completion)
                    self.acks.add(completion.seqnum)
                
                # Process delivered packages (UDeliveryMade)
                for delivered in response.delivered:
                    self._handle_delivered(delivered)
                    self.acks.add(delivered.seqnum)
                
                # Process truck status responses (UTruck)
                for status in response.truckstatus:
                    self._handle_truck_status(status)
                    self.acks.add(status.seqnum)
                
                # Process errors (UErr)
                for error in response.error:
                    self._handle_error(error)
                    self.acks.add(error.seqnum)
                
                # Process acknowledgments
                for ack in response.acks:
                    self._handle_ack(ack)
                
                # Check if disconnect finished
                if response.HasField("finished") and response.finished:
                    logger.info("World simulator has finished processing and disconnected")
                    self.connected = False
                    break
                
                # Send acknowledgments
                if self.acks:
                    self._send_acks()
            
            except Exception as e:
                logger.error(f"Error processing world responses: {e}")
                time.sleep(1)
        
        logger.info("World response processor stopped")
    
    def _send_acks(self):
        """Send acknowledgments for received messages"""
        with self.lock:
            if self.acks:
                # Create UCommands message with only acknowledgments
                command = ups_pb2.UCommands()
                command.acks.extend(self.acks)
                
                # Send command to world
                if self._send_message(command):
                    logger.debug(f"Sent {len(self.acks)} acknowledgments to world")
                    self.acks.clear()
    
    def _handle_ack(self, ack_num):
        """
        Handle an acknowledgment from the world simulator.
        
        Args:
            ack_num: Sequence number being acknowledged
        """
        logger.debug(f"Received acknowledgment for seqnum {ack_num}")
        
        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cursor:
                # Mark command as acknowledged
                cursor.execute(
                    "UPDATE command_logs SET acknowledged_at = NOW() WHERE seq_num = %s",
                    (ack_num,)
                )
                conn.commit()
        except Exception as e:
            logger.error(f"Database error handling acknowledgment: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                self.db_pool.putconn(conn)
    
    def _handle_completion(self, completion):
        """
        Handle a completion notification (UFinished).
        
        This is called when:
        (a) A truck reaches a warehouse for pickup
        (b) A truck finishes all deliveries
        
        Args:
            completion: UFinished message
        """
        logger.info(f"Completion: Truck {completion.truckid} at ({completion.x}, {completion.y}) with status '{completion.status}'")
        
        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cursor:
                # Update truck location and status
                cursor.execute(
                    "UPDATE trucks SET status = %s, x = %s, y = %s WHERE id = %s",
                    (completion.status, completion.x, completion.y, completion.truckid)
                )
                
                # Case (a): Truck arrived at warehouse
                if completion.status == "arrive warehouse":
                    # Find warehouse at this location
                    cursor.execute(
                        "SELECT id FROM warehouses WHERE x = %s AND y = %s",
                        (completion.x, completion.y)
                    )
                    warehouse_result = cursor.fetchone()
                    
                    if warehouse_result:
                        warehouse_id = warehouse_result[0]
                        
                        # Check for packages waiting at this warehouse for this truck
                        cursor.execute(
                            """
                            SELECT p.id, p.user_id 
                            FROM packages p 
                            WHERE p.warehouse_id = %s AND p.truck_id = %s
                            AND p.status IN ('pickup_assigned', 'ready_for_pickup')
                            """,
                            (warehouse_id, completion.truckid)
                        )
                        packages = cursor.fetchall()
                        
                        if packages:
                            # Notify Amazon that truck has arrived at warehouse
                            if self.amazon_communication:
                                self.amazon_communication.notify_truck_arrived(completion.truckid, warehouse_id)
                            else:
                                # Queue notification for later
                                cursor.execute(
                                    """
                                    INSERT INTO amazon_message
                                    (message_type, message_content, status, created_at) 
                                    VALUES ('truck_arrived', %s, 'pending', NOW())
                                    """,
                                    (json.dumps({
                                        'truck_id': completion.truckid,
                                        'warehouse_id': warehouse_id
                                    }),)
                                )
                            
                            # Create notifications for users
                            for package_id, user_id in packages:
                                if user_id:
                                    cursor.execute(
                                        """
                                        INSERT INTO notifications (user_id, message, created_at)
                                        VALUES (%s, %s, NOW())
                                        """,
                                        (user_id, f"Truck {completion.truckid} has arrived at the warehouse for your package {package_id}")
                                    )
                    else:
                        logger.warning(f"No warehouse found at location ({completion.x}, {completion.y})")
                
                # Case (b): Truck finished deliveries
                elif completion.status == "idle":
                    # Get packages that were being delivered by this truck
                    cursor.execute(
                        """
                        SELECT id FROM packages 
                        WHERE truck_id = %s AND status = 'delivering'
                        """,
                        (completion.truckid,)
                    )
                    undelivered_packages = cursor.fetchall()
                    
                    # This shouldn't happen normally as we should receive UDeliveryMade for each package
                    # But just in case, mark any remaining packages as delivered
                    for package_id, in undelivered_packages:
                        logger.warning(f"Package {package_id} marked as delivered due to truck {completion.truckid} becoming idle")
                        
                        cursor.execute(
                            """
                            UPDATE packages 
                            SET status = 'delivered', updated_at = NOW()
                            WHERE id = %s
                            """,
                            (package_id,)
                        )
                        
                        # Get user for notification
                        cursor.execute(
                            "SELECT user_id FROM packages WHERE id = %s",
                            (package_id,)
                        )
                        user_result = cursor.fetchone()
                        
                        if user_result and user_result[0]:
                            # Create notification
                            cursor.execute(
                                """
                                INSERT INTO notifications (user_id, message, created_at)
                                VALUES (%s, %s, NOW())
                                """,
                                (user_result[0], f"Your package {package_id} has been delivered")
                            )
                        
                        # Notify Amazon
                        if self.amazon_communication:
                            self.amazon_communication.notify_package_delivered(
                                package_id,
                                completion.truckid,
                                completion.x,
                                completion.y
                            )
                    
                    # Check for pending pickups to assign to this truck
                    cursor.execute(
                        """
                        SELECT id, warehouse_id FROM packages 
                        WHERE status = 'waiting_for_pickup' AND truck_id IS NULL
                        LIMIT 1
                        """
                    )
                    pending_pickup = cursor.fetchone()
                    
                    if pending_pickup:
                        package_id, warehouse_id = pending_pickup
                        
                        # Assign truck to this package
                        cursor.execute(
                            """
                            UPDATE packages 
                            SET truck_id = %s, status = 'pickup_assigned', updated_at = NOW() 
                            WHERE id = %s
                            """,
                            (completion.truckid, package_id)
                        )
                        
                        cursor.execute(
                            "UPDATE trucks SET status = 'traveling' WHERE id = %s",
                            (completion.truckid,)
                        )
                        
                        # Send pickup command
                        conn.commit()  # Commit before sending command
                        self.send_pickup(completion.truckid, warehouse_id)
                
                conn.commit()
        
        except Exception as e:
            logger.error(f"Error handling completion: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                self.db_pool.putconn(conn)
    
    def _handle_delivered(self, delivered):
        """
        Handle a package delivery notification (UDeliveryMade).
        
        Args:
            delivered: UDeliveryMade message
        """
        logger.info(f"Delivered: Package {delivered.packageid} by truck {delivered.truckid}")
        
        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cursor:
                # Find the actual package ID if we're using numeric IDs
                package_id_str = str(delivered.packageid)
                
                # If this is a hash or numeric conversion, try to find the real package ID
                if not package_id_str.isalpha() and int(delivered.packageid) > 1000000:
                    cursor.execute(
                        """
                        SELECT id FROM packages 
                        WHERE truck_id = %s AND status = 'delivering'
                        """,
                        (delivered.truckid,)
                    )
                    results = cursor.fetchall()
                    if results:
                        if len(results) == 1:
                            package_id_str = results[0][0]
                        else:
                            logger.warning(f"Multiple packages found for truck {delivered.truckid}, can't determine which was delivered")
                
                # Update package status
                cursor.execute(
                    """
                    UPDATE packages 
                    SET status = 'delivered', updated_at = NOW()
                    WHERE id = %s AND status = 'delivering'
                    RETURNING user_id
                    """,
                    (package_id_str,)
                )
                result = cursor.fetchone()
                
                if result:
                    user_id = result[0]
                    
                    # Create notification for user
                    if user_id:
                        cursor.execute(
                            """
                            INSERT INTO notifications (user_id, message, created_at)
                            VALUES (%s, %s, NOW())
                            """,
                            (user_id, f"Your package {package_id_str} has been delivered!")
                        )
                    
                    # Get truck location
                    cursor.execute(
                        "SELECT x, y FROM trucks WHERE id = %s",
                        (delivered.truckid,)
                    )
                    location = cursor.fetchone()
                    
                    if location:
                        x, y = location
                        
                        # Notify Amazon
                        if self.amazon_communication:
                            self.amazon_communication.notify_package_delivered(
                                package_id_str,
                                delivered.truckid,
                                x,
                                y
                            )
                        else:
                            # Queue notification for later
                            cursor.execute(
                                """
                                INSERT INTO amazon_message
                                (message_type, message_content, status, created_at) 
                                VALUES ('package_delivered', %s, 'pending', NOW())
                                """,
                                (json.dumps({
                                    'package_id': package_id_str,
                                    'truck_id': delivered.truckid,
                                    'x': x,
                                    'y': y
                                }),)
                            )
                else:
                    logger.warning(f"Package {package_id_str} not found or not in 'delivering' status")
                
                conn.commit()
        
        except Exception as e:
            logger.error(f"Error handling delivered notification: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                self.db_pool.putconn(conn)
    
    def _handle_truck_status(self, status):
        """
        Handle a truck status response (UTruck).
        
        Args:
            status: UTruck message
        """
        logger.info(f"Truck Status: Truck {status.truckid} is '{status.status}' at ({status.x}, {status.y})")
        
        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cursor:
                # Update truck status and location in database
                cursor.execute(
                    "UPDATE trucks SET status = %s, x = %s, y = %s, updated_at = NOW() WHERE id = %s",
                    (status.status, status.x, status.y, status.truckid)
                )
                
                # Notify any connected clients about truck status change
                cursor.execute(
                    """
                    SELECT user_id FROM user_truck_subscriptions 
                    WHERE truck_id = %s
                    """,
                    (status.truckid,)
                )
                subscribers = cursor.fetchall()
                
                # Create notifications for users tracking this truck
                for user_id, in subscribers:
                    cursor.execute(
                        """
                        INSERT INTO notifications (user_id, message, created_at)
                        VALUES (%s, %s, NOW())
                        """,
                        (user_id, f"Truck {status.truckid} is now {status.status} at location ({status.x}, {status.y})")
                    )
                
                # Check if this is a truck that recently finished loading packages
                # and should now be sent for delivery
                if status.status == "arrive warehouse":
                    cursor.execute(
                        """
                        SELECT COUNT(*) FROM packages
                        WHERE truck_id = %s AND status = 'loaded'
                        """,
                        (status.truckid,)
                    )
                    loaded_packages_count = cursor.fetchone()[0]
                    
                    if loaded_packages_count > 0:
                        # Get package destinations
                        cursor.execute(
                            """
                            SELECT id, delivery_x, delivery_y FROM packages
                            WHERE truck_id = %s AND status = 'loaded'
                            """,
                            (status.truckid,)
                        )
                        packages = cursor.fetchall()
                        
                        if packages:
                            # Prepare package locations for delivery
                            package_locations = []
                            for pkg_id, x, y in packages:
                                package_locations.append({
                                    'package_id': pkg_id,
                                    'x': x,
                                    'y': y
                                })
                            
                            # Commit changes before sending command
                            conn.commit()
                            
                            # Send delivery command
                            self.send_delivery(status.truckid, package_locations)
                            return  # Avoid second commit
                
                # Update truck location in any active deliveries
                if status.status == "delivering":
                    cursor.execute(
                        """
                        UPDATE active_deliveries 
                        SET current_x = %s, current_y = %s, updated_at = NOW()
                        WHERE truck_id = %s
                        """,
                        (status.x, status.y, status.truckid)
                    )
                    
                    # Calculate and update ETA for packages being delivered by this truck
                    cursor.execute(
                        """
                        SELECT p.id, p.delivery_x, p.delivery_y, 
                               SQRT(POWER(p.delivery_x - %s, 2) + POWER(p.delivery_y - %s, 2)) as distance
                        FROM packages p
                        WHERE p.truck_id = %s AND p.status = 'delivering'
                        """,
                        (status.x, status.y, status.truckid)
                    )
                    packages = cursor.fetchall()
                    
                    for pkg_id, dest_x, dest_y, distance in packages:
                        # Assuming average speed of 1 unit per simulation tick
                        # Multiply by current simulation speed to get approximate ETA
                        cursor.execute(
                            "SELECT sim_speed FROM world_state WHERE world_id = %s",
                            (self.world_id,)
                        )
                        result = cursor.fetchone()
                        
                        if result:
                            sim_speed = result[0]
                            eta_minutes = int(distance / (sim_speed / 100.0))
                            
                            cursor.execute(
                                """
                                UPDATE packages 
                                SET estimated_delivery = NOW() + INTERVAL '%s minutes', 
                                    updated_at = NOW()
                                WHERE id = %s
                                """,
                                (eta_minutes, pkg_id)
                            )
                            
                            # Notify user of updated ETA
                            cursor.execute(
                                "SELECT user_id FROM packages WHERE id = %s",
                                (pkg_id,)
                            )
                            user_result = cursor.fetchone()
                            
                            if user_result and user_result[0]:
                                cursor.execute(
                                    """
                                    INSERT INTO notifications (user_id, message, created_at)
                                    VALUES (%s, %s, NOW())
                                    """,
                                    (user_result[0], f"Your package {pkg_id} is en route! Estimated delivery in {eta_minutes} minutes.")
                                )
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error handling truck status: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                self.db_pool.putconn(conn)
    
    def _handle_error(self, error):
        """
        Handle an error response (UErr).
        
        Args:
            error: UErr message
        """
        if hasattr(error, 'originseqnum'):
            logger.error(f"Error from world: {error.err} (Original seqnum: {error.originseqnum})")
        else:
            logger.error(f"Error from world: {error.err} (No sequence number provided)")
            return
        
        try:
            conn = self.db_pool.getconn()
            try:
                with conn.cursor() as cursor:
                    # Log error in database
                    cursor.execute(
                        """
                        INSERT INTO error_logs (seq_num, error_message, created_at)
                        VALUES (%s, %s, NOW())
                        """,
                        (error.originseqnum, error.err)
                    )
                    
                    # Find the original command
                    cursor.execute(
                        "SELECT command_type, command_data FROM command_logs WHERE seq_num = %s",
                        (error.originseqnum,)
                    )
                    command = cursor.fetchone()
                    
                    if command:
                        command_type, command_data = command
                        
                        try:
                            command_data = json.loads(command_data)
                        except json.JSONDecodeError:
                            logger.error(f"Failed to parse command data: {command_data}")
                            command_data = {}
                        
                        # Handle retry logic based on command type
                        if command_type == 'pickup':
                            truck_id = command_data.get('truck_id')
                            if truck_id:
                                # Reset truck status
                                cursor.execute(
                                    "UPDATE trucks SET status = 'idle' WHERE id = %s",
                                    (truck_id,)
                                )
                                
                                # Reset package status
                                cursor.execute(
                                    """
                                    UPDATE packages 
                                    SET status = 'waiting_for_pickup', truck_id = NULL, updated_at = NOW()
                                    WHERE truck_id = %s AND status = 'pickup_assigned'
                                    """,
                                    (truck_id,)
                                )
                                logger.info(f"Reset truck {truck_id} and associated packages after pickup error")
                        
                        elif command_type == 'delivery':
                            truck_id = command_data.get('truck_id')
                            if truck_id:
                                # Reset truck status
                                cursor.execute(
                                    "UPDATE trucks SET status = 'arrive_warehouse' WHERE id = %s",
                                    (truck_id,)
                                )
                                
                                # Reset package status
                                cursor.execute(
                                    """
                                    UPDATE packages 
                                    SET status = 'loaded', updated_at = NOW()
                                    WHERE truck_id = %s AND status = 'delivering'
                                    """,
                                    (truck_id,)
                                )
                                logger.info(f"Reset truck {truck_id} and associated packages after delivery error")
                        
                        elif command_type == 'query':
                            # No specific recovery needed for query errors
                            logger.info(f"No recovery needed for query error with seqnum {error.originseqnum}")
                    
                    # Notify admin about the error
                    cursor.execute(
                        """
                        INSERT INTO admin_alerts (alert_type, message, created_at)
                        VALUES ('world_error', %s, NOW())
                        """,
                        (f"World error: {error.err} (Seq: {error.originseqnum})",)
                    )
                    
                    # Schedule automatic retry if needed and possible
                    if hasattr(error, 'retry') and error.retry:
                        # Check retry count for this command
                        cursor.execute(
                            """
                            SELECT retry_count FROM command_logs 
                            WHERE seq_num = %s
                            """,
                            (error.originseqnum,)
                        )
                        result = cursor.fetchone()
                        
                        if result and result[0] < 3:  # Max 3 retries
                            retry_count = result[0] + 1
                            
                            # Update retry count
                            cursor.execute(
                                """
                                UPDATE command_logs 
                                SET retry_count = %s, updated_at = NOW()
                                WHERE seq_num = %s
                                """,
                                (retry_count, error.originseqnum)
                            )
                            
                            # Schedule retry
                            cursor.execute(
                                """
                                INSERT INTO command_retry_queue
                                (original_seq_num, retry_count, status, created_at)
                                VALUES (%s, %s, 'pending', NOW())
                                """,
                                (error.originseqnum, retry_count)
                            )
                            
                            logger.info(f"Scheduled retry #{retry_count} for command with seqnum {error.originseqnum}")
                    
                    conn.commit()
                    
            except Exception as e:
                conn.rollback()
                logger.error(f"Error handling world error: {e}")
        
        except Exception as e:
            logger.error(f"Database connection error while handling world error: {e}")
        
        finally:
            if conn:
                self.db_pool.putconn(conn)
    
    def process_retry_queue(self):
        """Process the command retry queue"""
        logger.info("Starting retry queue processor")
        
        while self.connected:
            try:
                conn = self.db_pool.getconn()
                with conn.cursor() as cursor:
                    # Get pending retries
                    cursor.execute(
                        """
                        SELECT id, original_seq_num, retry_count
                        FROM command_retry_queue
                        WHERE status = 'pending'
                        ORDER BY created_at ASC
                        LIMIT 1
                        """
                    )
                    retry = cursor.fetchone()
                    
                    if retry:
                        retry_id, original_seq_num, retry_count = retry
                        
                        # Mark as processing
                        cursor.execute(
                            """
                            UPDATE command_retry_queue
                            SET status = 'processing', updated_at = NOW()
                            WHERE id = %s
                            """,
                            (retry_id,)
                        )
                        
                        # Get original command
                        cursor.execute(
                            """
                            SELECT command_type, command_data 
                            FROM command_logs
                            WHERE seq_num = %s
                            """,
                            (original_seq_num,)
                        )
                        command = cursor.fetchone()
                        
                        if command:
                            command_type, command_data = command
                            try:
                                command_data = json.loads(command_data)
                                
                                # Commit before executing command
                                conn.commit()
                                
                                # Resend command based on type
                                success = False
                                if command_type == 'pickup':
                                    truck_id = command_data.get('truck_id')
                                    warehouse_id = command_data.get('warehouse_id')
                                    if truck_id and warehouse_id:
                                        success = self.send_pickup(truck_id, warehouse_id)
                                
                                elif command_type == 'delivery':
                                    truck_id = command_data.get('truck_id')
                                    package_locations = command_data.get('package_locations')
                                    if truck_id and package_locations:
                                        success = self.send_delivery(truck_id, package_locations)
                                
                                elif command_type == 'query':
                                    truck_id = command_data.get('truck_id')
                                    if truck_id:
                                        success = self.query_truck(truck_id)
                                
                                # Update retry status
                                conn = self.db_pool.getconn()  # Get new connection
                                with conn.cursor() as cursor:
                                    cursor.execute(
                                        """
                                        UPDATE command_retry_queue
                                        SET status = %s, completed_at = NOW()
                                        WHERE id = %s
                                        """,
                                        ('success' if success else 'failed', retry_id)
                                    )
                                    conn.commit()
                                
                                logger.info(f"Retry #{retry_count} for command {original_seq_num}: {'Success' if success else 'Failed'}")
                            
                            except Exception as e:
                                logger.error(f"Error processing retry: {e}")
                                if conn:
                                    conn.rollback()
                        else:
                            # Mark as failed if original command not found
                            cursor.execute(
                                """
                                UPDATE command_retry_queue
                                SET status = 'failed', completed_at = NOW()
                                WHERE id = %s
                                """,
                                (retry_id,)
                            )
                            conn.commit()
                
                # Sleep before checking again
                time.sleep(5)
                
            except Exception as e:
                logger.error(f"Error in retry queue processor: {e}")
                time.sleep(5)
                
            finally:
                if conn:
                    self.db_pool.putconn(conn)
        
        logger.info("Retry queue processor stopped")
    
    def send_heartbeat(self):
        """Send periodic heartbeat to keep connection alive"""
        logger.info("Starting heartbeat sender")
        
        while self.connected:
            try:
                # Create UCommands message with just acknowledgments
                command = ups_pb2.UCommands()
                
                # Send command to world
                if self._send_message(command):
                    logger.debug(f"Sent heartbeat to world")
                
                # Sleep for 30 seconds
                time.sleep(30)
                
            except Exception as e:
                logger.error(f"Error sending heartbeat: {e}")
                time.sleep(5)
        
        logger.info("Heartbeat sender stopped")
    
    def process_command_queue(self):
        """Process commands queued in the database"""
        logger.info("Starting command queue processor")
        
        while self.connected:
            try:
                conn = self.db_pool.getconn()
                with conn.cursor() as cursor:
                    # Get pending commands
                    cursor.execute(
                        """
                        SELECT id, command_type, command_data
                        FROM command_queue
                        WHERE status = 'pending'
                        ORDER BY priority DESC, created_at ASC
                        LIMIT 1
                        """
                    )
                    command = cursor.fetchone()
                    
                    if command:
                        command_id, command_type, command_data = command
                        
                        # Mark as processing
                        cursor.execute(
                            """
                            UPDATE command_queue
                            SET status = 'processing', updated_at = NOW()
                            WHERE id = %s
                            """,
                            (command_id,)
                        )
                        
                        try:
                            command_data = json.loads(command_data)
                            
                            # Commit before executing command
                            conn.commit()
                            
                            # Execute command based on type
                            success = False
                            if command_type == 'pickup':
                                truck_id = command_data.get('truck_id')
                                warehouse_id = command_data.get('warehouse_id')
                                if truck_id and warehouse_id:
                                    success = self.send_pickup(truck_id, warehouse_id)
                            
                            elif command_type == 'delivery':
                                truck_id = command_data.get('truck_id')
                                package_locations = command_data.get('package_locations')
                                if truck_id and package_locations:
                                    success = self.send_delivery(truck_id, package_locations)
                            
                            elif command_type == 'query':
                                truck_id = command_data.get('truck_id')
                                if truck_id:
                                    success = self.query_truck(truck_id)
                            
                            # Update command status
                            conn = self.db_pool.getconn()  # Get new connection
                            with conn.cursor() as cursor:
                                cursor.execute(
                                    """
                                    UPDATE command_queue
                                    SET status = %s, completed_at = NOW()
                                    WHERE id = %s
                                    """,
                                    ('success' if success else 'failed', command_id)
                                )
                                conn.commit()
                            
                        except Exception as e:
                            logger.error(f"Error processing command: {e}")
                            # Mark as failed
                            conn = self.db_pool.getconn()  # Get new connection
                            with conn.cursor() as cursor:
                                cursor.execute(
                                    """
                                    UPDATE command_queue
                                    SET status = 'failed', error_message = %s, updated_at = NOW()
                                    WHERE id = %s
                                    """,
                                    (str(e), command_id)
                                )
                                conn.commit()
                
                # Sleep before checking again
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error in command queue processor: {e}")
                time.sleep(5)
                
            finally:
                if conn:
                    self.db_pool.putconn(conn)
        
        logger.info("Command queue processor stopped")
    
    def process_amazon_messages(self):
        """Process pending messages to Amazon"""
        logger.info("Starting Amazon message processor")
        
        while self.connected:
            if not self.amazon_communication:
                logger.warning("Amazon communication not configured, waiting...")
                time.sleep(10)
                continue
            
            try:
                conn = self.db_pool.getconn()
                with conn.cursor() as cursor:
                    # Get pending messages
                    cursor.execute(
                        """
                        SELECT id, message_type, message_content
                        FROM amazon_message
                        WHERE status = 'pending'
                        ORDER BY created_at ASC
                        LIMIT 10
                        """
                    )
                    messages = cursor.fetchall()
                    
                    for msg_id, msg_type, msg_content in messages:
                        # Mark as processing
                        cursor.execute(
                            """
                            UPDATE amazon_message
                            SET status = 'processing', updated_at = NOW()
                            WHERE id = %s
                            """,
                            (msg_id,)
                        )
                        
                        try:
                            content = json.loads(msg_content)
                            success = False
                            
                            # Process based on message type
                            if msg_type == 'truck_arrived':
                                truck_id = content.get('truck_id')
                                warehouse_id = content.get('warehouse_id')
                                if truck_id and warehouse_id:
                                    self.amazon_communication.notify_truck_arrived(truck_id, warehouse_id)
                                    success = True
                            
                            elif msg_type == 'package_delivered':
                                package_id = content.get('package_id')
                                truck_id = content.get('truck_id')
                                x = content.get('x')
                                y = content.get('y')
                                if package_id and truck_id and x is not None and y is not None:
                                    self.amazon_communication.notify_package_delivered(package_id, truck_id, x, y)
                                    success = True
                            
                            # Update message status
                            cursor.execute(
                                """
                                UPDATE amazon_message
                                SET status = %s, completed_at = NOW()
                                WHERE id = %s
                                """,
                                ('success' if success else 'failed', msg_id)
                            )
                            
                        except Exception as e:
                            logger.error(f"Error processing Amazon message: {e}")
                            cursor.execute(
                                """
                                UPDATE amazon_message
                                SET status = 'failed', error_message = %s, updated_at = NOW()
                                WHERE id = %s
                                """,
                                (str(e), msg_id)
                            )
                    
                    conn.commit()
                
                # Sleep before checking again
                time.sleep(5)
                
            except Exception as e:
                logger.error(f"Error in Amazon message processor: {e}")
                time.sleep(5)
                
            finally:
                if conn:
                    self.db_pool.putconn(conn)
        
        logger.info("Amazon message processor stopped")
    
    def start(self):
        """Start all background threads for processing"""
        # Start response processor
        self.response_thread = threading.Thread(target=self.process_responses)
        self.response_thread.daemon = True
        self.response_thread.start()
        
        # Start retry queue processor
        self.retry_thread = threading.Thread(target=self.process_retry_queue)
        self.retry_thread.daemon = True
        self.retry_thread.start()
        
        # Start command queue processor
        self.command_thread = threading.Thread(target=self.process_command_queue)
        self.command_thread.daemon = True
        self.command_thread.start()
        
        # Start Amazon message processor
        self.amazon_thread = threading.Thread(target=self.process_amazon_messages)
        self.amazon_thread.daemon = True
        self.amazon_thread.start()
        
        # Start heartbeat sender
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        
        logger.info("All background processors started")

    def stop(self):
        """Stop the connection and all background threads"""
        self.disconnect()
        
        # Wait for threads to stop (they should exit when connected=False)
        if hasattr(self, 'response_thread') and self.response_thread.is_alive():
            self.response_thread.join(timeout=5)
        
        if hasattr(self, 'retry_thread') and self.retry_thread.is_alive():
            self.retry_thread.join(timeout=5)
        
        if hasattr(self, 'command_thread') and self.command_thread.is_alive():
            self.command_thread.join(timeout=5)
        
        if hasattr(self, 'amazon_thread') and self.amazon_thread.is_alive():
            self.amazon_thread.join(timeout=5)
        
        if hasattr(self, 'heartbeat_thread') and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join(timeout=5)
        
        logger.info("World connection stopped")

