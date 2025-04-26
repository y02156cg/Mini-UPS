import socket
import threading
import time
from django.utils import timezone
import json
import logging
import psycopg2
import psycopg2.extras
import os
import sys

# Import generated Protocol Buffer classes
import world_ups_1_pb2 as ups_pb2
from core.models import *
from django.db import transaction


# Configure logging
logging.Formatter.converter = time.localtime
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    filename='/app/ups/logs/logs.txt',  
                    filemode='a' )
logger = logging.getLogger('world_connection')

def update_world_connected_status(world_id, is_connected):
    """
    根据 world_id 更新 WorldState 的 is_connected 字段。
    
    Args:
        world_id (int): 要更新的世界 ID
        is_connected (bool): 要设置的连接状态

    Returns:
        bool: True 更新成功，False 更新失败
    """
    try:
        with transaction.atomic():
            world_state = WorldState.objects.filter(world_id=world_id).first()
            if world_state:
                world_state.is_connected = is_connected
                world_state.save()
                return True
            else:
                logger.warning(f"WorldState with world_id {world_id} not found")
                return False
    except Exception as e:
        logger.error(f"Error updating is_connected for world_id {world_id}: {e}")
        return False
    
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
        logger.info("World connection init started")
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
        logger.info("World connection init finished")
    
    def set_amazon_communication(self, amazon_communication):
        """Set the Amazon communication handler"""
        logger.info("World connection set amazon communication")
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
            self.socket.connect((self.host, self.port)) # If fail, exception

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
                update_world_connected_status(self.world_id, False)
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
            with transaction.atomic():
                for truck in trucks:
                    Truck.objects.update_or_create(
                        id=truck['id'],
                        defaults={
                            'status': 'idle',
                            'x': truck['x'],
                            'y': truck['y'],
                            'world_id': world_id,
                        }
                    )
            logger.info(f"Saved {len(trucks)} trucks to database for world {world_id}")
        except Exception as e:
            logger.error(f"Database error in save_trucks_to_db: {e}")
    
    def _get_next_seq_num(self):
        """Get the next sequence number for commands"""
        logger.info("into next")
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
        logger.info("Called send_pickup")
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
                    
                    # ORM: Log the command into command_logs
                    CommandLog.objects.create(
                        seq_num=pickup.seqnum,
                        command_type='pickup',
                        command_data={
                            'truck_id': truck_id,
                            'warehouse_id': warehouse_id
                        },
                        status='success',
                        created_at=timezone.now()
                    )
                else:
                    logger.info(f"Pickup command sending fails: Truck {truck_id} to Warehouse {warehouse_id} (seqnum: {pickup.seqnum})")
                    CommandLog.objects.create(
                        seq_num=pickup.seqnum,
                        command_type='pickup',
                        command_data={
                            'truck_id': truck_id,
                            'warehouse_id': warehouse_id
                        },
                        status='pending',
                        created_at=timezone.now()
                    )
                return success
            except Exception as e:
                logger.error(f"Error sending pickup command to world simulator: {e}")
                return False
    
    def _send_message(self, message):
        """
        Send a Protocol Buffer message to the world simulator.
        
        Args:
            message: Protocol Buffer message to send
            
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info("Called send message")
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
            
            logger.info("Finished sending")
            return True
            
        except Exception as e:
            logger.error(f"Error sending message to world: {e}")
            self.connected = False
            update_world_connected_status(self.world_id, False)
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
                logger.error("Socket not connected to world")
                return None
            
            # Read the size varint
            size_bytes = bytearray()
            while True:
                byte = self.socket.recv(1)
                if not byte:
                    logger.error("Connection closed while reading size")
                    self.connected = False
                    update_world_connected_status(self.world_id, False)
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
                    update_world_connected_status(self.world_id, False)
                    return None
                data.extend(chunk)
            
            # Parse the message
            message = type(message_type)()
            message.ParseFromString(bytes(data))
            
            return message
            
        except Exception as e:
            logger.error(f"Error receiving message from world: {e}")
            self.connected = False
            update_world_connected_status(self.world_id, False)
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
            updated = CommandLog.objects.filter(seq_num=ack_num).update(
                acknowledged_at=timezone.now()
            )
            if updated == 0:
                logger.warning(f"Acknowledged seqnum {ack_num} not found in command_logs")
        except Exception as e:
            logger.error(f"Database error handling acknowledgment: {e}")
    
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
            with transaction.atomic():
                # Update Truck status and location
                Truck.objects.filter(id=completion.truckid).update(
                    status=completion.status,
                    x=completion.x,
                    y=completion.y,
                    updated_at=timezone.now()
                )

                if completion.status == "arrive warehouse":
                    # Find warehouse at truck location
                    warehouse = Warehouse.objects.filter(
                        x=completion.x, y=completion.y
                    ).first()

                    if warehouse:
                        # Find packages waiting at this warehouse assigned to this truck
                        packages = Package.objects.filter(
                            warehouse_id=warehouse.id,
                            truck_id=completion.truckid,
                            status="ready_for_pickup"
                        )

                        # Notify Amazon
                        if self.amazon_communication:
                            self.amazon_communication.notify_truck_arrived(
                                completion.truckid, warehouse.id
                            )
                        else:
                            AmazonMessage.objects.create(
                                message_type='truck_arrived',
                                message_content={
                                    'truck_id': completion.truckid,
                                    'warehouse_id': warehouse.id
                                },
                                status='pending',
                                created_at=timezone.now()
                            )

                        if packages.exists():
                            # Notify users about truck arrival
                            for package in packages:
                                package.status = 'pickup_complete'
                                package.updated_at = timezone.now()
                                package.save()

                                if package.user_id:
                                    Notification.objects.create(
                                        user_id=package.user_id,
                                        message=f"Truck {completion.truckid} has arrived at the warehouse for your package {package.id}",
                                        created_at=timezone.now()
                                    )
                    else:
                        logger.warning(f"No warehouse found at location ({completion.x}, {completion.y})")

                elif completion.status == "idle":
                    # Truck finished deliveries
                    delivering_packages = Package.objects.filter(
                        truck_id=completion.truckid,
                        status='out_for_delivery'
                    )

                    for pkg in delivering_packages:
                        logger.warning(f"Package {pkg.id} marked as delivered because truck {completion.truckid} became idle.")

                        pkg.status = 'delivered'
                        pkg.updated_at = timezone.now()
                        pkg.save()

                        # Notify user
                        if pkg.user_id:
                            Notification.objects.create(
                                user_id=pkg.user_id,
                                message=f"Your package {pkg.id} has been delivered",
                                created_at=timezone.now()
                            )

                        # Notify Amazon
                        if self.amazon_communication:
                            self.amazon_communication.notify_package_delivered(
                                pkg.id,
                                completion.truckid,
                                completion.x,
                                completion.y
                            )

                    # # Try to assign new pending pickup to this truck
                    # pending_pickup = Package.objects.filter(
                    #     status='waiting_for_pickup',
                    #     truck_id__isnull=True
                    # ).order_by('created_at').first()

                    # if pending_pickup:
                    #     pending_pickup.truck_id = completion.truckid
                    #     pending_pickup.status = 'pickup_assigned'
                    #     pending_pickup.updated_at = timezone.now()
                    #     pending_pickup.save()

                    #     Truck.objects.filter(id=completion.truckid).update(
                    #         status='traveling',
                    #         updated_at=timezone.now()
                    #     )

                    #     # Important: send pickup command
                    #     self.send_pickup(completion.truckid, pending_pickup.warehouse_id)

        except Exception as e:
            logger.error(f"Error handling completion: {e}")
    
    def _handle_delivered(self, delivered):
        """
        Handle a package delivery notification (UDeliveryMade).
        
        Args:
            delivered: UDeliveryMade message
        """
        logger.info(f"Delivered: Package {delivered.packageid} by truck {delivered.truckid}")
        
        try:
            with transaction.atomic():
                package_id_str = str(delivered.packageid)

                # 处理 ID 不是字母开头，且特别大的 case
                if not package_id_str.isalpha() and int(delivered.packageid) > 1000000:
                    packages = Package.objects.filter(
                        truck_id=delivered.truckid,
                        status='delivering'
                    )

                    if packages.count() == 1:
                        package_id_str = packages.first().id
                    elif packages.count() > 1:
                        logger.warning(f"Multiple delivering packages found for truck {delivered.truckid}, can't determine which was delivered.")

                # 更新 Package 状态
                updated_package = Package.objects.filter(
                    id=package_id_str,
                    status='delivering'
                ).first()

                if updated_package:
                    updated_package.status = 'delivered'
                    updated_package.updated_at = timezone.now()
                    updated_package.save()

                    user_id = updated_package.user_id

                    # 通知用户
                    if user_id:
                        Notification.objects.create(
                            user_id=user_id,
                            message=f"Your package {package_id_str} has been delivered!",
                            created_at=timezone.now()
                        )

                    # 获取 Truck 位置信息
                    truck = Truck.objects.filter(id=delivered.truckid).first()

                    if truck:
                        x, y = truck.x, truck.y

                        # 通知 Amazon
                        if self.amazon_communication:
                            self.amazon_communication.notify_package_delivered(
                                package_id_str,
                                delivered.truckid,
                                x,
                                y
                            )
                        else:
                            AmazonMessage.objects.create(
                                message_type='package_delivered',
                                message_content={
                                    'package_id': package_id_str,
                                    'truck_id': delivered.truckid,
                                    'x': x,
                                    'y': y
                                },
                                status='pending',
                                created_at=timezone.now()
                            )
                else:
                    logger.warning(f"Package {package_id_str} not found or not in 'delivering' status")

        except Exception as e:
            logger.error(f"Error handling delivered notification (ORM version): {e}")
    
    def _handle_truck_status(self, status):
        """
        Handle a truck status response (UTruck).
        
        Args:
            status: UTruck message
        """
        logger.info(f"Truck Status: Truck {status.truckid} is '{status.status}' at ({status.x}, {status.y})")
        
        try:
            with transaction.atomic():
                # Update Truck status and location
                Truck.objects.filter(id=status.truckid).update(
                    status=status.status,
                    x=status.x,
                    y=status.y,
                    updated_at=timezone.now()
                )
                
                # If you had user subscriptions, here would query and notify them
                # You can skip it now, or define a UserTruckSubscription model later
                
                # Handle "arrive warehouse" status: check if loaded packages exist
                if status.status == "arrive warehouse":
                    loaded_packages = Package.objects.filter(
                        truck_id=status.truckid,
                        status='loaded'
                    )
                    
                    if loaded_packages.exists():
                        package_locations = [
                            {
                                'package_id': pkg.id,
                                'x': pkg.destination_x,
                                'y': pkg.destination_y
                            }
                            for pkg in loaded_packages
                        ]
                        
                        # Send delivery command
                        self.send_delivery(status.truckid, package_locations)
                        return  # Return early to avoid duplicate commit
                
                # Handle "delivering" status: update location info and ETA
                if status.status == "delivering":
                    delivering_packages = Package.objects.filter(
                        truck_id=status.truckid,
                        status='delivering'
                    )
                    
                    # Fetch simulation speed
                    world_state = WorldState.objects.filter(world_id=self.world_id).first()
                    sim_speed = world_state.sim_speed if world_state else 100  # Default to 100
                    
                    for pkg in delivering_packages:
                        distance = ((pkg.destination_x - status.x) ** 2 + (pkg.destination_y - status.y) ** 2) ** 0.5
                        if sim_speed > 0:
                            eta_minutes = int(distance / (sim_speed / 100.0))
                        else:
                            eta_minutes = 0
                        
                        pkg.estimated_delivery = timezone.now() + timezone.timedelta(minutes=eta_minutes)
                        pkg.updated_at = timezone.now()
                        pkg.save()
                        
                        # Send notification to user
                        if pkg.user_id:
                            Notification.objects.create(
                                user_id=pkg.user_id,
                                message=f"Your package {pkg.id} is en route! Estimated delivery in {eta_minutes} minutes.",
                                created_at=timezone.now()
                            )
        
        except Exception as e:
            logger.error(f"Error handling truck status: {e}")
    
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
            with transaction.atomic():
                # Log the error into ErrorLog
                ErrorLog.objects.create(
                    seq_num=error.originseqnum,
                    error_message=error.err,
                    created_at=timezone.now()
                )

                # Fetch the original command
                command_log = CommandLog.objects.filter(seq_num=error.originseqnum).first()

                if command_log:
                    command_type = command_log.command_type
                    command_data = command_log.command_data or {}

                    if isinstance(command_data, str):
                        try:
                            command_data = json.loads(command_data)
                        except json.JSONDecodeError:
                            logger.error(f"Failed to parse command data: {command_data}")
                            command_data = {}

                    truck_id = command_data.get('truck_id')

                    # Handle recovery logic
                    if command_type == 'pickup' and truck_id:
                        Truck.objects.filter(id=truck_id).update(status='idle', updated_at=timezone.now())
                        Package.objects.filter(truck_id=truck_id, status='pickup_assigned').update(
                            status='waiting_for_pickup',
                            truck_id=None,
                            updated_at=timezone.now()
                        )
                        logger.info(f"Reset truck {truck_id} and associated packages after pickup error")

                    elif command_type == 'delivery' and truck_id:
                        Truck.objects.filter(id=truck_id).update(status='arrive_warehouse', updated_at=timezone.now())
                        Package.objects.filter(truck_id=truck_id, status='delivering').update(
                            status='loaded',
                            updated_at=timezone.now()
                        )
                        logger.info(f"Reset truck {truck_id} and associated packages after delivery error")

                    elif command_type == 'query':
                        logger.info(f"No recovery needed for query error with seqnum {error.originseqnum}")

                # Schedule automatic retry if needed
                if hasattr(error, 'retry') and error.retry:
                    if command_log and command_log.retry_count < 3:
                        # Update retry count
                        command_log.retry_count += 1
                        command_log.updated_at = timezone.now()
                        command_log.save()

                        # Insert into retry queue
                        CommandRetryQueue.objects.create(
                            original_seq_num=error.originseqnum,
                            retry_count=command_log.retry_count,
                            status='pending',
                            created_at=timezone.now()
                        )
                        logger.info(f"Scheduled retry #{command_log.retry_count} for command with seqnum {error.originseqnum}")

        except Exception as e:
            logger.error(f"Error handling world error (ORM version): {e}")
    
    def process_retry_queue(self):
        """Process the command retry queue"""
        logger.info("Starting retry queue processor")
        
        while self.connected:
            try:
                # Fetch a pending retry entry
                retry_entry = CommandRetryQueue.objects.filter(
                    status='pending'
                ).order_by('created_at').first()

                if retry_entry:
                    try:
                        with transaction.atomic():
                            # Mark it as processing
                            retry_entry.status = 'processing'
                            retry_entry.updated_at = timezone.now()
                            retry_entry.save()

                            # Fetch corresponding original command
                            command_log = CommandLog.objects.filter(
                                seq_num=retry_entry.original_seq_num
                            ).first()

                            if command_log:
                                command_data = json.loads(command_log.command_data)
                                command_type = command_log.command_type
                                success = False

                                # Try to resend command
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
                                retry_entry.status = 'success' if success else 'failed'
                                retry_entry.completed_at = timezone.now()
                                retry_entry.save()

                                logger.info(f"Retry #{retry_entry.retry_count} for command {retry_entry.original_seq_num}: {'Success' if success else 'Failed'}")
                            else:
                                # Cannot find original command, mark as failed
                                retry_entry.status = 'failed'
                                retry_entry.completed_at = timezone.now()
                                retry_entry.save()

                    except Exception as e:
                        logger.error(f"Error processing retry entry {retry_entry.id}: {e}")
                        if retry_entry:
                            retry_entry.status = 'failed'
                            retry_entry.completed_at = timezone.now()
                            retry_entry.save()

                time.sleep(5)

            except Exception as e:
                logger.error(f"Error in retry queue processor: {e}")
                time.sleep(5)

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
                # Fetch a pending command ordered by priority desc, created_at asc
                command_entry = CommandLog.objects.filter(
                    status='pending'
                ).order_by('created_at').first()

                if command_entry:
                    try:
                        with transaction.atomic():
                            # Mark it as processing
                            command_entry.status = 'processing'
                            command_entry.updated_at = timezone.now()
                            command_entry.save()

                            # Parse the command data
                            command_data = command_entry.command_data
                            command_type = command_entry.command_type

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

                            # Update status based on success
                            command_entry.status = 'success' if success else 'failed'
                            command_entry.updated_at = timezone.now()
                            command_entry.save()

                            logger.info(f"Processed command {command_entry.id} ({command_type}): {'Success' if success else 'Failed'}")

                    except Exception as e:
                        logger.error(f"Error processing command {command_entry.id}: {e}")
                        command_entry.status = 'failed'
                        command_entry.error_message = str(e)
                        command_entry.updated_at = timezone.now()
                        command_entry.save()

                time.sleep(1)

            except Exception as e:
                logger.error(f"Error in command queue processor loop: {e}")
                time.sleep(5)

        logger.info("Command queue processor stopped")
    
    # def process_amazon_messages(self):
    #     """Process pending messages to Amazon"""
    #     logger.info("Starting Amazon message processor")

    #     while self.connected:
    #         if not self.amazon_communication:
    #             logger.warning("Amazon communication not configured, waiting...")
    #             time.sleep(10)
    #             continue

    #         try:
    #             # Fetch pending messages (limit 10)
    #             messages = AmazonMessage.objects.filter(
    #                 status='pending'
    #             ).order_by('created_at')[:10]

    #             for message in messages:
    #                 try:
    #                     with transaction.atomic():
    #                         # Mark as processing
    #                         message.status = 'processing'
    #                         message.updated_at = timezone.now()
    #                         message.save()

    #                         content = message.message_content
    #                         success = False

    #                         # Process based on message type
    #                         if message.message_type == 'truck_arrived':
    #                             truck_id = content.get('truck_id')
    #                             warehouse_id = content.get('warehouse_id')
    #                             if truck_id and warehouse_id:
    #                                 self.amazon_communication.notify_truck_arrived(truck_id, warehouse_id)
    #                                 success = True

    #                         elif message.message_type == 'package_delivered':
    #                             package_id = content.get('package_id')
    #                             truck_id = content.get('truck_id')
    #                             x = content.get('x')
    #                             y = content.get('y')
    #                             if package_id and truck_id and x is not None and y is not None:
    #                                 self.amazon_communication.notify_package_delivered(package_id, truck_id, x, y)
    #                                 success = True

    #                         # Update message status
    #                         message.status = 'success' if success else 'failed'
    #                         message.completed_at = timezone.now()
    #                         message.save()

    #                 except Exception as e:
    #                     logger.error(f"Error processing Amazon message {message.id}: {e}")
    #                     # If something goes wrong inside processing, fail this message
    #                     message.status = 'failed'
    #                     message.error_message = str(e)
    #                     message.updated_at = timezone.now()
    #                     message.save()

    #             time.sleep(5)

    #         except Exception as e:
    #             logger.error(f"Error in Amazon message processor: {e}")
    #             time.sleep(5)

    #     logger.info("Amazon message processor stopped")

    def query_truck(self, truck_id):
        """Send a query to get the status of a specific truck."""
        with self.lock:
            try:
                command = ups_pb2.UCommands()
                query = command.queries.add()
                query.truckid = truck_id
                query.seqnum = self._get_next_seq_num()

                if self.acks:
                    command.acks.extend(self.acks)
                    self.acks.clear()

                success = self._send_message(command)
                if success:
                    CommandLog.objects.create(
                        seq_num=query.seqnum,
                        command_type='query',
                        command_data={
                            'truck_id': truck_id
                        },
                        status='success',
                        retry_count=0
                    )
                    logger.info(f"Sent truck status query for Truck {truck_id} (seqnum: {query.seqnum})")
                else:
                    CommandLog.objects.create(
                        seq_num=query.seqnum,
                        command_type='query',
                        command_data={
                            'truck_id': truck_id
                        },
                        status='pending',
                        retry_count=0
                    )
                    logger.info(f"Fail sending truck status query for Truck {truck_id} (seqnum: {query.seqnum})")
                return success
            except Exception as e:
                logger.error(f"Error sending truck query for Truck {truck_id}: {e}")
                return False

    def send_delivery(self, truck_id, package_locations):
        """
        Send a delivery command to the world for a truck.
        
        Args:
            truck_id: ID of the truck.
            package_locations: list of dicts, each dict has 'package_id', 'x', 'y'
        
        Example of package_locations:
            [
                {"package_id": 10001, "x": 10, "y": 20},
                {"package_id": 10002, "x": 12, "y": 22},
            ]
        """
        with self.lock:
            try:
                command = ups_pb2.UCommands()
                delivery = command.deliveries.add()
                delivery.truckid = truck_id
                delivery.seqnum = self._get_next_seq_num()
                
                for pkg in package_locations:
                    loc = delivery.packages.add()
                    loc.packageid = int(pkg["package_id"])
                    loc.x = int(pkg["x"])
                    loc.y = int(pkg["y"])
                
                if self.acks:
                    command.acks.extend(self.acks)
                    self.acks.clear()
                
                success = self._send_message(command)
                if success:
                    CommandLog.objects.create(
                        seq_num=delivery.seqnum,
                        command_type='delivery',
                        command_data={
                            'truck_id': truck_id,
                            'package_locations': package_locations
                        },
                        status='success',
                        created_at=timezone.now()
                    )
                    logger.info(f"Sent delivery command: Truck {truck_id} to deliver {len(package_locations)} packages (seqnum: {delivery.seqnum})")
                else:
                    CommandLog.objects.create(
                        seq_num=delivery.seqnum,
                        command_type='delivery',
                        command_data={
                            'truck_id': truck_id,
                            'package_locations': package_locations
                        },
                        status='pending',
                        created_at=timezone.now()
                    )
                    logger.info(f"Fail sending delivery command: Truck {truck_id} to deliver {len(package_locations)} packages (seqnum: {delivery.seqnum})")
                return success

            except Exception as e:
                logger.error(f"Error sending delivery command for Truck {truck_id}: {e}")
                return False

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
        # self.amazon_thread = threading.Thread(target=self.process_amazon_messages)
        # self.amazon_thread.daemon = True
        # self.amazon_thread.start()
        
        # # Start heartbeat sender
        # self.heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        # self.heartbeat_thread.daemon = True
        # self.heartbeat_thread.start()
        
        logger.info("All background processors for world connection started")

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

