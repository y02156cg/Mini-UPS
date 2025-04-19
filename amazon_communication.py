import requests
import json
import uuid
import time
import threading
import logging
import psycopg2
import psycopg2.extras
from psycopg2 import pool
import psycopg2.pool
from datetime import datetime, timezone
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('amazon_comm') # amazon communication logger name

class AmazonCommunication:
    # needs to improve based on the protocol

    def __init__(self, amazon_url, db_pool, world_connection):
        """
        Initialize the Amazon communication module.
        
        Args:
            amazon_url: Base URL for Amazon API
            db_pool: Database connection pool
            world_connection: Connection to the world simulator
        """

        self.amazon_url = amazon_url
        self.db_pool = db_pool
        self.world_connection = world_connection
        self.running = False
        self.message_lock = threading.Lock()
        self.processed_messages = set()

    def start(self):
        """Start the Amazon communication"""
        self.running = True
        logger.info("Start the Amazon communication")

        outgoing_thread = threading.Thread(target=self._process_outgoing_messages)
        outgoing_thread.daemon = True
        outgoing_thread.start()

    def stop(self):
        """Stop the Amazon communication"""
        self.running = False
        logger.info("Stop the Amazon communication")

    def _process_outgoing_messages(self):
        """Process outgoing messages to Amazon from the database queue"""
        while self.running:
            try:
                conn = self.db_pool.getconn()
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                    cursor.execute(
                        """
                        SELECT id, message_type, message_content
                        FROM amazon_messages
                        WHERE status = 'pending'
                        ORDER BY created_at
                        LIMIT 10
                        FOR UPDATE SKIP LOCKED
                        """
                    )
                    messages = cursor.fetchall()

                    for message in messages:
                        cursor.execute(
                            "UPDATE amazon_messages SET status = 'processing' WHERE id = %s",
                            (message['id'],)
                        )
                        conn.commit()

                        success = self._send_message_to_amazon(message['message_type'], json.loads(message['message_content']))

                        status = 'completed' if success else 'failed'
                        cursor.execute(
                            "UPDATE amazon_message SET status = %s, processed_at = NOW() WHERE id = %s",
                            (status, message['id'])
                        )
                        conn.commit()

                        if not success:
                            cursor.execute(
                                """
                                SELECT COUNT(*) FROM amazon_messages 
                                WHERE status = 'failed' AND message_type = %s AND message_content = %s
                                """,
                                (message['message_type'], message['message_content'])
                            )
                            retry_count = cursor.fetchone()[0]
                            
                            if retry_count < 3:  # Retry up to 3 times
                                # Create a new message for retry with exponential backoff
                                backoff_time = 5 * (2 ** retry_count)  # 5, 10, 20 seconds
                                
                                cursor.execute(
                                    """
                                    INSERT INTO amazon_messages 
                                    (message_type, message_content, status, created_at) 
                                    VALUES (%s, %s, 'pending', NOW() + interval '%s seconds')
                                    """,
                                    (message['message_type'], message['message_content'], backoff_time)
                                )
                                conn.commit()

            except Exception as e:
                logger.error(f"Error processing outgoing messages: {e}")
                if conn:
                    conn.rollback()
            finally:
                if conn:
                    self.db_pool.putconn(conn)

            time.sleep(1)

    def _send_message_to_amazon(self, message_type, content):
        """
        Send a message to Amazon.

        Args:
            message_type: Type of message to send
            content: Message content

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if message_type == 'truck_arrived':
                message = self._notify_truck_arrived(content)
            elif message_type == 'package_loaded':
                message = self._notify_package_loaded(content)
            elif message_type == 'delivery_started':
                message = self._notify_delivery_started(content)
            elif message_type == 'package_delivered':
                message = self._notify_package_delivered(content)
            elif message_type == 'query_status':
                message = self._notify_query_status(content)
            elif message_type == 'redirect_package':
                message = self._notify_redirect_package(content)  
            else:
                logger.error(f"Unknown message type: {message_type}")
                return False

            # Send the request POST message to Amazon
            response = requests.post(
                f"{self.amazon_url}/api/ups", # send request to this url
                json=message,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )

            if response.status_code == 200:
                response_data = response.json()
                if response_data.get('status') == 'success':
                    logger.info(f"Successfully sent {message_type} message to Amazon")
                    return True
                else:
                    logger.error(f"Error from Amazon: {response_data.get('message')}")
            else:
                logger.error(f"HTTP error {response.status_code} from Amazon: {response.text}")
            return False
        
        except Exception as e:
            logger.error(f"Error sending message to Amazon: {e}")
            return False
            
    def _notify_truck_arrived(self, content):
        """
        Notify Amazon that a truck has arrived at a warehouse.

        Args:
            content: Dictionary with truck_id and warehouse_id
        """

        message = {
            "action": "truck_arrived",
            "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            "message_id": str(uuid.uuid4()),
            "truck_id": content['truck_id'],
            "warehouse_id": content['warehouse_id']
        }

        return message

    def _notify_package_loaded(self, content):
        """
        Notify Amazon that a package has been loaded onto a truck.

        Args:
            content: Dictionary with truck_id and package_id
        """

        message = {
            "action": "package_loaded",
            "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            "message_id": str(uuid.uuid4()),
            "package_id": content['package_id'],
            "truck_id": content['truck_id']
        }
        return message

    def _notify_delivery_started(self, content):
        """
        Notify Amazon that delivery has started for a package.

        Args:
            content: Dictionary with truck_id and package_id
        """

        message = {
            "action": "package_loaded",
            "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            "message_id": str(uuid.uuid4()),
            "package_id": content['package_id'],
            "truck_id": content['truck_id']
        }
        return message

    def _notify_delivery_delivered(self, content):
        """
        Notify Amazon that a package has been delivered.

        Args:
            content: Dictionary with truck_id and package_id, x, and y
        """

        message = {
            "action": "package_loaded",
            "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            "message_id": str(uuid.uuid4()),
            "package_id": content['package_id'],
            "truck_id": content['truck_id'],
            "delivery_x": content['x'],
            "delivery_y": content['y']
        }
        return message

    def _notify_query_status(self, content):
        """
        Notify Amazon that a query to status is made.

        Args:
            content: Dictionary with package_id
        """

        message = {
            "action": "package_loaded",
            "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            "message_id": str(uuid.uuid4()),
            "package_id": content['package_id'],
        }
        return message

    def _notify_redirect_package(self, content):
        """
        Notify Amazon that a package needs to be redirected.

        Args:
            content: Dictionary with package_id, new_destination_x, and new_destination_y, and user_id
        """

        message = {
            "action": "package_loaded",
            "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            "message_id": str(uuid.uuid4()),
            "package_id": content['package_id'],
            "truck_id": content['truck_id'],
            "new_destination_x": content['x'],
            "new_destination_y": content['y'],
            "user_id": content.get('user_id')
        }
        return message

    def handle_request_pickup(self, request):
        """
        Handle a request_pickup message from Amazon:

        Args:
            request: JSON request from Amazon
        
        Returns:
            dict: Response to send back to Amazon
        """

        try:
            message_id = request.get('message_id')
            with self.message_lock:
                if message_id in self.processed_messages:
                    logger.info(f"Duplicate message {message_id}, returning cached response")
                    return self._create_response('pickup_response', message_id, 'success', 
                                              tracking_number=request.get('package_id'),
                                              message="Pickup request already processed")
                
            warehouse_id = request.get('warehouse_id')
            user_id = request.get('user_id')
            destination_x = request.get('destination_x')
            destination_y = request.get('destination_y')
            description = request.get('description')
            items = request.get('items', [])

            # Generate tracking number if not provided
            package_id = request.get('package_id')
            if not package_id:
                package_id = f"UPS{int(time.time())}{uuid.uuid4().hex[:8].upper()}"

            conn = self.db_pool.getconn()
            try:
                with conn.cursor() as cursor:
                    # Create package
                    cursor.execute(
                        """
                        INSERT INTO packages
                        (id, user_id, warehouse_id, status, destination_x, destination_y, description)
                        VALUES (%s, %s, %s, 'waiting_for_pickup', %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING
                        """,
                        (package_id, user_id, warehouse_id, destination_x, destination_y, description)
                    )

                    for item in items:
                        cursor.execute(
                            """
                            INSERT INTO items (package_id, name, description, quantity) 
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (id) DO NOTHING
                            """,
                            (package_id, item.get('name'), item.get('description'), item.get('quantity'))
                        )
                    
                    # Create notification for user if they exist
                    if user_id:
                        cursor.execute(
                            "INSERT INTO notifications (user_id, message) VALUES (%s, %s)",
                            (user_id, f"A new package {package_id} has been created for you")
                        )
                    
                    conn.commit()

                    with self.message_lock:
                        self.processed_messages.add(message_id)
                        if len(self.processed_messages) > 1000:
                            self.processed_messages.pop()

                    # Return success response
                    return self._create_response('pickup_response', message_id, 'success', 
                                              tracking_number=package_id,
                                              message="Pickup request received")
                
            except Exception as e:
                conn.rollback()
                logger.error(f"Database error in handle_request_pickup: {e}")
                return self._create_response('pickup_response', message_id, 'error', 
                                          message=f"Database error: {str(e)}")
            
            finally:
                self.db_pool.putconn(conn)
                
        except Exception as e:
            logger.error(f"Error handling request_pickup: {e}")
            return self._create_response('pickup_response', request.get('message_id', 'unknown'), 'error', 
                                      message=f"Internal error: {str(e)}")
        
    
    def handle_package_ready(self, request):
        """
        Handle a package_ready message from Amazon.
        
        Args:
            request: The JSON request from Amazon
            
        Returns:
            dict: Response to send back to Amazon
        """
        try:
            # Check for duplicate message
            message_id = request.get('message_id')
            with self.message_lock:
                if message_id in self.processed_messages:
                    logger.info(f"Duplicate message {message_id}, returning cached response")
                    return self._create_response('package_ready_response', message_id, 'success', 
                                              message="Package ready notification already processed")
            
            # Extract request data
            package_id = request.get('package_id')
            
            # Update package in database
            conn = self.db_pool.getconn()
            try:
                with conn.cursor() as cursor:
                    # Update package status
                    cursor.execute(
                        "UPDATE packages SET status = 'ready_for_pickup', updated_at = NOW() WHERE id = %s",
                        (package_id,)
                    )
                    
                    # Get package info
                    cursor.execute(
                        "SELECT user_id, warehouse_id FROM packages WHERE id = %s",
                        (package_id,)
                    )
                    result = cursor.fetchone()
                    
                    if result:
                        user_id, warehouse_id = result
                        
                        # Find an idle truck to assign to pickup
                        cursor.execute(
                            "SELECT id FROM trucks WHERE status = 'idle' LIMIT 1"
                        )
                        truck_result = cursor.fetchone()
                        
                        if truck_result:
                            truck_id = truck_result[0]
                            
                            # Update truck and package
                            cursor.execute(
                                "UPDATE trucks SET status = 'traveling' WHERE id = %s",
                                (truck_id,)
                            )
                            
                            cursor.execute(
                                "UPDATE packages SET truck_id = %s, status = 'pickup_assigned', updated_at = NOW() WHERE id = %s",
                                (truck_id, package_id)
                            )
                            
                            # Create notification for user
                            if user_id:
                                cursor.execute(
                                    "INSERT INTO notifications (user_id, message) VALUES (%s, %s)",
                                    (user_id, f"Your package {package_id} is ready for pickup")
                                )
                            
                            # Send pickup command to world
                            self.world_connection.send_pickup(warehouse_id, truck_id)
                    
                    conn.commit()
                    
                    # Mark message as processed
                    with self.message_lock:
                        self.processed_messages.add(message_id)
                    
                    # Return success response
                    return self._create_response('package_ready_response', message_id, 'success', 
                                              message="Package ready notification processed")
            
            except Exception as e:
                conn.rollback()
                logger.error(f"Database error in handle_package_ready: {e}")
                return self._create_response('package_ready_response', message_id, 'error', 
                                          message=f"Database error: {str(e)}")
            
            finally:
                self.db_pool.putconn(conn)
                
        except Exception as e:
            logger.error(f"Error handling package_ready: {e}")
            return self._create_response('package_ready_response', request.get('message_id', 'unknown'), 'error', 
                                      message=f"Internal error: {str(e)}")
    
    def handle_load_package(self, request):
        """
        Handle a load_package message from Amazon.
        
        Args:
            request: The JSON request from Amazon
            
        Returns:
            dict: Response to send back to Amazon
        """
        try:
            # Check for duplicate message
            message_id = request.get('message_id')
            with self.message_lock:
                if message_id in self.processed_messages:
                    logger.info(f"Duplicate message {message_id}, returning cached response")
                    return self._create_response('load_package_response', message_id, 'success', 
                                              message="Load package request already processed")
            
            # Extract request data
            package_id = request.get('package_id')
            truck_id = request.get('truck_id')
            warehouse_id = request.get('warehouse_id')
            
            # Verify in database
            conn = self.db_pool.getconn()
            try:
                with conn.cursor() as cursor:
                    # Check if package exists and is ready
                    cursor.execute(
                        "SELECT status FROM packages WHERE id = %s",
                        (package_id,)
                    )
                    package_result = cursor.fetchone()
                    
                    if not package_result:
                        return self._create_response('load_package_response', message_id, 'error', 
                                                  message=f"Package {package_id} not found")
                    
                    package_status = package_result[0]
                    if package_status not in ['ready_for_pickup', 'pickup_assigned']:
                        return self._create_response('load_package_response', message_id, 'error', 
                                                  message=f"Package {package_id} is not ready for pickup (status: {package_status})")
                    
                    # Check if truck is at warehouse
                    cursor.execute(
                        "SELECT status FROM trucks WHERE id = %s",
                        (truck_id,)
                    )
                    truck_result = cursor.fetchone()
                    
                    if not truck_result:
                        return self._create_response('load_package_response', message_id, 'error', 
                                                  message=f"Truck {truck_id} not found")
                    
                    truck_status = truck_result[0]
                    if truck_status != 'arrive_warehouse':
                        return self._create_response('load_package_response', message_id, 'error', 
                                                  message=f"Truck {truck_id} is not at warehouse (status: {truck_status})")
                    
                    # Update package and truck status
                    cursor.execute(
                        "UPDATE packages SET status = 'loading', truck_id = %s, updated_at = NOW() WHERE id = %s",
                        (truck_id, package_id)
                    )
                    
                    cursor.execute(
                        "UPDATE trucks SET status = 'loading' WHERE id = %s",
                        (truck_id,)
                    )
                    
                    # Get user ID for notification
                    cursor.execute(
                        "SELECT user_id FROM packages WHERE id = %s",
                        (package_id,)
                    )
                    user_result = cursor.fetchone()
                    
                    if user_result and user_result[0]:
                        # Create notification for user
                        cursor.execute(
                            "INSERT INTO notifications (user_id, message) VALUES (%s, %s)",
                            (user_result[0], f"Your package {package_id} is being loaded onto truck {truck_id}")
                        )
                    
                    conn.commit()
                    
                    # Mark message as processed
                    with self.message_lock:
                        self.processed_messages.add(message_id)
                    
                    # After a short delay, update to loaded and notify Amazon
                    # In a real implementation, this would be based on world simulator events
                    threading.Timer(2.0, self._complete_loading, args=[package_id, truck_id]).start()
                    
                    # Return success response
                    return self._create_response('load_package_response', message_id, 'success', 
                                              message="Package loading initiated")
            
            except Exception as e:
                conn.rollback()
                logger.error(f"Database error in handle_load_package: {e}")
                return self._create_response('load_package_response', message_id, 'error', 
                                          message=f"Database error: {str(e)}")
            
            finally:
                self.db_pool.putconn(conn)
                
        except Exception as e:
            logger.error(f"Error handling load_package: {e}")
            return self._create_response('load_package_response', request.get('message_id', 'unknown'), 'error', 
                                      message=f"Internal error: {str(e)}")
        
    def _complete_loading(self, package_id, truck_id):
        """
        Complete the loading process and notify Amazon.
        
        Args:
            package_id: Package ID
            truck_id: Truck ID
        """
        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cursor:
                # Update package and truck status
                cursor.execute(
                    "UPDATE packages SET status = 'loaded', updated_at = NOW() WHERE id = %s",
                    (package_id,)
                )
                
                cursor.execute(
                    "UPDATE trucks SET status = 'arrive_warehouse' WHERE id = %s",
                    (truck_id,)
                )
                
                # Get destination coordinates
                cursor.execute(
                    "SELECT destination_x, destination_y, user_id FROM packages WHERE id = %s",
                    (package_id,)
                )
                result = cursor.fetchone()
                
                if result:
                    dest_x, dest_y, user_id = result
                    
                    # Create notification for user
                    if user_id:
                        cursor.execute(
                            "INSERT INTO notifications (user_id, message) VALUES (%s, %s)",
                            (user_id, f"Your package {package_id} has been loaded onto truck {truck_id}")
                        )
                    
                    # Add message to notify Amazon that package is loaded
                    cursor.execute(
                        """
                        INSERT INTO amazon_message
                        (message_type, message_content, status, created_at) 
                        VALUES ('package_loaded', %s, 'pending', NOW())
                        """,
                        (json.dumps({
                            'package_id': package_id,
                            'truck_id': truck_id
                        }),)
                    )
                    
                    # Send the truck for delivery
                    self.world_connection.send_delivery(truck_id, [{
                        'package_id': int(package_id) if package_id.isdigit() else hash(package_id) % (2**63),
                        'x': dest_x, 
                        'y': dest_y
                    }])
                
                conn.commit()
        
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Error completing loading: {e}")
        
        finally:
            if conn:
                self.db_pool.putconn(conn)

    def handle_query_status(self, request):
        """
        Handle a query_status request from Amazon.
        
        Args:
            request: The JSON request from Amazon
            
        Returns:
            dict: Response to send back to Amazon
        """
        try:
            # Check for duplicate message
            message_id = request.get('message_id')
            with self.message_lock:
                if message_id in self.processed_messages:
                    logger.info(f"Duplicate message {message_id}, returning cached response")
                    return self._create_response('query_status_response', message_id, 'success', 
                                              message="Status query already processed")
            
            # Extract request data
            package_id = request.get('package_id')
            
            # Get package status from database
            conn = self.db_pool.getconn()
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                    cursor.execute(
                        """
                        SELECT p.status as package_status, 
                               t.id as truck_id, t.status as truck_status, 
                               t.x as truck_x, t.y as truck_y
                        FROM packages p
                        LEFT JOIN trucks t ON p.truck_id = t.id
                        WHERE p.id = %s
                        """,
                        (package_id,)
                    )
                    result = cursor.fetchone()
                    
                    if not result:
                        return self._create_response('query_status_response', message_id, 'error', 
                                                  message=f"Package {package_id} not found")
                    
                    # Mark message as processed
                    with self.message_lock:
                        self.processed_messages.add(message_id)
                    
                    # Prepare response
                    response = self._create_response('query_status_response', message_id, 'success',
                                                 package_status=result['package_status'],
                                                 message="Status retrieved successfully")
                    
                    # Add truck info if available
                    if result['truck_id']:
                        response['truck_id'] = result['truck_id']
                        response['truck_status'] = result['truck_status']
                        
                        # Add truck location if available
                        if result['truck_x'] is not None and result['truck_y'] is not None:
                            response['truck_location'] = {
                                'x': result['truck_x'],
                                'y': result['truck_y']
                            }
                    
                    return response
            
            except Exception as e:
                logger.error(f"Database error in handle_query_status: {e}")
                return self._create_response('query_status_response', message_id, 'error', 
                                          message=f"Database error: {str(e)}")
            
            finally:
                self.db_pool.putconn(conn)
                
        except Exception as e:
            logger.error(f"Error handling query_status: {e}")
            return self._create_response('query_status_response', request.get('message_id', 'unknown'), 'error', 
                                      message=f"Internal error: {str(e)}")
        
    def _create_response(self, action, in_response_to, status, **kwargs):
        """
        Create a response message.
        
        Args:
            action: Action type (e.g., 'pickup_response')
            in_response_to: Message ID being responded to
            status: 'success' or 'error'
            **kwargs: Additional fields to include in the response
            
        Returns:
            dict: Formatted response message
        """
        response = {
            "action": f"{action}_response",
            "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            "message_id": str(uuid.uuid4()),
            "in_response_to": in_response_to,
            "status": status
        }
        
        # Add additional fields
        response.update(kwargs)
        
        # Ensure message field is present
        if 'message' not in response:
            response['message'] = "Success" if status == 'success' else "Error"
        
        return response
    
    def notify_truck_arrived(self, truck_id, warehouse_id):
        """
        Notify Amazon that a truck has arrived at a warehouse.
        
        Args:
            truck_id: ID of the truck
            warehouse_id: ID of the warehouse
            
        Returns:
            bool: True if successfully queued, False otherwise
        """
        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO amazon_message
                    (message_type, message_content, status, created_at) 
                    VALUES ('truck_arrived', %s, 'pending', NOW())
                    """,
                    (json.dumps({
                        'truck_id': truck_id,
                        'warehouse_id': warehouse_id
                    }),)
                )
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error queuing truck_arrived message: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                self.db_pool.putconn(conn)
    
    def notify_package_loaded(self, package_id, truck_id):
        """
        Notify Amazon that a package has been loaded onto a truck.
        
        Args:
            package_id: ID of the package
            truck_id: ID of the truck
            
        Returns:
            bool: True if successfully queued, False otherwise
        """
        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO amazon_message
                    (message_type, message_content, status, created_at) 
                    VALUES ('package_loaded', %s, 'pending', NOW())
                    """,
                    (json.dumps({
                        'package_id': package_id,
                        'truck_id': truck_id
                    }),)
                )
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error queuing package_loaded message: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                self.db_pool.putconn(conn)
    
    def notify_delivery_started(self, package_id, truck_id):
        """
        Notify Amazon that delivery has started for a package.
        
        Args:
            package_id: ID of the package
            truck_id: ID of the truck
            
        Returns:
            bool: True if successfully queued, False otherwise
        """
        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO amazon_message
                    (message_type, message_content, status, created_at) 
                    VALUES ('delivery_started', %s, 'pending', NOW())
                    """,
                    (json.dumps({
                        'package_id': package_id,
                        'truck_id': truck_id
                    }),)
                )
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error queuing delivery_started message: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                self.db_pool.putconn(conn)
    
    def notify_package_delivered(self, package_id, truck_id, x, y):
        """
        Notify Amazon that a package has been delivered.
        
        Args:
            package_id: ID of the package
            truck_id: ID of the truck
            x: X coordinate of delivery location
            y: Y coordinate of delivery location
            
        Returns:
            bool: True if successfully queued, False otherwise
        """
        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO amazon_message
                    (message_type, message_content, status, created_at) 
                    VALUES ('package_delivered', %s, 'pending', NOW())
                    """,
                    (json.dumps({
                        'package_id': package_id,
                        'truck_id': truck_id,
                        'x': x,
                        'y': y
                    }),)
                )
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error queuing package_delivered message: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                self.db_pool.putconn(conn)
    
    def send_redirect_package(self, package_id, new_x, new_y, user_id):
        """
        Send a redirect_package message to Amazon.
        
        Args:
            package_id: ID of the package
            new_x: New X coordinate for delivery
            new_y: New Y coordinate for delivery
            user_id: ID of the user requesting redirection
            
        Returns:
            bool: True if successfully queued, False otherwise
        """
        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO amazon_message
                    (message_type, message_content, status, created_at) 
                    VALUES ('redirect_package', %s, 'pending', NOW())
                    """,
                    (json.dumps({
                        'package_id': package_id,
                        'x': new_x,
                        'y': new_y,
                        'user_id': user_id
                    }),)
                )
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error queuing redirect_package message: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                self.db_pool.putconn(conn)
    
    def notify_world_created(self, world_id):
        """
        Notify Amazon about a newly created world.
        
        Args:
            world_id: ID of the world
            
        Returns:
            bool: True if successfully queued, False otherwise
        """
        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO amazon_message
                    (message_type, message_content, status, created_at) 
                    VALUES ('world_created', %s, 'pending', NOW())
                    """,
                    (json.dumps({
                        'world_id': world_id
                    }),)
                )
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error queuing world_created message: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                self.db_pool.putconn(conn)
    
    def handle_world_created_response(self, request):
        """
        Handle a world_created_response message from Amazon.
        
        Args:
            request: The JSON request from Amazon
            
        Returns:
            dict: Response to send back to Amazon
        """
        try:
            # Check for duplicate message
            message_id = request.get('message_id')
            in_response_to = request.get('in_response_to')
            status = request.get('status')
            
            logger.info(f"Received world_created_response: {status} (Message: {request.get('message', '')})")
            
            # No specific processing needed here, just log the response
            
            return None  # No response needed for this response
            
        except Exception as e:
            logger.error(f"Error handling world_created_response: {e}")
            return None
    
    def handle_request(self, request_data):
        """
        Handle an incoming request from Amazon.
        
        Args:
            request_data: The JSON request data
            
        Returns:
            dict: Response to send back to Amazon
        """
        try:
            # Extract action and message ID
            action = request_data.get('action')
            message_id = request_data.get('message_id', str(uuid.uuid4()))
            
            logger.info(f"Received {action} request from Amazon (ID: {message_id})")
            
            # Handle based on action type
            if action == 'request_pickup':
                return self.handle_request_pickup(request_data)
            elif action == 'package_ready':
                return self.handle_package_ready(request_data)
            elif action == 'load_package':
                return self.handle_load_package(request_data)
            elif action == 'query_status':
                return self.handle_query_status(request_data)
            elif action == 'world_created_response':
                return self.handle_world_created_response(request_data)
            elif action == 'heartbeat':
                return self._create_response('heartbeat', message_id, 'success', message="Service is up")
            else:
                logger.warning(f"Unknown action received: {action}")
                return self._create_response(action, message_id, 'error', message=f"Unknown action: {action}")
            
        except Exception as e:
            logger.error(f"Error handling request: {e}")
            return {
                "action": "error_response",
                "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                "message_id": str(uuid.uuid4()),
                "in_response_to": request_data.get('message_id', 'unknown'),
                "status": "error",
                "message": f"Internal error: {str(e)}"
            }