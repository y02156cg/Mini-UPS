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
from datetime import datetime
from django.utils import timezone
from django.utils.timezone import now
import os
from django.contrib.auth.hashers import make_password
from core.models import *
from django.contrib.auth.models import User
from django.db import transaction
from django.utils.timezone import now

logging.basicConfig (level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    # filename='/app/ups/logs/amazon_communication.log',  
                    filemode='a' )

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

    '''
    Send messages from databases AmazonMessage
    _notify functions are to form the message from given content, so that messages can be sent in _send_message_to_amazon
    notify functions are to store the messages to databases, so the _process_outgoing_messages can read it out and send it periodically
    '''
    def _process_outgoing_messages(self):
        """Process outgoing messages to Amazon from the database queue"""
        while self.running:
            try:
                with transaction.atomic():
                    messages = (
                        AmazonMessage.objects
                        .select_for_update(skip_locked=True)
                        .filter(status='pending', created_at__lte=timezone.now())
                        .order_by('created_at')[:10]
                    )

                    for message in messages:
                        message.status = 'processing'
                        message.save()

                        success = self._send_message_to_amazon(
                            message.message_type,
                            message.message_content
                        )

                        message.status = 'completed' if success else 'failed'
                        message.processed_at = timezone.now()
                        message.save()

                        if not success:
                            retry_count = AmazonMessage.objects.filter(
                                status='failed',
                                message_type=message.message_type,
                                message_content=message.message_content
                            ).count()

                            if retry_count < 3:
                                backoff_time = 5 * (2 ** retry_count)  # 5, 10, 20 seconds
                                AmazonMessage.objects.create(
                                    message_type=message.message_type,
                                    message_content=message.message_content,
                                    status='pending',
                                    created_at=timezone.now() + timezone.timedelta(seconds=backoff_time)
                                )

            except Exception as e:
                logger.error(f"Error processing outgoing messages: {e}")

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
                f"{self.amazon_url}/api/ups/", # send request to this url
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
            AmazonMessage.objects.create(
                message_type='truck_arrived',
                message_content={
                    'truck_id': truck_id,
                    'warehouse_id': warehouse_id
                },
                status='pending'
            )
            return True
        except Exception as e:
            logger.error(f"Error queuing truck_arrived message: {e}")
            return False
    
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
            AmazonMessage.objects.create(
                message_type='package_loaded',
                message_content={
                    'package_id': package_id,
                    'truck_id': truck_id
                },
                status='pending'
            )
            return True
        except Exception as e:
            logger.error(f"Error queuing package_loaded message: {e}")
            return False
    
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
            AmazonMessage.objects.create(
                message_type='delivery_started',
                message_content={
                    'package_id': package_id,
                    'truck_id': truck_id
                },
                status='pending'
            )
            return True
        except Exception as e:
            logger.error(f"Error queuing delivery_started message: {e}")
            return False
    
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
            AmazonMessage.objects.create(
                message_type='package_delivered',
                message_content={
                    'package_id': package_id,
                    'truck_id': truck_id,
                    'x': x,
                    'y': y
                },
                status='pending',
            )
            return True
        except Exception as e:
            logger.error(f"Error queuing package_delivered message: {e}")
            return False
    
    def notify_world_created(self, world_id):
        """
        Notify Amazon about a newly created world.
        
        Args:
            world_id: ID of the world
            
        Returns:
            bool: True if successfully queued, False otherwise
        """
        try:
            AmazonMessage.objects.create(
            message_type='world_created',
            message_content={'world_id': world_id},
            status='pending'
            )
            return True
        except Exception as e:
            logger.error(f"Error queuing world_created message: {e}")
            return False
    

    '''
    Handling requests
    '''
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
            if not action:
                return JsonResponse({
                    'status': 'error',
                    'message': 'Missing action field'
                }, status=400)
            
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

            with transaction.atomic():
                # ensure warehouse exists
                warehouse, _ = Warehouse.objects.get_or_create(
                    id=warehouse_id,
                    defaults={
                        'x': destination_x,
                        'y': destination_y,
                        'world_id': request.get('world_id', 1),
                    }
                )

                # ensure user exist
                user = None
                if user_id:
                    user, created = User.objects.get_or_create(
                        id=user_id,
                        defaults={
                            'username': f"user_{user_id}",
                            'password': make_password("1234"),
                            'email': f"user_{user_id}@example.com",
                            'is_active': True,
                            'is_staff': False,
                            'is_superuser': False,
                            'date_joined': now()
                        }
                    )

                # create package
                Package.objects.get_or_create(
                    id=package_id,
                    defaults={
                        'user': user,
                        'warehouse': warehouse,
                        'truck': None,
                        'status': 'created', 
                        'destination_x': destination_x,
                        'destination_y': destination_y,
                        'description': description
                    }
                )

                # add items
                for item in items:
                    Item.objects.create(
                        package_id=package_id,
                        name=item.get('name'),
                        description=item.get('description'),
                        quantity=item.get('quantity')
                    )

                # notification
                if user:
                    Notification.objects.create(
                        user=user,
                        message=f"A new package {package_id} has been created for you"
                    )

                with self.message_lock:
                    self.processed_messages.add(message_id)
                    if len(self.processed_messages) > 1000:
                        self.processed_messages.pop()

                return self._create_response('pickup_response', message_id, 'success',
                                            tracking_number=package_id,
                                            message="Pickup request received")

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
            message_id = request.get('message_id')
            with self.message_lock:
                if message_id in self.processed_messages:
                    logger.info(f"Duplicate message {message_id}, returning cached response")
                    return self._create_response(
                        'package_ready_response', message_id, 'success',
                        message="Package ready notification already processed"
                    )

            package_id = request.get('package_id')
            package = Package.objects.select_related('user', 'warehouse').filter(id=package_id).first()

            if not package:
                return self._create_response(
                    'package_ready_response', message_id, 'error',
                    message=f"Package {package_id} not found"
                )

            # If created, then waiting 
            if package.status == 'created':
                package.status = 'waiting_for_pickup'
                logger.info(f"Package {package_id} updated: created -> waiting_for_pickup")

                # Notify users
                user = package.user
                # warehouse_id = package.warehouse.id if package.warehouse else None
                if user:
                    Notification.objects.create(
                        user=user,
                        message=f"Your package {package_id} is waiting for pickup"
                    )
            else:
                logger.warning(f"Package {package_id} in unexpected state '{package.status}' when handling package_ready")
                return self._create_response('package_ready_response', request.get('message_id', 'unknown'), 'error', 
                                      message=f"Package {package_id} in unexpected state '{package.status}' when handling package_ready")
            
            package.updated_at = now()
            package.save()

            # Mark message as processed
            with self.message_lock:
                self.processed_messages.add(message_id)
            
            # Return success response
            return self._create_response('package_ready_response', message_id, 'success', 
                                        message="Package ready notification processed")
                
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
            message_id = request.get('message_id')
            with self.message_lock:
                if message_id in self.processed_messages:
                    logger.info(f"Duplicate message {message_id}, returning cached response")
                    return self._create_response(
                        'load_package_response', message_id, 'success',
                        message="Load package request already processed"
                    )

            package_id = request.get('package_id')
            truck_id = request.get('truck_id')

            # 1. 查找包裹
            package = Package.objects.select_related('user').filter(id=package_id).first()
            if not package:
                return self._create_response(
                    'load_package_response', message_id, 'error',
                    message=f"Package {package_id} not found"
                )

            if package.status not in ['ready_for_pickup', 'pickup_assigned']:
                return self._create_response(
                    'load_package_response', message_id, 'error',
                    message=f"Package {package_id} is not ready for pickup (status: {package.status})"
                )

            # 2. 查找卡车
            truck = Truck.objects.filter(id=truck_id).first()
            if not truck:
                return self._create_response(
                    'load_package_response', message_id, 'error',
                    message=f"Truck {truck_id} not found"
                )

            if truck.status != 'arrive_warehouse':
                return self._create_response(
                    'load_package_response', message_id, 'error',
                    message=f"Truck {truck_id} is not at warehouse (status: {truck.status})"
                )

            # 3. Update package and truck status
            package.status = 'loading'
            package.truck = truck
            package.updated_at = now()
            package.save()

            truck.status = 'loading'
            truck.save()

            # 4. Get user ID for notification
            if package.user:
                Notification.objects.create(
                    user=package.user,
                    message=f"Your package {package_id} is being loaded onto truck {truck_id}"
                )

            # 5. Mark as processed
            with self.message_lock:
                self.processed_messages.add(message_id)

            # 6. Asynchronized loading 
            threading.Timer(2.0, self._complete_loading, args=[package_id, truck_id]).start()

            return self._create_response(
                'load_package_response', message_id, 'success',
                message="Package loading initiated"
            )

        except Exception as e:
            logger.error(f"Error handling load_package: {e}")
            return self._create_response(
                'load_package_response', request.get('message_id', 'unknown'), 'error',
                message=f"Internal error: {str(e)}"
            )

    def handle_query_status(self, request):
        """
        Handle a query_status request from Amazon.
        
        Args:
            request: The JSON request from Amazon
            
        Returns:
            dict: Response to send back to Amazon
        """
        try:
            message_id = request.get('message_id')
            with self.message_lock:
                if message_id in self.processed_messages:
                    logger.info(f"Duplicate message {message_id}, returning cached response")
                    return self._create_response(
                        'query_status_response', message_id, 'success',
                        message="Status query already processed"
                    )

            package_id = request.get('package_id')
            package = Package.objects.select_related('truck').filter(id=package_id).first()

            if not package:
                return self._create_response(
                    'query_status_response', message_id, 'error',
                    message=f"Package {package_id} not found"
                )

            # Mark message as processed
            with self.message_lock:
                self.processed_messages.add(message_id)

            # Prepare response
            response = self._create_response(
                'query_status_response',
                message_id,
                'success',
                package_status=package.status,
                message="Status retrieved successfully"
            )

            if package.truck:
                response['truck_id'] = package.truck.id
                response['truck_status'] = package.truck.status
                if package.truck.x is not None and package.truck.y is not None:
                    response['truck_location'] = {
                        'x': package.truck.x,
                        'y': package.truck.y
                    }

            return response

        except Exception as e:
            logger.error(f"Error handling query_status: {e}")
            return self._create_response(
                'query_status_response',
                request.get('message_id', 'unknown'),
                'error',
                message=f"Internal error: {str(e)}"
            )
        
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
    
    '''
    Helper functions
    '''
    def _complete_loading(self, package_id, truck_id):
        """
        Complete the loading process and notify Amazon.
        
        Args:
            package_id: Package ID
            truck_id: Truck ID
        """
        try:
            package = Package.objects.select_related('truck').filter(id=package_id).first()
            truck = Truck.objects.filter(id=truck_id).first()
            if not package or not truck:
                logger.warning(f"Package or Truck not found (pkg: {package}, truck: {truck})")
                return
            
            # 1. Update statuses
            package.status = 'loaded'
            package.updated_at = now()
            package.save()

            truck.status = 'arrive_warehouse'
            truck.save()

            # 2. Get destination + user
            dest_x, dest_y = package.destination_x, package.destination_y
            user = package.user

            # 3. Add user notification
            if user:
                Notification.objects.create(
                    user=user,
                    message=f"Your package {package_id} has been loaded onto truck {truck_id}"
                )

            # 4. Add message to notify Amazon that package is loaded
            AmazonMessage.objects.create(
                message_type='package_loaded',
                message_content={
                    'package_id': package_id,
                    'truck_id': truck_id
                },
                status='pending'
            )

            # 5. Send delivery command
            world_pkg_id = int(package_id) if str(package_id).isdigit() else hash(package_id) % (2**63)
            self.world_connection.send_delivery(truck_id, [{
                'package_id': world_pkg_id,
                'x': dest_x,
                'y': dest_y
            }])
        except Exception as e:
            logger.error(f"Error completing loading: {e}")

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
    
    # Used in views
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
            AmazonMessage.objects.create(
                message_type='redirect_package',
                message_content={
                    'package_id': package_id,
                    'x': new_x,
                    'y': new_y,
                    'user_id': user_id
                },
                status='pending'
            )
            return True
        except Exception as e:
            logger.error(f"Error queuing redirect_package message: {e}")
            return False
    
