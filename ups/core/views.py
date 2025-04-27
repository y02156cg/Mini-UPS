from django.shortcuts import render

# Create your views here.
from django.shortcuts import render, redirect, get_list_or_404
from django.http import JsonResponse
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.models import User

from django.views.decorators.csrf import csrf_exempt
from django.db import transaction
from django.db.models import Q
from django.contrib import messages
from django.conf import settings
from django.contrib.auth.decorators import login_required
import json
import logging
import psycopg2
import psycopg2.extras
import time
import threading
import os
import random
import string
from notification_manager import notification_manager

from core.models import *
from django.utils.timezone import now
from datetime import datetime

from amazon_communication import AmazonCommunication
from core.global_context import amazon_communication_instance
logger = logging.getLogger(__name__)

"""不确定这样connect db是不是正确"""
def get_db_connection():
    return psycopg2.connect(
        dbname=settings.DATABASES['default']['NAME'],
        user=settings.DATABASES['default']['USER'],
        password=settings.DATABASES['default']['PASSWORD'],
        host=settings.DATABASES['default']['HOST'],
        port=settings.DATABASES['default']['PORT']
    )

def generate_tracking_num(length=10):
    """Generate a random tracking number"""
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choice(chars) for _ in range(length))

def home(request):
    world_id = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT world_id FROM world_state LIMIT 1")
            result = cursor.fetchone()
            if result:
                world_id = result['world_id']
    except Exception as e:
        print(f"Error getting world info: {e}")
    finally:
        if conn:
            conn.close()

    context = {
        'world_id': world_id,
        'is_authenticated': request.user.is_authenticated
    }
    return render(request, 'core/home.html', context)

def login_view(request):
    if request.method == 'POST':
        username = request.POST.get('username')
        password = request.POST.get('password')
        user = authenticate(request, username=username, password=password)

        if user is not None:
            login(request, user)
            return redirect('dashboard')
        else:
            messages.error(request, 'Invalid username or password')
    return render(request, 'core/login.html')

def logout_view(request):
    logout(request)
    return redirect('home')

def register(request):
    if request.method == 'POST':
        username = request.POST.get('username')
        password = request.POST.get('password')
        email = request.POST.get('email')

        if not username or not password:
            messages.error(request, 'Username and password are required')
            return render(request, 'core/register.html')
        
        if User.objects.filter(username=username).exists():
            messages.error(request, 'Username already exists')
            return render(request, 'core/register.html')

        user = User.objects.create_user(username=username, password=password, email=email)
        user.save()

        login(request, user)
        return redirect('dashboard')

    return render(request, 'core/register.html')

@login_required
def dashboard(request):
    user = request.user
    # Query user packages and trucks
    packages = (
        Package.objects
        .select_related("truck")
        .filter(user=user)
        .order_by("-created_at")
    )

    # Query unread notifications
    notifications = (
        Notification.objects
        .filter(user=user, read=False)
        .order_by("-created_at")
    )

    # mark as read
    notifications.update(read=True, updated_at=now())

    context = {
        'packages': packages,
        'notifications': notifications
    }
    return render(request, 'core/dashboard.html', context)

def track_package(request):
    """Track a package by tracking number"""
    if request.method == 'POST':
        tracking_number = request.POST.get('tracking_number')
        return redirect('package_details', tracking_number=tracking_number)
    
    return render(request, 'core/track.html')

def package_details(request, tracking_number):
    """
    查看某个包裹的详细信息
    """
    package = (
        Package.objects
        .select_related('truck', 'user')
        .filter(id=tracking_number)
        .first()
    )
    
    if not package:
        messages.error(request, f'Package with tracking number {tracking_number} not found')
        return redirect('track_package')

    # Query item
    items = Item.objects.filter(package_id=tracking_number)

    # Determine if redirect is available
    can_redirect = package.status not in ['delivering', 'delivered']

    # Determine if the user is owner
    is_owner = request.user.is_authenticated and request.user.id == package.user_id

    # If owner, mark as read
    if is_owner:
        Notification.objects.filter(
            user=request.user,
            message__icontains=tracking_number,
            read=False
        ).update(read=True, updated_at=now())

    context = {
        'package': package,
        'items': items,
        'can_redirect': can_redirect,
        'is_owner': is_owner,
    }
    return render(request, 'core/package_details.html', context)

@login_required
def redirect_package(request, tracking_number):
    """
    用户请求修改包裹的投递地址
    """
    if request.method == 'POST':
        new_x = request.POST.get('new_x')
        new_y = request.POST.get('new_y')

        try:
            new_x = int(new_x)
            new_y = int(new_y)
        except (ValueError, TypeError):
            messages.error(request, 'Invalid coordinates')
            return redirect('package_details', tracking_number=tracking_number)

        package = Package.objects.select_related('truck', 'user').filter(id=tracking_number).first()

        if not package:
            messages.error(request, 'Package not found')
            return redirect('dashboard')

        if package.user_id != request.user.id:
            messages.error(request, 'You do not own this package')
            return redirect('dashboard')

        if package.status in ['delivering', 'delivered']:
            messages.error(request, 'Redirection Failed: This package has already been delivered')
            return redirect('package_details', tracking_number=tracking_number)

        # Update package coordinate
        package.destination_x = new_x
        package.destination_y = new_y
        package.updated_at = now()
        package.save()

        # Create notification
        Notification.objects.create(
            user=request.user,
            message=f"Your package {tracking_number} has been redirected to ({new_x}, {new_y})"
        )

        # Send redirect message if the truck is assigned and status is valid # TODO
        if package.truck and package.status not in ['created', 'waiting_for_pickup', 'pickup_assigned']:
            amazon_communication_instance.send_redirect_package(
                package_id=tracking_number,
                new_x=new_x,
                new_y=new_y,
                user_id=request.user.id
            )

        messages.success(request, f'Package {tracking_number} redirected to ({new_x}, {new_y})')

        return redirect('package_details', tracking_number=tracking_number)

    # GET 请求，渲染重定向页面 
    return render(request, 'core/redirect_package.html', {'tracking_number': tracking_number})

@login_required
def admin_dashboard(request):
    trucks = []
    packages = []

    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(
                "SELECT id, status, x, y FROM trucks ORDER BY id"
            )
            trucks = cursor.fetchall()

            cursor.execute(
                """
                SELECT p.id, p.status, p.destination_x, p.destination_y, p.created_at,
                       t.id as truck_id, u.username as owner
                FROM packages p
                LEFT JOIN trucks t ON p.truck_id = t.id
                LEFT JOIN auth_user u ON p.user_id = u.id
                ORDER BY p.created_at DESC
                LIMIT 50
                """
            )
            packages = cursor.fetchall()
    except Exception as e:
        print(f"Error loading admin dashboard: {e}")
        messages.error(request, 'An error occurred while loading the admin dashboard')
    finally:
        if conn:
            conn.close()

    context = {
        'trucks': trucks,
        'packages': packages
    }
    return render(request, 'core/admin_dashboard.html', context)

@csrf_exempt
def amazon_api(request):
    if request.method != 'POST':
        return JsonResponse({
            'status': 'error',
            'message': 'Method not allowed, can only POST'
        }, status=405)
    
    try:
        data = json.loads(request.body)
        result = amazon_communication_instance.handle_request(data)
        return JsonResponse(result)
    except json.JSONDecodeError:
        return JsonResponse({
                'status': 'error',
                'message': 'Invalid JSON'
            }, status=400)
    except Exception as e:
        print(f"Error in Amazon API: {e}")
        return JsonResponse({
            'status': 'error',
            'message': str(e)
        }, status=500)
    
    
def truck_status_api(request, truck_id):
    """API endpoint to get truck status for AJAX updates"""
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(
                "SELECT id, status, x, y FROM trucks WHERE id = %s",
                (truck_id,)
            )
            truck = cursor.fetchone()

            if truck:
                return JsonResponse({
                    'status': 'success',
                    'truck': {
                        'id': truck['id'],
                        'status': truck['status'],
                        'x': truck['x'],
                        'y': truck['y']
                    }
                })
            else:
                return JsonResponse({
                    'status': 'error',
                    'message': 'Truck not found'
                }, status=404)
    except Exception as e:
        print(f"Error in truck status API: {e}")
        return JsonResponse({
            'status': 'error',
            'message': str(e)
        }, status=500)
    finally:
        if conn:
            conn.close()

def package_status_api(request, tracking_number):
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(
                """
                SELECT p.id, p.status, p.destination_x, p.destination_y, t.id as truck_id, t.status as truck_status,
                       t.x as truck_x, t.y as truck_y
                FROM packages p
                LEFT JOIN trucks t ON p.truck_id = t.id
                WHERE p,id = %s
                """,
                (tracking_number,)
            )
            package = cursor.fetchone()

            if package:
                return JsonResponse({
                    'status': 'success',
                    'package': {
                        'id': package['id'],
                        'status': package['status'],
                        'destination': {
                            'x': package['destination_x'],
                            'y': package['destination_y']
                        },
                        'truck': {
                            'id': package['truck_id'],
                            'status': package['truck_status'],
                            'x': package['truck_x'],
                            'y': package['truck_y']
                        } if package['truck_id'] else None
                    }
                })
            else:
                return JsonResponse({
                    'status': 'error',
                    'message': 'Package not found'
                }, status=404)
    except Exception as e:
        print(f"Error in package status API: {e}")
        return JsonResponse({
            'status': 'error',
            'message': str(e)
        }, status=500)
    finally:
        if conn:
            conn.close()

@login_required
def world_control(request):
    """Admin control for world simulation"""
    
    if request.method == 'POST':
        action = request.POST.get('action')
        
        if action == 'create_world':
            # Create a new world
            try:
                # Call daemon to create a new world
                # This would be implemented via a message to the daemon
                messages.success(request, 'Request to create new world sent to daemon')
            except Exception as e:
                print(f"Error creating world: {e}")
                messages.error(request, 'An error occurred while creating a new world')
        
        elif action == 'set_speed':
            speed = request.POST.get('speed')
            try:
                speed = int(speed)
                if speed < 1:
                    speed = 1
                elif speed > 1000:
                    speed = 1000
                
                conn = get_db_connection()
                with conn.cursor() as cursor:
                    # Update world state
                    cursor.execute(
                        "UPDATE world_state SET sim_speed = %s",
                        (speed,)
                    )
                    
                    # Add message to send sim speed command
                    cursor.execute(
                        """
                        INSERT INTO amazon_messages 
                        (message_type, message_content, status) 
                        VALUES ('set_sim_speed', %s, 'pending')
                        """,
                        (json.dumps({'speed': speed}),)
                    )
                    
                    conn.commit()
                
                messages.success(request, f'Simulation speed set to {speed}')
            except (ValueError, TypeError):
                messages.error(request, 'Invalid speed value')
            except Exception as e:
                print(f"Error setting sim speed: {e}")
                messages.error(request, 'An error occurred while setting simulation speed')
    
    # Get current world state
    world_info = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT world_id, sim_speed, connected_at FROM world_state LIMIT 1")
            world_info = cursor.fetchone()
    except Exception as e:
        print(f"Error getting world info: {e}")
    finally:
        if conn:
            conn.close()
    
    context = {
        'world_info': world_info
    }
    return render(request, 'core/world_control.html', context)

@login_required
def delivery_map(request):
    warehouses = Warehouse.objects.all()

    # Get active trucks
    trucks = Truck.objects.exclude(status='error')
    
    # If user is authenticated, get their packages
    packages = []
    if request.user.is_authenticated:
        packages = Package.objects.filter(
            user=request.user,
            status__in=['waiting_for_pickup', 'pickup_assigned', 'ready_for_pickup', 'loading', 'loaded', 'delivering']
        ).select_related('truck')
    
    context = {
        'warehouses': warehouses,
        'trucks': trucks,
        'packages': packages,
        'is_authenticated': request.user.is_authenticated
    }
    return render(request, 'core/delivery_map.html', context)

@login_required
def map_data_api(request):
    """API endpoint to get real-time map data"""
    # Get warehouses
    warehouses = list(Warehouse.objects.values('id', 'x', 'y'))
    
    # Get active trucks
    trucks = list(Truck.objects.exclude(status='error').values('id', 'status', 'x', 'y'))
    
    # Get user's packages or all packages for admin
    if request.user.is_staff:
        package_query = Package.objects.exclude(status='delivered').select_related('truck')
        packages = []
        for package in package_query:
            package_dict = {
                'id': package.id,
                'status': package.status,
                'destination_x': package.destination_x,
                'destination_y': package.destination_y,
                'truck_id': package.truck.id if package.truck else None
            }
            if package.truck:
                package_dict.update({
                    'truck_status': package.truck.status,
                    'truck_x': package.truck.x,
                    'truck_y': package.truck.y
                })
            packages.append(package_dict)
    else:
        package_query = Package.objects.filter(
            user=request.user
        ).exclude(status='delivered').select_related('truck')
        packages = []
        for package in package_query:
            package_dict = {
                'id': package.id,
                'status': package.status,
                'destination_x': package.destination_x,
                'destination_y': package.destination_y,
                'truck_id': package.truck.id if package.truck else None
            }
            if package.truck:
                package_dict.update({
                    'truck_status': package.truck.status,
                    'truck_x': package.truck.x,
                    'truck_y': package.truck.y
                })
            packages.append(package_dict)
    
    return JsonResponse({
        'warehouses': warehouses,
        'trucks': trucks,
        'packages': packages,
        'timestamp': timezone.now().isoformat()
    })

@login_required
def truck_status_api(request, truck_id):
    """API endpoint to get truck status for AJAX updates"""
    try:
        truck = Truck.objects.get(id=truck_id)
        return JsonResponse({
            'status': 'success',
            'truck': {
                'id': truck.id,
                'status': truck.status,
                'x': truck.x,
                'y': truck.y
            }
        })
    except Truck.DoesNotExist:
        return JsonResponse({
            'status': 'error',
            'message': 'Truck not found'
        }, status=404)
    except Exception as e:
        return JsonResponse({
            'status': 'error',
            'message': str(e)
        }, status=500)
    
@login_required
def package_status_api(request, tracking_number):
    """API endpoint to get package status for AJAX updates"""
    try:
        package = Package.objects.select_related('truck').get(id=tracking_number)
        response_data = {
            'status': 'success',
            'package': {
                'id': package.id,
                'status': package.status,
                'destination': {
                    'x': package.destination_x,
                    'y': package.destination_y
                }
            }
        }
        
        if package.truck:
            response_data['package']['truck'] = {
                'id': package.truck.id,
                'status': package.truck.status,
                'x': package.truck.x,
                'y': package.truck.y
            }
        
        return JsonResponse(response_data)
    except Package.DoesNotExist:
        return JsonResponse({
            'status': 'error',
            'message': 'Package not found'
        }, status=404)
    except Exception as e:
        return JsonResponse({
            'status': 'error',
            'message': str(e)
        }, status=500)


@login_required
def notification_settings(request):
    """View for managing notification preferences"""
    if request.method == 'POST':
        # Update notification preferences
        for notification_type in ['delivery', 'pickup', 'status_change', 'truck_arrival', 'system']:
            enabled = request.POST.get(f'{notification_type}_enabled') == 'on'
            
            # Update or create preference
            notification_manager.register_notification_preference(
                user=request.user, 
                notification_type=notification_type,
                enabled=enabled,
            )
        
        # Update email
        email = request.POST.get('email', request.user.email)
        if email:
            request.user.email = email
            request.user.save(update_fields=['email'])
        
        messages.success(request, 'Notification preferences updated successfully')
        return redirect('notification_settings')
    
    # Get current preferences
    preferences = {}
    for pref in NotificationPreference.objects.filter(user=request.user):
        preferences[pref.notification_type] = {
            'enabled': pref.enabled,
        }
    
    # Recent notifications
    recent_notifications = Notification.objects.filter(
        user=request.user
    ).order_by('-created_at')[:10]
    
    context = {
        'preferences': preferences,
        'email': request.user.email,
        'recent_notifications': recent_notifications
    }
    
    return render(request, 'core/notification_settings.html', context)

@login_required
def get_notifications(request):
    """API endpoint to get user's recent notifications"""
    # Mark all as read if requested
    mark_read = request.GET.get('mark_read') == 'true'
    
    # Get unread notifications first, then most recent read ones
    notifications = list(Notification.objects.filter(
        user=request.user,
        read=False
    ).order_by('-created_at').values('id', 'message', 'created_at', 'read'))
    
    # If we need more to reach 10, get some read ones too
    if len(notifications) < 10:
        read_notifications = list(Notification.objects.filter(
            user=request.user,
            read=True
        ).order_by('-created_at').values('id', 'message', 'created_at', 'read')[:10-len(notifications)])
        
        notifications.extend(read_notifications)
    
    # Convert datetime to string
    for notification in notifications:
        notification['created_at'] = notification['created_at'].isoformat()
    
    # Mark as read if requested
    if mark_read and notifications:
        unread_ids = [n['id'] for n in notifications if not n['read']]
        if unread_ids:
            Notification.objects.filter(id__in=unread_ids).update(read=True)
            # Update the response to show they're now read
            for notification in notifications:
                if notification['id'] in unread_ids:
                    notification['read'] = True
    
    return JsonResponse({
        'notifications': notifications,
        'unread_count': Notification.objects.filter(user=request.user, read=False).count()
    })


@login_required
def test_notification(request):
    """Send a test notification to the user"""
    if request.method == 'POST':
        notification_type = request.POST.get('notification_type', 'system')
        try:
            # Create a test notification
            message = f"This is a test {notification_type} notification. Sent at {timezone.now().strftime('%H:%M:%S')}."
            
            # Create in database
            Notification.objects.create(
                user=request.user,
                message=message
            )
            
            # Send email
            if request.user.email:
                from django.core.mail import send_mail
                from django.conf import settings
                
                send_mail(
                    f"Test {notification_type.title()} Notification",
                    message,
                    settings.DEFAULT_FROM_EMAIL,
                    [request.user.email],
                    fail_silently=True
                )
                
                messages.success(request, f"Test notification sent to {request.user.email}")
            else:
                messages.warning(request, "No email address set. Notification saved to system only.")

            
                
            return redirect('notification_settings')
        
        except Exception as e:
            messages.error(request, f"Error sending test notification: {str(e)}")
            return redirect('notification_settings')
    
    return redirect('notification_settings')

