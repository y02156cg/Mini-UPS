from django.shortcuts import render, redirect, get_list_or_404
from django.http import JsonResponse
from django.contrib.auth import authenticate, login, logout
from django.views.decorators.csrf import csrf_exempt
from django.db import transaction
from django.contrib import messages
from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.shortcuts import render, redirect
from django.http import JsonResponse
import json
import psycopg2
import psycopg2.extras
import time
import threading
import os
import random
import string
from datetime import datetime

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
    return render(request, 'ups/home.html', context)

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

    return render(request, 'ups/login.html')

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
            return render(request, 'ups/register.html')
        
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                cursor.execute("SELECT id FROM users WHERE username = %s", (username,))
                if cursor.fetchone():
                    messages.error(request, 'Username already exists')
                    return render(request, 'ups/register.html')
                
                cursor.execute(
                    "INSERT INTO users (username, password_hash, email) VALUES (%s, %s, %s) RETURNING id",
                    (username, password, email)
                )
                user_id = cursor.fetchone()[0]
                conn.commit()

                user = authenticate(request, username=username, password=password)
                if user is not None:
                    login(request, user)
                    return redirect('dashboard')
        except Exception as e:
            print(f"Error registering user: {e}")
            messages.error(request, 'An error occurred during registration')
        finally:
            if conn:
                conn.close

    return render(request, 'ups/register.html')

@login_required
def dashboard(request):
    user_id = request.user.id
    packages = []
    notifications = []

    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(
                """
                SELECT p.id, p.status, p.destination_x, p.destination_y, p.created_at, t.id as truck_id, t.status as truck_status
                FROM packages p
                LEFT JOIN trucks t ON p.truck_id = t.id
                WHERE p.user_id = %s
                ORDER BY p.created_at DESC
                """,
                (user_id,)
            )

            packages = cursor.fetchall()
            cursor.execute(
                "SELECT id, message, created_at FROM notifications WHERE user_id = %s AND read = FALSE ORDER BY created_at DESC",
                (user_id,)
            )
            notifications = cursor.fetchall()

            if notifications:
                cursor.execute(
                    "UPDATE notifications SET read = TRUE WHERE user_id = %s AND read = FALSE",
                    (user_id,)
                )
                conn.commit()
    except Exception as e:
        print(f"Error loading dashboard: {e}")
        messages.error(request, 'An error occurred while loading your dashboard')
    finally:
        if conn:
            conn.close()

    context = {
        'packages': packages,
        'notifications': notifications
    }
    return render(request, 'ups/dashboard.html', context)

def track_package(request):
    """Track a package by tracking number"""
    if request.method == 'POST':
        tracking_number = request.POST.get('tracking_number')
        return redirect('package_details', tracking_number=tracking_number)
    
    return render(request, 'ups/track.html')

def package_details(request, tracking_number):
    package = None
    items = []
    can_redirect = False

    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(
                """
                SELECT p.id, p.status, p.destination_x, p.destination_y, p.created_at, p.updated_at,
                       t.id as truck_id, t.status as truck_status, t.x as truck_x, p.user_id, u.username
                FROM packages p
                LEFT JOIN trucks t ON p.truck_id = t.id
                LEFT JOIN users u ON p.user_id = u.id
                WHERE p.id = %s
                """ ,
                (tracking_number,)
            )
            package = cursor.fetchone()

            if not package:
                messages.error(request, f'Package with tracking number {tracking_number} not found')
                return redirect('track_package')
            
            cursor.execute(
                "SELECT id, name, description, quantity FROM items WHERE package_id = %s",
                (tracking_number,)
            )
            items = cursor.fetchall()

            can_redirect = package['status'] not in ['delivering', 'delivered']

            if request.user.is_authenticated and request.user.id == package['user_id']:
                cursor.execute(
                    "UPDATE notifications SET read = TRUE WHERE user_id = %s AND message LIKE %s",
                    (request.user.id, f"%{tracking_number}%")
                )
                conn.commit()
    except Exception as e:
        print(f"Error loading package details: {e}")
        messages.error(request, 'An error occurred while loading package details')
    finally:
        if conn:
            conn.close()

    context = {
        'package': package,
        'items': items,
        'can_redirect': can_redirect,
        'is_owner': request.user.is_authenticated and request.user.id == package['user_id']
    }
    return render(request, 'ups/package_details.html', context)

@login_required
def redirect_package(request, tracking_number):
    if request.method == 'POST':
        new_x = request.POST.get('new_x')
        new_y = request.POST.get('new_y')

        try:
            new_x = int(new_x)
            new_y = int(new_y)
        except (ValueError, TypeError):
            messages.error(request, 'Invalid coordinates')
            return redirect('package_details', tracking_number=tracking_number)
        
        try:
            conn = get_db_connection()
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(
                    "SELECT truck_id, status, user_id FROM packages WHERE id = %s",
                    (tracking_number,)
                )
                package = cursor.fetchone()

                if not package:
                    messages.error(request, 'Package not found')
                    return redirect('dashboard')
                
                if package['user_id'] != request.user.id:
                    messages.error(request, 'You do not own this package')
                    return redirect('dashboard')
                
                if package['status'] in ['delivering', 'delivered']:
                    messages.error(request, 'This package has been delivered')
                    return redirect('package_details', tracking_number=tracking_number)
                
                cursor.execute(
                    "UPDATE packages SET destination_x = %s, destination_y = %s, updated_at = NOW() WHERE id = %s",
                    (new_x, new_y, tracking_number)
                )

                cursor.execute(
                    "INSERT INTO notifications (user_id, message) VALUES (%s, %s)",
                    (request.user.id, f"Your package {tracking_number} has been redirected to ({new_x}, {new_y})")
                )

                if package['truck_id'] and package['status'] not in ['created', 'waiting_for_pickup', 'pickup_assigned']:
                    cursor.execute(
                        """
                        INSERT INTO amazon_messages (message_type, message_content)
                        VALUES ('redirect_package', %s)
                        """,
                        (json.dumps({
                            'trucking_number': tracking_number,
                            'truck_id': package['truck_id'],
                            'x': new_x,
                            'y': new_y
                        }),) # return the whole characters contents
                    )

                    conn.commit()
                    messages.success(request, f'Package {tracking_number} redirected to ({new_x}, {new_y})')
        except Exception as e:
            print(f"Error redirecting package: {e}")
            messages.error(request, 'An error occured while redirecting the package')
        finally:
            if conn:
                conn.close()

        return redirect('package_details', tracking_number=tracking_number)
    
    return render(request, 'ups/redirect_package.html', {'tracking_number': tracking_number})

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
                LEFT JOIN users u ON p.user_id = u.id
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
    return render(request, 'ups/admin_dashboard.html', context)

@csrf_exempt
def amazon_api(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            action = data.get('action')

            if action == 'request_pickup':
                # Amazon is requesting a pickup
                package_id = data.get('package_id')
                warehouse_id = data.get('warehouse_id')
                user_id = data.get('user_id')
                destination_x = data.get('destination_x')
                destination_y = data.get('destination_y')
                description = data.get('description')
                items = data.get('items', [])

                if not package_id:
                    package_id = generate_tracking_num()

                conn = get_db_connection()
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO packages,
                        (id, user_id, warehouse_id, status, destination_x, destination_y, description)
                        VALUES (%s, %s, %s, 'waiting_for_pickup', %s, %s, %s)
                        """,
                        (package_id, user_id, warehouse_id, destination_x, destination_y, description)
                    )

                    for item in items:
                        cursor.execute(
                            "INSERT INTO items (package_id, name, description, quantity) VALUES (%s, %s, %s, %s)",
                            (package_id, item.get('name'), item.get('description'), item.get('quantity'))
                        )

                    if user_id:
                        cursor.execute(
                            "INSERT INTO notifications (user_id, message) VALUES (%s, %s)",
                            (user_id, f"A new package {package_id} has been created for you")
                        )

                    conn.commit()

                return JsonResponse({
                    'status': 'success',
                    'tracking_number': package_id,
                    'message': 'Pickup request received'
                })
            
            elif action == 'package_ready':
                package_id = data.get('package_id')

                conn = get_db_connection()
                with conn.cursor() as cursor:
                    cursor.execute(
                        "UPDATE packages SET status = 'ready_for_pickup', updated_at = NOW() WHERE id = %s",
                        (package_id,)
                    )

                    cursor.execute(
                        "SELECT user_id, warehouse_id FROM packages WHERE id = %s",
                        (package_id,)
                    )
                    result = cursor.fetchone()

                    if result:
                        user_id, warehouse_id = result

                        cursor.execute(
                            "SELECT id FROM trucks WHERE status = 'idle' LIMIT 1"
                        )
                        truck_result = cursor.fetchone()

                        if truck_result:
                            truck_id = truck_result[0]

                            cursor.execute(
                                "UPDATE trucks SET status = 'traveling' WHERE id = %s",
                                (truck_id,)
                            )

                            cursor.execute(
                                "UPDATE package SET truck_id = %s, status = 'pickup_assigned', updated_at = NOW() WHERE id = %s",
                                (truck_id, package_id)
                            )

                            if user_id:
                                cursor.execute(
                                    "INSERT INTO notifications (user_id, message) VALUES (%s, %s)",
                                    (user_id, f"Your package {package_id} is ready for pickup")
                                )
                            
                            cursor.execute(
                                """
                                INSERT INTO amazon_messages
                                (message_type, message_content, status)
                                VALUES ('pickup_command', %s, 'pending)
                                """,
                                (json.dumps({
                                    'truck_id': truck_id,
                                    'warehouse_id': warehouse_id,
                                    'package_id': package_id
                                }),)
                            )
                    conn.commit()

                return JsonResponse({
                    'status': 'success',
                    'message': 'Package ready for pickup'
                })

            elif action == 'query_status':
                package_id = data.get('package_id')

                conn = get_db_connection()
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                    cursor.execute(
                        """
                        SELECT p.status, t.status as truck_status, t.x as truck_x, t.y as truck_y
                        FROM package p
                        LEFT JOIN trucks t ON p.truck_id = t.id
                        WHERE p.id = %s
                        """,
                        (package_id,)
                    )
                    result = cursor.fetchone()

                if result:
                    return JsonResponse({
                        'status': 'success',
                        'package_status': result['status'],
                        'truck_status': result['truck_status'] if result['truck_status'] else None,
                        'truck_location':{
                            'x': result['truck_x'],
                            'y': result['truck_y']
                        } if result['truck_x'] is not None else None
                    })
                else:
                    return JsonResponse({
                        'status': 'error',
                        'message': 'Package not found'
                    }, status=404)
                
            else:
                return JsonResponse({
                    'status': 'error',
                    'message': f'Unknown action: {action}'
                }, status=400)
            
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
        
    return JsonResponse({
        'status': 'error',
        'message': 'Method not allowed'
    }, status=405)
    
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
    return render(request, 'ups/world_control.html', context)

