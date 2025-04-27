# notification_manager.py
import logging
import threading
import time
from datetime import datetime, timedelta
from django.utils import timezone
from django.core.mail import send_mail
from django.conf import settings
from django.db.models import Q

from core.models import Package, Notification, NotificationPreference, User

logger = logging.getLogger('notification_system')

class NotificationManager:
    """
    A system for managing user notifications for package deliveries.
    Simplified to send notifications only when packages are delivered.
    """
    
    def __init__(self):
        """Initialize the notification manager."""
        self.running = False
        self.delivery_thread = None
    
    def start(self):
        """Start the notification background processes"""
        self.running = True
        logger.info("Notification manager started")
    
    def stop(self):
        """Stop the notification background processes"""
        self.running = False
        logger.info("Notification manager stopped")
    
    def register_notification_preference(self, user, notification_type, enabled=True, threshold_minutes=30):
        """
        Register or update a user's notification preferences.
        
        Args:
            user: User instance
            notification_type: Type of notification (e.g., 'delivery', 'truck_arrival')
            enabled: Whether notifications are enabled
            threshold_minutes: No longer used but kept for compatibility
            
        Returns:
            NotificationPreference: The created or updated preference
        """
        pref, created = NotificationPreference.objects.update_or_create(
            user=user,
            notification_type=notification_type,
            defaults={
                'enabled': enabled,
                'threshold_minutes': threshold_minutes  # Kept for backwards compatibility
            }
        )
        
        return pref
    
    def send_delivery_notification(self, package):
        """
        Send a notification that a package has been delivered.
        
        Args:
            package: Package instance
        """
        if not package.user:
            return
        
        try:
            # Check if user has delivery notifications enabled
            try:
                pref = NotificationPreference.objects.get(
                    user=package.user,
                    notification_type='delivery'
                )
                
                if not pref.enabled:
                    return
            except NotificationPreference.DoesNotExist:
                # Default to enabled if no preference set
                pass
            
            # Create message
            message = f"Your package {package.id} has been delivered successfully!"
            
            # Send email notification
            self._send_email_notification(
                package.user.email,
                "Package Delivery Notification",
                message
            )
            
            logger.info(f"Sent delivery notification for package {package.id} to user {package.user.username}")
        
        except Exception as e:
            logger.error(f"Error sending delivery notification for package {package.id}: {e}")

    def send_pickup_notification(self, package):
        """
        Send a notification that a package is ready for pickup.
        
        Args:
            package: Package instance
        """
        if not package.user:
            return
        
        try:
            # Check if user has delivery notifications enabled
            try:
                pref = NotificationPreference.objects.get(
                    user=package.user,
                    notification_type='pickup'
                )
                
                if not pref.enabled:
                    return
            except NotificationPreference.DoesNotExist:
                # Default to enabled if no preference set
                pass
            
            # Create message
            message = f"Your package {package.id} is ready for pickup!"
            
            # Send email notification
            self._send_email_notification(
                package.user.email,
                "Package Pickup Notification",
                message
            )
            
            logger.info(f"Sent pickup notification for package {package.id} to user {package.user.username}")
        
        except Exception as e:
            logger.error(f"Error sending delivery notification for package {package.id}: {e}")
    
    def send_truck_arrival(self, package, warehouse_id):
        """
        Send a notification that a truck has arrived at a warehouse for pickup.
        
        Args:
            package: Package instance
            warehouse_id: ID of the warehouse
        """
        if not package.user:
            return
        
        try:
            # Check if user has pickup notifications enabled
            try:
                pref = NotificationPreference.objects.get(
                    user=package.user,
                    notification_type='truck_arrival'
                )
                
                if not pref.enabled:
                    return
            except NotificationPreference.DoesNotExist:
                # Default to enabled if no preference set
                pass
            
            # Create message
            message = f"A truck has arrived at warehouse {warehouse_id} to pick up your package {package.id}."
            
            # Create notification in database
            Notification.objects.create(
                user=package.user,
                message=message
            )
            
            # Send email notification
            self._send_email_notification(
                package.user.email,
                "Truck Arrival Notification",
                message
            )
            
            logger.info(f"Sent truck arrival notification for package {package.id} to user {package.user.username}")
        
        except Exception as e:
            logger.error(f"Error sending truck arrival notification for package {package.id}: {e}")
    
    def _send_email_notification(self, email, subject, message):
        """
        Send email notification.
        
        Args:
            email: Email address
            subject: Email subject
            message: Notification message
        """
        try:
            send_mail(
                subject,
                message,
                settings.DEFAULT_FROM_EMAIL,
                [email],
                fail_silently=False,
            )
            logger.info(f"Sent email notification to {email}")
        except Exception as e:
            logger.error(f"Error sending email notification to {email}: {e}")

# Create a global instance to be imported by other modules
notification_manager = NotificationManager()