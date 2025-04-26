# Create your models here.
from django.db import models
from django.conf import settings

class Truck(models.Model):
    id = models.IntegerField(primary_key=True)
    status = models.CharField(max_length=20, default='idle')
    x = models.IntegerField(default=0)
    y = models.IntegerField(default=0)
    world_id = models.BigIntegerField()
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'trucks' 

class Warehouse(models.Model):
    id = models.IntegerField(primary_key=True)
    x = models.IntegerField()
    y = models.IntegerField()
    world_id = models.BigIntegerField()

    class Meta:
        db_table = 'warehouses' 

class Package(models.Model):
    id = models.CharField(max_length=50, primary_key=True)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True)
    truck = models.ForeignKey(Truck, on_delete=models.SET_NULL, null=True, blank=True)
    warehouse = models.ForeignKey(Warehouse, on_delete=models.SET_NULL, null=True)
    status = models.CharField(max_length=20, default='created')
    destination_x = models.IntegerField(null=True, blank=True)
    destination_y = models.IntegerField(null=True, blank=True)
    description = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'packages' 

class Item(models.Model):
    id = models.AutoField(primary_key=True)  
    package = models.ForeignKey('Package', on_delete=models.CASCADE, related_name='items')
    name = models.CharField(max_length=100)
    description = models.TextField(blank=True, null=True)
    quantity = models.IntegerField(default=1)

    class Meta:
        db_table = 'items' 

    def __str__(self):
        return f"{self.quantity} x {self.name}"

class Notification(models.Model):
    id = models.AutoField(primary_key=True)  
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    message = models.TextField()
    read = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'notifications' 

class ErrorLog(models.Model):
    id = models.AutoField(primary_key=True)  
    seqnum = models.BigIntegerField(null=True, blank=True)
    error_message = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'error_logs'


class WorldState(models.Model):
    id = models.AutoField(primary_key=True)  
    world_id = models.BigIntegerField(unique=True)
    sim_speed = models.IntegerField(default=100)
    connected_at = models.DateTimeField(auto_now_add=True)
    is_connected = models.BooleanField(default=False)

    class Meta:
        db_table = 'world_state'  


class SequenceNum(models.Model):
    id = models.AutoField(primary_key=True)  
    last_seq_num = models.BigIntegerField(default=0)
    last_ack_received = models.BigIntegerField(default=0)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'sequence_num'


class AmazonMessage(models.Model): # need to send to amazon
    id = models.AutoField(primary_key=True)  
    message_type = models.CharField(max_length=50)
    message_content = models.JSONField()
    status = models.CharField(max_length=20, default='pending')
    created_at = models.DateTimeField(auto_now_add=True)
    processed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = 'amazon_messages'

class CommandRetryQueue(models.Model):
    id = models.AutoField(primary_key=True)  
    original_seq_num = models.BigIntegerField()
    retry_count = models.IntegerField()
    status = models.CharField(max_length=20, default='pending')
    created_at = models.DateTimeField()
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = 'command_retry_queue'


# creation needs: seq_num, command_type, command_data
class CommandLog(models.Model): # request from amazon
    id = models.AutoField(primary_key=True)  
    seq_num = models.BigIntegerField()
    command_type = models.CharField(max_length=20) # 'pickup', 'delivery', 'query'
    command_data = models.JSONField()
    status = models.CharField(max_length=20, default='pending')  #pending, processing, success, failed // failed resent?
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    acknowledged_at = models.DateTimeField(null=True, blank=True) #completed
    error_message = models.TextField(null=True, blank=True) #
    retry_count = models.IntegerField(default=0) #

    class Meta:
        db_table = 'command_logs'
