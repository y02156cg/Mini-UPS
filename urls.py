from django.urls import path
from . import views

urlpatterns = [
    path('', views.home, name='home'),
    path('login/', views.login_view, name='login'),
    path('logout/', views.logout_view, name='logout'),
    path('register/', views.register, name='register'),
    path('dashboard/', views.dashboard, name='dashboard'),
    
    path('track/', views.track_package, name='track_package'),
    path('package/<str:tracking_number>/', views.package_details, name='package_details'),
    path('package/<str:tracking_number>/redirect/', views.redirect_package, name='redirect_package'),

    path('admin/dashboard/', views.admin_dashboard, name='admin_dashboard'),
    path('admin/world/', views.world_control, name='world_control'),
    
    path('api/amazon/', views.amazon_api, name='amazon_api'),
    path('api/truck/<int:truck_id>/', views.truck_status_api, name='truck_status_api'),
    path('api/package/<str:tracking_number>/', views.package_status_api, name='package_status_api'),
]