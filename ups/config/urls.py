"""config URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from core import views

urlpatterns = [
    # path('admin/', admin.site.urls),
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
    
    path('api/amazon/', views.amazon_api, name='amazon_api'),  #
    path('api/truck/<int:truck_id>/', views.truck_status_api, name='truck_status_api'),
    path('api/package/<str:tracking_number>/', views.package_status_api, name='package_status_api'),
]