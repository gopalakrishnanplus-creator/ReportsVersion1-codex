from django.urls import path

from pe_reports import views


app_name = "pe_reports"

urlpatterns = [
    path("", views.campaign_menu, name="menu"),
    path("debug/etl/", views.etl_debug, name="etl-debug"),
    path("campaign/<str:campaign_id>/login/", views.campaign_login, name="login"),
    path("campaign/<str:campaign_id>/access/", views.campaign_access_page, name="access"),
    path("campaign/<str:campaign_id>/send-access-email/", views.send_access_email_view, name="send-access-email"),
    path("campaign/<str:campaign_id>/", views.dashboard, name="dashboard"),
    path("campaign/<str:campaign_id>/details/<str:metric>/", views.detail_view, name="detail"),
    path("campaign/<str:campaign_id>/details/<str:metric>/export/", views.detail_export, name="detail-export"),
    path("campaign/<str:campaign_id>/export/dashboard.pdf", views.dashboard_export, name="dashboard-export"),
]
