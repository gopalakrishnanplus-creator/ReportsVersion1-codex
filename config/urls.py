from django.contrib import admin
from django.urls import path
from dashboard.views import campaign_overview, menu_page, campaign_login, export_report

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", menu_page, name="menu"),
    path("campaign/<str:brand_campaign_id>/login/", campaign_login, name="campaign-login"),
    path("campaign/<str:brand_campaign_id>/", campaign_overview, name="campaign-overview-specific"),
    path("campaign/<str:brand_campaign_id>/export/", export_report, name="campaign-export"),
]
