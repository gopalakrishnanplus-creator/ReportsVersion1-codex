from django.contrib import admin
from django.urls import include, path
from dashboard.views import campaign_overview, menu_page, campaign_login, export_report, etl_debug_page

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", menu_page, name="menu"),
    path("sapa-growth/", include("sapa_growth.urls")),
    path("debug/etl/", etl_debug_page, name="etl-debug"),
    path("campaign/<str:brand_campaign_id>/login/", campaign_login, name="campaign-login"),
    path("campaign/<str:brand_campaign_id>/", campaign_overview, name="campaign-overview-specific"),
    path("campaign/<str:brand_campaign_id>/export/", export_report, name="campaign-export"),
]
