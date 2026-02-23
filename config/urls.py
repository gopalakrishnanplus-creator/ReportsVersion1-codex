from django.contrib import admin
from django.urls import path
from dashboard.views import campaign_overview

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", campaign_overview, name="campaign-overview"),
    path("campaign/<str:brand_campaign_id>/", campaign_overview, name="campaign-overview-specific"),
]
