from django.contrib import admin
from django.urls import include, path
from dashboard.views import campaign_overview, menu_page, campaign_login, export_report, etl_debug_page, send_access_email_view, campaign_access_page, reports_home, campaign_performance_links_page, campaign_performance_page
from dashboard.internal_data_admin import (
    internal_data_admin_delete,
    internal_data_admin_edit,
    internal_data_admin_home,
    internal_data_admin_login,
    internal_data_admin_logout,
    internal_data_admin_new,
    internal_data_admin_row,
    internal_data_admin_table,
)
from reporting.api_views import campaign_performance_api, campaign_performance_page_api, in_clinic_api, patient_education_api, red_flag_alert_api

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", reports_home, name="reports-home"),
    path("inclinic/", menu_page, name="menu"),
    path("reporting/api/red_flag_alert/", red_flag_alert_api, name="reporting-api-red-flag-alert"),
    path("reporting/api/in_clinic/", in_clinic_api, name="reporting-api-in-clinic"),
    path("reporting/api/patient_education/", patient_education_api, name="reporting-api-patient-education"),
    path("reporting/api/campaign-performance/", campaign_performance_api, name="reporting-api-campaign-performance"),
    path("reporting/api/campaign-performance/<str:campaign_id>/", campaign_performance_api, name="reporting-api-campaign-performance-specific"),
    path("reporting/api/campaign-performance-page/<str:campaign_id>/", campaign_performance_page_api, name="reporting-api-campaign-performance-page"),
    path("campaign-performance/links/", campaign_performance_links_page, name="campaign-performance-links"),
    path("campaign-performance/<str:campaign_id>/", campaign_performance_page, name="campaign-performance-page"),
    path("sapa-growth/", include("sapa_growth.urls")),
    path("pe-reports/", include("pe_reports.urls")),
    path("_internal/data-admin/login/", internal_data_admin_login, name="internal-data-admin-login"),
    path("_internal/data-admin/logout/", internal_data_admin_logout, name="internal-data-admin-logout"),
    path("_internal/data-admin/", internal_data_admin_home, name="internal-data-admin-home"),
    path("_internal/data-admin/<str:schema>/<str:table>/new/", internal_data_admin_new, name="internal-data-admin-new"),
    path("_internal/data-admin/<str:schema>/<str:table>/row/", internal_data_admin_row, name="internal-data-admin-row"),
    path("_internal/data-admin/<str:schema>/<str:table>/row/edit/", internal_data_admin_edit, name="internal-data-admin-edit"),
    path("_internal/data-admin/<str:schema>/<str:table>/row/delete/", internal_data_admin_delete, name="internal-data-admin-delete"),
    path("_internal/data-admin/<str:schema>/<str:table>/", internal_data_admin_table, name="internal-data-admin-table"),
    path("debug/etl/", etl_debug_page, name="etl-debug"),
    path("campaign/<str:brand_campaign_id>/login/", campaign_login, name="campaign-login"),
    path("campaign/<str:brand_campaign_id>/access/", campaign_access_page, name="campaign-access"),
    path("campaign/<str:brand_campaign_id>/send-access-email/", send_access_email_view, name="campaign-send-access-email"),
    path("campaign/<str:brand_campaign_id>/performance/", campaign_performance_page, name="campaign-performance-page-legacy"),
    path("campaign/<str:brand_campaign_id>/", campaign_overview, name="campaign-overview-specific"),
    path("campaign/<str:brand_campaign_id>/export/", export_report, name="campaign-export"),
]
