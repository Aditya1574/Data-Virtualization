from django.urls import path
from . import views

urlpatterns = [
    path('', views.TheRunner),
    path('options_selected',views.options_selected),
    path('data_collected',views.collected_data_processing),
    path('final_data_collected',views.send_to_tableSelection),
    path('addTableHandler',views.SaveAddedtableMetaData),
    path('see_sample_data',views.SeeSampleData),
    path('data_selected',views.GenerateColumns),
    path('get_join_data',views.Datajoiner)
]