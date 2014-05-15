from django.conf.urls.defaults import *
from django.contrib import admin
from django.conf import settings 
from fileupload.conf import settings as fileuploadsettings

admin.autodiscover()


urlpatterns = patterns('',
    # Example:
    # (r'^fileupload/', include('fileupload.foo.urls')),

    # Uncomment this for admin:
    (r'^admin/', include(admin.site.urls)),

    # Default page
    #(r'^$', 						'django.views.generic.simple.direct_to_template',{'template': 'index.html'}),	

    #urls for the fileupload app
    (r'^$',				'vigmod.fileupload.views.upload'),
    (r'^upload',			'vigmod.fileupload.views.upload'),
    (r'^success', 		'vigmod.fileupload.views.success'),
    (r'^validatelink', 	'vigmod.fileupload.views.validatelink'),
    (r'^download', 		'vigmod.fileupload.views.download'),
    (r'^fileupload/files/(?P<path>.*)$',    'django.views.static.serve', {'document_root': fileuploadsettings.DOWNLOADPATH}),
        #old ones, need to be changed to redirects
    (r'^files/(?P<path>.*)$',    'django.views.static.serve', {'document_root': fileuploadsettings.DOWNLOADPATH}),
    (r'^fileupload/$',				'vigmod.fileupload.views.upload'),
    (r'^fileupload/upload',			'vigmod.fileupload.views.upload'),
    (r'^fileupload/success', 		'vigmod.fileupload.views.success'),
    (r'^fileupload/validatelink', 	'vigmod.fileupload.views.validatelink'),
    (r'^fileupload/download', 		'vigmod.fileupload.views.download'),
    
)

#For css etc in dev environment
if settings.DEBUG:
    urlpatterns += patterns('',
        (r'^site_media/(?P<path>.*)$', 'django.views.static.serve', {'document_root': fileuploadsettings.SITE_MEDIA}),
    )
