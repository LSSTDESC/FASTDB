AUTOMAKE_OPTION = subdir-objects

webapdir = @installdir@/webserver
templdir = @installdir@/webserver/templates
staticdir = @installdir@/webserver/static

webap_DATA = __init__.py baseview.py dbapp.py ltcvapp.py spectrumapp.py server.py \
	../../extern/rkwebutil/rkauth_flask.py

static_DATA = static/fastdb.css static/fastdb.js static/fastdb_ns.js static/fastdb_start.js \
	static/objectsearch.js static/objectlist.js static/objectinfo.js \
	static/rkauth.js static/resetpasswd_start.js \
	../../extern/rkwebutil/rkwebutil.js
	../../extern/rkwebutil/svgplot.js

templ_DATA = templates/base.html templates/fastdb_webap.html
