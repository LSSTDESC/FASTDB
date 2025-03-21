AC_INIT([fastdb], [0.1], [raknop@lbl.gov])
AM_INIT_AUTOMAKE([foreign])
AC_CONFIG_FILES([
  Makefile
  src/Makefile
  src/webserver/Makefile
  src/config.py
  share/Makefile
])

AC_ARG_WITH( installdir,
             [AS_HELP_STRING([--with-installdir=DIR], [Directory to install to [/usr/local/fastdb]])],
             [installdir=$withval],
             [installdir=/usr/local/fastdb] )

AC_ARG_WITH( dbhost,
             [AS_HELP_STRING([--with-dbhost=HOST], [Postgres host [postgres]])],
             [dbhost=$withval],
             [dbhost=postgres] )
AC_ARG_WITH( dbport,
             [AS_HELP_STRING([--with-dbport=PORT], [Postgres port [5432]])],
             [dbport=$withval],
             [dbport=5432] )
AC_ARG_WITH( dbdatabase,
             [AS_HELP_STRING([--with-dbdatabase=PORT], [Database name [fastdb]])],
             [dbdatabase=$withval],
             [dbdatabase=fastdb] )
AC_ARG_WITH( dbuser,
             [AS_HELP_STRING([--with-dbuser=USER], [Postgres username [postgres]])],
             [dbuser=$withval],
             [dbuser=postgres] )
AC_ARG_WITH( db-passwd-file,
             [AS_HELP_STRING([--with-db-passwd-file=PWFILE], [File with postgres password [/secrets/pgpasswd]])],
             [dbpasswdfile=$withval],
             [dbpasswdfile=/secrets/pgpasswd] )

AC_ARG_WITH( secret-key-file,
             [AS_HELP_STRING([--with-secret-key-file=KEYFILE], [File with flask secret key [/secrets/secretkey]])],
             [secretkeyfile=$withfal],
             [secretkeyfile=/secrets/secretkey] )

AC_ARG_WITH( email-from,
             [AS_HELP_STRING([--with-email-from=EMAILFROM], [Who is email from? [fastdb@nowhere.org]])],
             [emailfrom=$withval],
             [emailfrom=fastdb@nowhere.org] )
AC_ARG_WITH( smtp-server,
             [AS_HELP_STRING([--with-smtp-server=SMTPSERVER], [smtp server [localhost]])],
             [smtpserver=$withval],
             [smtpserver=localhost] )
AC_ARG_WITH( smtp-port,
             [AS_HELP_STRING([--with-smtp-port=SMTPPORT], [smtp port [25]])],
             [smtpport=$withval],
             [smtpport=25] )
AC_ARG_ENABLE( smtp-ssl,
               [AS_HELP_STRING([--enable-smtp-ssl], [smtp server uses ssl])],
               [smtpusessl=True],
               [smtpusessl=False] )
AC_ARG_WITH( smtp-user,
             [AS_HELP_STRING([--with-smtp-user=USER], [smtp username])],
             [smtpusername=$withval],
             [smtpusername=None] )
AC_ARG_WITH( smtp-password,
             [AS_HELP_STRING([--with-smtp-password=PASSWORD], [smtp password])],
             [smtppassword=$withval],
             [smtppassword=None] )

AC_SUBST(installdir)
AC_SUBST(dbhost)
AC_SUBST(dbport)
AC_SUBST(dbdatabase)
AC_SUBST(dbuser)
AC_SUBST(dbpasswdfile)
AC_SUBST(secretkeyfile)
AC_SUBST(emailfrom)
AC_SUBST(smtpserver)
AC_SUBST(smtpport)
AC_SUBST(smtpusessl)
AC_SUBST(smtpusername)
AC_SUBST(smtppassword)

AC_OUTPUT

