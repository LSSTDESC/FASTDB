-- Oops... made a couple of tables without primary keys, which pisses off fastdb_loader
--   when it tries to remove all constraints.  Turn the unique indexes into primary keys,
--   should work the same way.

DROP INDEX ix_root_diaobject_tag_unique;
ALTER TABLE root_diaobject_tag ADD PRIMARY KEY(rootid, tagid);

DROP INDEX ix_authgroup_unique;
ALTER TABLE auth_user_group ADD PRIMARY KEY(userid, groupid);


