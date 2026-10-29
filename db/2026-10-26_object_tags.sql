CREATE TABLE tags(
  id           uuid PRIMARY KEY,
  tag          text,
  description  text,
  created_at   timestamp with time zone default NOW()
);
CREATE UNIQUE INDEX ix_tags_tag ON tags(tag);


CREATE TABLE root_diaobject_tag(
  rootid   uuid,
  tagid    uuid
);
ALTER TABLE root_diaobject_tag ADD CONSTRAINT fk_root_diaobject_tag_rootid
  FOREIGN KEY (rootid) REFERENCES root_diaobject(id) ON DELETE CASCADE;
ALTER TABLE root_diaobject_tag ADD CONSTRAINT fk_root_diaobject_tag_tagid
  FOREIGN KEY (tagid) REFERENCES tags(id) ON DELETE CASCADE;
CREATE UNIQUE INDEX ix_root_diaobject_tag_unique ON root_diaobject_tag(rootid, tagid);
CREATE INDEX ix_root_diaobject_tag_rootid ON root_diaobject_tag(rootid);
CREATE INDEX ix_root_diaobject_tag_tagid ON root_diaobject_tag(tagid);


CREATE TABLE authgroup(
  id           uuid PRIMARY KEY,
  name         text,
  description  text
);
CREATE UNIQUE INDEX ix_authgroup_name ON authgroup(name);

CREATE TABLE auth_user_group(
  userid   uuid,
  groupid  uuid
);
ALTER TABLE auth_user_group ADD CONSTRAINT fk_auth_user_group_groupid
  FOREIGN KEY (groupid) REFERENCES authgroup(id) ON DELETE CASCADE;
ALTER TABLE auth_user_group ADD CONSTRAINT fk_auth_user_group_userid
  FOREIGN KEY (userid) REFERENCES authuser(id) ON DELETE CASCADE;
CREATE INDEX ix_authgroup_userid ON auth_user_group(userid);
CREATE INDEX ix_authgroup_groupid ON auth_user_group(groupid);
CREATE UNIQUE INDEX ix_authgroup_unique ON auth_user_group(userid,groupid);

INSERT INTO authgroup VALUES(gen_random_uuid(), 'admin', 'admin access');
INSERT INTO authgroup VALUES(gen_random_uuid(), 'tag_create', 'can create tags');
INSERT INTO authgroup VALUES(gen_random_uuid(), 'users', 'all users');
INSERT INTO auth_user_group(userid, groupid)
  SELECT u.id, g.id
  FROM authuser u
  INNER JOIN authgroup g ON g.name='users';
