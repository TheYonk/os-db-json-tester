drop database if exists movie_json_test;
create database movie_json_test;

create user movie_json_user with password 'Change_me_1st!';
GRANT all privileges on database movie_json_test to movie_json_user;  

drop database if exists movie_json_test;
create database movie_json_test owner movie_json_user;

\c postgresql://movie_json_user:Change_me_1st!@localhost/movie_json_test

CREATE SCHEMA movies authorization movie_json_user;

create SCHEMA movie_json_test;

drop table movies_json;

drop table movies_json_hybrid;
drop table movies_json_generated;;


drop table movies_normalized_meta;
drop table movies_normalized_actors;
drop table movies_normalized_cast;


create table movies_json (
	ai_myid serial primary key, 
	imdb_id varchar(255),
	json_column json not null
);

create unique index movies_json_imdb_idx on movies_json(imdb_id);
CREATE INDEX gin_index ON movies_json USING gin (jsonb_column);


create table movies_jsonb (
	ai_myid serial primary key, 
	imdb_id varchar(255),
	jsonb_column jsonb not null
);

create unique index movies_jsonb_imdb_idx on movies_jsonb(imdb_id);
CREATE INDEX movies_jsonb_gin_index ON movies_jsonb USING gin (json_column);

create table movies_json_generated (
	ai_myid serial primary key, 
	imdb_id varchar(255) generated always as (jsonb_column ->> 'imdb_id') stored,
	title varchar(255) generated always as (jsonb_column ->> 'title') stored,
    imdb_rating decimal(5,2) generated always as ((jsonb_column  ->> 'imdb_rating')::numeric) stored,
	overview text generated always as (jsonb_column ->> 'overview') stored,
	director jsonb generated always as ((jsonb_column ->> 'director')::json) stored,
	country varchar(100) generated always as (jsonb_column ->> 'country') stored,
	jsonb_column jsonb,
	json_column json
);

create unique index gen_imdb_idx on movies_json_generated(imdb_id);
create index gen_title_idx on movies_json_generated(title);
create index gen_func_title_index on movies_json_generated (((json_column ->> 'title')::varchar));	
CREATE INDEX Gen_gin_index ON movies_json_generated USING gin (jsonb_column);
	

create table movies_normalized_meta (
	ai_myid serial primary key, 
	imdb_id varchar(32), 
	title varchar(255), 
	imdb_rating decimal(5,2),
	year int,
	country varchar(100),
	overview text,
	json_column jsonb,
	upvotes int,
	downvotes int
);

CREATE INDEX gin_index ON movies_normalized_meta USING gin (json_column);
create unique index imdb_id_idx  on movies_normalized_meta (imdb_id);


create table movies_normalized_actors (
	ai_actor_id serial primary key, 
	actor_id varchar(50), 
	actor_name varchar(500)
	);
create unique index actor_id_idx  on movies_normalized_actors (actor_id);

create table movies_normalized_cast (
	inc_id serial primary key, 
	ai_actor_id int, 
	ai_myid int,
    actor_character varchar(500)	
	);

create index cast_id_idx  on movies_normalized_cast (ai_actor_id,ai_myid);
create index cast_id2_idx  on movies_normalized_cast (ai_myid);
create index cast_character_idx  on movies_normalized_cast (actor_character);
create unique index u_cast_idx  on movies_normalized_cast (ai_myid,ai_actor_id,actor_character);
	
create table movies_normalized_director (
  director_id serial primary key, 
  ai_myid int,
  director varchar(500)	
);

create index director_mv_idx  on movies_normalized_director (ai_myid);

create table movies_normalized_genres_tags (
	  genre_id serial primary key, 
	  genre varchar(500)	
	);
create unique index genre_idx  on movies_normalized_genres_tags(genre);

	
create table movies_normalized_genres (
	  genre_id int, 
	  ai_myid int
);	

alter table movies_normalized_genres add primary key (genre_id,ai_myid);

create table movies_normalized_user_comments(
  comment_id serial primary key, 
  ai_myid int,
  rating int,
  comment text,
  imdb_id varchar(20),
  comment_add_time timestamp WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, 
  comment_update_time timestamp WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);


create index comment_movie_id_idx  on movies_normalized_user_comments (ai_myid);


create table movies_normalized_aggregate_ratings(
	  ai_myid serial primary key,
	  user_rating int,
	  up_votes int,
	  down_votes int,
	  imdb_rating int
);	

	

	
CREATE TABLE voting_count_history (
  ai_myid int,
  store_time timestamp WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  title varchar(255),
  imdb_id varchar(20),
  comment_count bigint NOT NULL DEFAULT '0',
  max_rate int DEFAULT NULL,
  avg_rate decimal(14,4) DEFAULT NULL,
  upvotes decimal(32,0) DEFAULT NULL,
  downvotes decimal(32,0) DEFAULT NULL
) ;
alter table voting_count_history add primary key (title,ai_myid,store_time);

CREATE INDEX cnt_hist_ai_idx ON voting_count_history (ai_myid);



create table movies_viewed_logs (
	  view_id serial primary key,
	  ai_myid int,
	  imdb_id varchar(32), 
          watched_time timestamp WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
	  watched_user_id int,
	  time_watched_sec int,
	  encoded_data varchar(500),
          json_payload jsonb,
	  json_imdb_id varchar(255) generated always as (json_payload ->> 'imdb_id') stored	
);	

CREATE INDEX m_view_idx1 ON movies_viewed_logs (ai_myid,watched_time);
CREATE INDEX m_view_idx2 ON movies_viewed_logs (watched_time);
CREATE INDEX m_view_idx3 ON movies_viewed_logs (watched_user_id);

GRANT all privileges on all tables in schema public to movie_json_user; 	
GRANT all privileges on all sequences in schema public to movie_json_user; 	
	
create index idx_comments_id on movies_normalized_user_comments(ai_myid,comment_add_time);
create index idx_comments_com_time on movies_normalized_user_comments(comment_add_time);

create index idx_nmm_rate on movies_normalized_meta(imdb_rating);
create index idx_nmm_year_rate on movies_normalized_meta(year,imdb_rating);
create index idx_nmm_country_year on movies_normalized_meta(country,year,imdb_rating);
create index idx_nmm_title on movies_normalized_meta(title);

create index idx_nd_director on movies_normalized_director (director, ai_myid);

create index idx_nd_id on movies_normalized_director(ai_myid);

create index idx_nc_char on movies_normalized_cast(actor_character);
create index idx_nc_id2 on movies_normalized_cast(ai_actor_id,ai_myid);
create index idx_nc_id on movies_normalized_cast(ai_myid);

create index idx_na_name on movies_normalized_actors(actor_name);	
